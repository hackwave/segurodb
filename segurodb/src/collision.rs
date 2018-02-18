use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::slice;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use memmap::{Mmap, Protection};

use error::Result;
use transaction::Operation;

/// A data file representing all the data for a given prefix. All the data for this prefix exists in
/// this file because there was a high threshold of collisions.
///
/// The data file is a log file backed by an in-memory BTree. All mutable operations are appended to
/// the log file, and the in-memory BTree maps the keys to their position in the log file.
///
/// When the collision file is opened it is traversed to build the in-memory index.
///
/// Idea: grow the log file in chunks (instead of appending to a file) and mmap it. When compacting
/// the log we rewrite it with the keys sorted (since we have the in-memory index), so iteration
/// should be fast on compacted log files.
///
/// Alternative: use exactly the same strategy as used for the data file but ignoring the first `n`
/// bits of the prefix and adding extra bits as needed
///
#[derive(Debug)]
pub struct Collision {
	index: BTreeMap<LogSlice, IndexEntry>,
	prefix: u32,
	path: PathBuf,
	mmap: Mmap,
	file: File,
}

#[derive(Debug)]
pub struct IndexEntry {
    position: u64,
	// TODO: we can optimize this data structure for constant value sizes
    size: usize,
}

impl Collision {
	fn collision_file_path<P: AsRef<Path>>(path: P, prefix: u32) -> PathBuf {
		let collision_file_name = format!("collision-{}.log", prefix);
		path.as_ref().join(collision_file_name)
	}

	fn build_index(data: &[u8]) -> Result<BTreeMap<LogSlice, IndexEntry>> {
		let log = LogIterator::new(data);

		let mut index = BTreeMap::new();

		for (position, entry) in log {
			if let Some(value) = entry.value {
				let position = position as u64;
				let size = LogEntry::len(&entry.key, &value);

				index.insert(
					LogSlice::new(entry.key),
					IndexEntry { position, size });

			} else {
				index.remove(&LogSlice::new(entry.key));
			}
		}

		Ok(index)
	}

	/// Create a new collision file for the given prefix.
	pub fn create<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Collision> {
		// Create directories if necessary.
		fs::create_dir_all(&path)?;

		let path = Self::collision_file_path(path, prefix);
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&path)?;

		// TODO: grow file in chunks to avoid rebuilding index on every mutable operation
		file.set_len(1)?;
		file.flush()?;
		let mmap = Mmap::open_path(&path, Protection::Read)?;

		let index = BTreeMap::new();

		Ok(Collision { index, prefix, path, mmap, file })
	}

	/// Open collision file if it exists, returns `None` otherwise.
	pub fn open<P: AsRef<Path>>(path: P, prefix: u32) -> Result<Option<Collision>> {
		let path = Self::collision_file_path(path, prefix);
		let open_options = fs::OpenOptions::new()
			.append(true)
			.open(&path);

		let file = match open_options {
			Ok(file) => file,
			Err(ref err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err.into()),
		};

		let mmap = Mmap::open_path(&path, Protection::Read)?;
		let index = {
			let data = unsafe { &mmap.as_slice() };
			Collision::build_index(data)?
		};

		Ok(Some(Collision { index, prefix, path, mmap, file }))
	}

	fn rebuild_index(&mut self) -> Result<()> {
		let mmap = Mmap::open_path(&self.path, Protection::Read)?;
		let index = {
			let data = unsafe { &mmap.as_slice() };
			Collision::build_index(data)?
		};

		self.mmap = mmap;
		self.index = index;

		Ok(())
	}

	/// Inserts the given key-value pair into the collision file.
	pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		if let Some(current_value) = self.get(key)? {
			if current_value == value { return Ok(()); }
		}

		// FIXME: have write return the `LogSlice` to avoid re-reading the entry
		let position = LogEntry::write(&mut self.file, key, value)?;
		let size = LogEntry::len(&key, &value);

		self.rebuild_index()?;

		let data = unsafe { &self.mmap.as_slice()[position as usize..] };
		let (_, entry) = LogEntry::read(data);

		assert!(key == entry.key,
				"found incorrect key after insertion into log");

		self.index.insert(LogSlice::new(entry.key), IndexEntry { position, size });

		Ok(())
	}

	/// Removes the given `key` from the collision file.
	pub fn delete(&mut self, key: &[u8]) -> Result<()> {
		if let Some(_) = self.index.remove(&LogSlice::new(key)) {
			LogEntry::write_deleted(&mut self.file, key)?;
			self.rebuild_index()?;
		}

		Ok(())
	}

	/// Lookup a value associated with the given `key` in the collision file.
	pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>> {
		if let Some(entry) = self.index.get(&LogSlice::new(key)) {
			let data = unsafe { &self.mmap.as_slice()[entry.position as usize..] };
			let (_, entry) = LogEntry::read(data);
			assert!(key == entry.key,
					"index pointed to log entry with different key");

			Ok(Some(entry.value.expect("index only points to live entries; qed")))
		} else {
			Ok(None)
		}
	}

	/// Applies the given `Operation` by dispatching to the `insert` or `delete` methods.
	pub fn apply(&mut self, op: Operation) -> Result<()> {
		match op {
			Operation::Delete(key) => self.delete(key),
			Operation::Insert(key, value) => self.insert(key, value),
		}
	}

	/// Return the `prefix` that this collision file refers to, i.e. all keys stored in this file
	/// have this prefix.
	pub fn prefix(&self) -> u32 {
		self.prefix
	}

	/// Returns an iterator over all key-value pairs in the collision file ordered by key.
	pub fn iter<'a>(&'a self) -> Result<CollisionLogIterator> {
		let data = unsafe { &self.mmap.as_slice() };

		CollisionLogIterator::new(data, self.index.values())
	}
}

pub struct CollisionLogIterator<'a> {
	data: &'a [u8],
	index_iter: btree_map::Values<'a, LogSlice, IndexEntry>,
}

impl<'a> CollisionLogIterator<'a> {
	fn new(
		data: &'a [u8],
		index_iter: btree_map::Values<'a, LogSlice, IndexEntry>,
	) -> Result<CollisionLogIterator<'a>> {
		Ok(CollisionLogIterator { data, index_iter })
	}
}

impl<'a> Iterator for CollisionLogIterator<'a> {
	type Item = Result<(&'a [u8], &'a [u8])>;

	fn next(&mut self) -> Option<Self::Item> {
		self.index_iter.next().and_then(|entry| {
			let read_next = || {
				let data = &self.data[entry.position as usize..];
				let (_, entry) = LogEntry::read(data);

				Ok((entry.key,
					entry.value.expect("index only points to live entries; qed")))
			};

			match read_next() {
				Err(err) => Some(Err(err)),
				Ok(res) => Some(Ok(res)),
			}
		})
	}
}

#[derive(Debug)]
struct LogEntry<'a> {
	key: &'a [u8],
	value: Option<&'a [u8]>,
}

/// Unsafe view onto memmap file memory which backs collision log file.
#[derive(Debug)]
struct LogSlice {
	data: *const u8,
	len: usize,
}

impl Ord for LogSlice {
    fn cmp(&self, other: &Self) -> Ordering {
		unsafe {
			self.as_slice().cmp(other.as_slice())
		}
	}
}

impl PartialOrd for LogSlice {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for LogSlice {
	fn eq(&self, other: &Self) -> bool {
		unsafe {
			self.as_slice().eq(other.as_slice())
		}
	}
}

impl Eq for LogSlice {}

impl LogSlice {
	fn new(data: &[u8]) -> LogSlice {
		LogSlice {
			data: data.as_ptr(),
			len: data.len(),
		}
	}

	unsafe fn as_slice<'a>(&self) -> &'a [u8] {
		slice::from_raw_parts(self.data, self.len)
	}
}

impl<'a> LogEntry<'a> {
	const ENTRY_STATIC_SIZE: usize = 8; // key_size(4) + value_size(4)
	const ENTRY_TOMBSTONE: u32 = !0; // used as value_size to represent a deleted entry
	// FIXME: validate max value size

	fn write_deleted<W: Write + Seek>(writer: &mut W, key: &[u8]) -> Result<u64> {
		let position = writer.seek(SeekFrom::Current(0))?;
		writer.write_u32::<LittleEndian>(key.len() as u32)?;
		writer.write_all(key)?;
		writer.write_u32::<LittleEndian>(LogEntry::ENTRY_TOMBSTONE)?;
		Ok(position)
	}

	fn write<W: Write + Seek>(writer: &mut W, key: &[u8], value: &[u8]) -> Result<u64> {
		let position = writer.seek(SeekFrom::Current(0))?;
		writer.write_u32::<LittleEndian>(key.len() as u32)?;
		writer.write_all(key)?;
		writer.write_u32::<LittleEndian>(value.len() as u32)?;
		writer.write_all(value)?;
		Ok(position)
	}

	// FIXME: should return Result
	fn read(data: &[u8]) -> (usize, LogEntry) {
		// FIXME: add sanity limits for key_size and value_size
		//        this is to avoid issues if the file gets corruped
		let mut offset = 4;
		let key_size = LittleEndian::read_u32(&data[..offset]) as usize;

		let key = &data[offset..offset + key_size];
		offset += key_size;

		let value_size = LittleEndian::read_u32(&data[offset..]) as usize;
		offset += 4;

		let value =
			if value_size == LogEntry::ENTRY_TOMBSTONE as usize {
				None
			} else {
				let v = Some(&data[offset..offset + value_size]);
				offset += value_size;
				v
			};


		(offset, LogEntry { key, value })
	}

	fn len(key: &[u8], value: &[u8]) -> usize {
		LogEntry::ENTRY_STATIC_SIZE + key.len() + value.len()
	}
}

struct LogIterator<'a> {
	data: &'a [u8],
	position: usize,
}

impl<'a> LogIterator<'a> {
	fn new(data: &[u8]) -> LogIterator {
		let position = 0;
		LogIterator { data, position }
	}
}

impl<'a> Iterator for LogIterator<'a> {
	type Item = (usize, LogEntry<'a>);

	fn next(&mut self) -> Option<(usize, LogEntry<'a>)> {
		if self.position >= self.data.len() { None }
		else {
			let (read, entry) = LogEntry::read(&self.data[self.position..]);
			let position = self.position;

			self.position += read;

			Some((position, entry))
		}
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::Collision;

	#[test]
	fn test_roundtrip() {
		let temp = tempdir::TempDir::new("test_roundtrip").unwrap();

		{
			let mut collision = Collision::create(temp.path(), 0).unwrap();
			collision.insert(b"hello", b"world").unwrap();
			assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");
		}

		let collision = Collision::open(temp.path(), 0).unwrap().unwrap();
		assert_eq!(collision.get(b"hello").unwrap().unwrap(), b"world");
	}

	#[test]
	fn test_iter() {
		let temp = tempdir::TempDir::new("test_roundtrip").unwrap();

		{
			let mut collision = Collision::create(temp.path(), 0).unwrap();
			collision.insert(b"0", b"0").unwrap();
			collision.insert(b"2", b"2").unwrap();
			collision.insert(b"1", b"1").unwrap();
			collision.insert(b"4", b"4").unwrap();
			collision.insert(b"3", b"3").unwrap();
			collision.delete(b"4").unwrap();
		}

		let collision = Collision::open(temp.path(), 0).unwrap().unwrap();
		let collision: Vec<_> = collision.iter().unwrap().flat_map(|entry| entry.ok()).collect();

		let expected: Vec<(&[u8], &[u8])> =
			vec![(b"0", b"0"), (b"1", b"1"), (b"2", b"2"), (b"3", b"3")];

		assert_eq!(collision, expected);
	}
}
