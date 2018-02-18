use std::collections::vec_deque::Drain;
use std::collections::{BTreeSet, HashMap, VecDeque, btree_set};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{PathBuf, Path};
use std::slice;

use hex_slice::AsHex;
use memmap::{Mmap, Protection};
use tiny_keccak::sha3_256;

use error::{ErrorKind, Result};
use transaction::{Operation, OperationsIterator, Transaction};

const CHECKSUM_SIZE: usize = 32;

#[derive(Debug, PartialEq)]
enum JournalOperation<T> {
	Insert(T),
	Delete,
}

/// Unsafe view onto memmap file memory which backs journal.
#[derive(Debug)]
struct JournalSlice {
	key: *const u8,
	len: usize,
}

impl JournalSlice {
	fn new(key: &[u8]) -> JournalSlice {
		JournalSlice {
			key: key.as_ptr(),
			len: key.len(),
		}
	}

	unsafe fn as_slice<'a>(&self) -> &'a [u8] {
		slice::from_raw_parts(self.key, self.len)
	}
}

impl Hash for JournalSlice {
	fn hash<H: Hasher>(&self, state: &mut H) {
		unsafe {
			self.as_slice().hash(state);
		}
	}
}

impl PartialEq for JournalSlice {
	fn eq(&self, other: &Self) -> bool {
		unsafe {
			self.as_slice().eq(other.as_slice())
		}
	}
}

impl Eq for JournalSlice {}

unsafe fn cache_memory(memory: &[u8]) -> HashMap<JournalSlice, JournalOperation<JournalSlice>> {
	let iterator = OperationsIterator::new(memory);
	iterator.map(|o| match o {
		Operation::Insert(key, value) => (JournalSlice::new(key), JournalOperation::Insert(JournalSlice::new(value))),
		Operation::Delete(key) => (JournalSlice::new(key), JournalOperation::Delete)
	}).collect()
}

#[derive(Debug)]
pub struct JournalEra {
	file: PathBuf,
	mmap: Mmap,
	cache: HashMap<JournalSlice, JournalOperation<JournalSlice>>,
}

impl JournalEra {
	// TODO [ToDr] Data should be written to a file earlier (for instance when transaction is created).
	// Consider an API like this:
	// ```
	// let mut transaction = Transaction::new();
	// ...
	// let prepared = db.prepare(transaction); // writes to a file (doesn't require write access to DB)
	// db.apply(prepared); // actually insert to db (requires write access)
	// ```
	fn create<P: AsRef<Path>>(file_path: P, transaction: &Transaction) -> Result<JournalEra> {
		let hash = sha3_256(transaction.raw());
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create_new(true)
			.open(&file_path)?;

		file.write_all(&hash)?;
		file.write_all(transaction.raw())?;
		file.flush()?;

		Self::open(file_path)
	}

	fn open<P: AsRef<Path>>(file: P) -> Result<JournalEra> {
		let mmap = Mmap::open_path(&file, Protection::Read)?;
		let cache = {
			let checksum = unsafe { &mmap.as_slice()[..CHECKSUM_SIZE] };
			let data = unsafe { &mmap.as_slice()[CHECKSUM_SIZE..] };
			let hash = sha3_256(data);
			if hash != checksum {
				return Err(ErrorKind::CorruptedJournal(
					file.as_ref().into(),
					format!(
						"Expected: {:02x}, Got: {:02x}",
						hash.as_hex(),
						checksum.as_hex(),
					)
				).into());
			}

			unsafe { cache_memory(data) }
		};

		let era = JournalEra {
			file: file.as_ref().to_path_buf(),
			mmap,
			cache,
		};

		Ok(era)
	}

	fn get<'a>(&'a self, key: &[u8]) -> Option<JournalOperation<&'a [u8]>> {
		let key = JournalSlice::new(key);

		match self.cache.get(&key) {
			None => None,
			Some(&JournalOperation::Insert(ref value)) => Some(JournalOperation::Insert(unsafe { value.as_slice() })),
			Some(&JournalOperation::Delete) => Some(JournalOperation::Delete),
		}
	}

	fn operations(&self) -> BTreeSet<Operation> {
		let mut ops = BTreeSet::new();

		for o in unsafe { OperationsIterator::new(&self.mmap.as_slice()[CHECKSUM_SIZE..]) } {
			ops.replace(o);
		}

		ops
	}

	/// Returns an iterator over era entries
	pub fn iter(&self) -> btree_set::IntoIter<Operation> {
		self.operations().into_iter()
	}

	/// Deletes underlying file
	pub fn delete(self) -> Result<()> {
		fs::remove_file(self.file)?;
		Ok(())
	}
}

mod dir {
	use std::fs::read_dir;
	use std::path::{Path, PathBuf};
	use error::{ErrorKind, Result};

	const ERA_EXTENSION: &str = ".era";

	pub fn era_files<P: AsRef<Path>>(dir: P) -> Result<Vec<PathBuf>> {
		if !dir.as_ref().is_dir() {
			return Err(ErrorKind::InvalidJournalLocation(dir.as_ref().into()).into());
		}

		let mut era_files: Vec<_> = read_dir(dir)?
			.collect::<::std::result::Result<Vec<_>, _>>()?
			.into_iter()
			.filter(|entry| entry.file_name().to_string_lossy().ends_with(ERA_EXTENSION))
			.map(|entry| entry.path())
			.collect();

		era_files.sort();

		let mut last = None;

		for era in &era_files {
			let idx = era_index(era)?;
			match last.take() {
				Some(era) if idx == era + 1 => {},
				None => {},
				_ => {
					return Err(ErrorKind::JournalEraMissing(idx).into());
				}
			}
			last = Some(idx);
		}

		Ok(era_files)
	}

	fn era_index<P: AsRef<Path>>(path: P) -> Result<u64> {
		let path = path.as_ref()
			.file_stem()
			.ok_or(ErrorKind::CorruptedJournal(
				path.as_ref().into(),
				"No file stem found".to_string()))?
			.to_string_lossy();
		Ok(1u64 + path.parse::<u64>()?)
	}

	pub fn next_era_index<P: AsRef<Path>>(files: &[P]) -> Result<u64> {
		match files.last() {
			Some(path) => era_index(path),
			None => Ok(0),
		}
	}

	pub fn next_era_filename<P: AsRef<Path>>(dir: P, next_index: u64) -> PathBuf {
		let mut dir = dir.as_ref().to_path_buf();
		dir.push(format!("{}{}", next_index, ERA_EXTENSION));
		dir
	}

	#[cfg(test)]
	mod tests {
		use super::era_index;

		#[test]
		fn test_era_index() {
			assert_eq!(1u64, era_index("0.era").unwrap());
			assert_eq!(1u64, era_index("/path/0.era").unwrap());
			assert!(era_index("a.era").is_err());
			assert!(era_index("/path/..").is_err());
		}
	}
}

#[derive(Debug)]
pub struct Journal {
	dir: PathBuf,
	eras: VecDeque<JournalEra>,
	next_era_index: u64,
}

impl Journal {
	pub fn open<P: AsRef<Path>>(jdir: P) -> Result<Self> {
		let era_files = dir::era_files(&jdir)?;
		let next_era_index = dir::next_era_index(&era_files)?;

		let eras = era_files.into_iter()
			.map(JournalEra::open)
			.collect::<Result<VecDeque<_>>>()?;

		let journal = Journal {
			dir: jdir.as_ref().to_path_buf(),
			eras,
			next_era_index,
		};

		Ok(journal)
	}

	pub fn push(&mut self, transaction: &Transaction) -> Result<()> {
		let new_path = dir::next_era_filename(&self.dir, self.next_era_index);
		self.next_era_index += 1;

		let new_era = JournalEra::create(new_path, &transaction)?;
		self.eras.push_back(new_era);

		Ok(())
	}

	pub fn drain_front(&mut self, elems: usize) -> Drain<JournalEra> {
		self.eras.drain(..elems)
	}

	pub fn len(&self) -> usize {
		self.eras.len()
	}

	pub fn get<'a>(&'a self, key: &[u8]) -> Option<&'a [u8]> {
		for era in self.eras.iter().rev() {
			if let Some(operation) = era.get(&key) {
				return match operation {
					JournalOperation::Insert(insert) => Some(insert),
					JournalOperation::Delete => None,
				}
			}
		}

		None
	}

	/// Returns an iterator over the journal entries across all eras
	pub fn iter(&self) -> btree_set::IntoIter<Operation> {
		let mut ops = BTreeSet::new();
		for era in self.eras.iter() {
			// append should take the value from `era` for keys that are equal
			ops.append(&mut era.operations())
		}

		ops.into_iter()
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use self::tempdir::TempDir;
	use std::fs;
	use std::io::Write;
	use error::ErrorKind;
	use transaction::{Operation, Transaction};
	use super::{Journal, JournalEra, JournalOperation};

	#[test]
	fn test_era_create() {
		let temp = TempDir::new("test_era_create").unwrap();
		let mut path = temp.path().to_path_buf();
		path.push("file");

		let mut tx = Transaction::new(4);
		tx.insert(b"key1", b"value").unwrap();
		tx.insert(b"key2", b"value").unwrap();
		tx.insert(b"key3", b"value").unwrap();
		tx.insert(b"key2", b"value2").unwrap();
		tx.delete(b"key3").unwrap();

		let era = JournalEra::create(path, &tx).unwrap();
		assert_eq!(JournalOperation::Insert(b"value" as &[u8]), era.get(b"key1").unwrap());
		assert_eq!(JournalOperation::Insert(b"value2" as &[u8]), era.get(b"key2").unwrap());
		assert_eq!(JournalOperation::Delete, era.get(b"key3").unwrap());
		assert_eq!(None, era.get(b"key4"));
	}

	#[test]
	fn test_journal_new() {
		let temp = TempDir::new("test_journal_new").unwrap();

		let mut journal = Journal::open(temp.path()).unwrap();
		journal.push(&Transaction::new(1)).unwrap();
		journal.push(&Transaction::new(1)).unwrap();
		journal.push(&Transaction::new(1)).unwrap();
		assert_eq!(journal.len(), 3);

		journal.drain_front(2);

		assert_eq!(journal.len(), 1);
	}

	#[test]
	fn test_journal_iter() {
		let temp = TempDir::new("test_journal_iter").unwrap();

		let mut journal = Journal::open(temp.path()).unwrap();

		let mut tx1 = Transaction::new(4);
		tx1.insert(b"key1", b"value").unwrap();
		tx1.insert(b"key2", b"value").unwrap();
		tx1.insert(b"key3", b"value").unwrap();

		let mut tx2 = Transaction::new(4);
		tx2.insert(b"key2", b"value2").unwrap();
		tx2.delete(b"key3").unwrap();
		tx2.insert(b"key4", b"value4").unwrap();

		journal.push(&tx1).unwrap();
		journal.push(&tx2).unwrap();

		assert_eq!(journal.len(), 2);

		assert_eq!(
			journal.iter().collect::<Vec<_>>(),
			vec![
				Operation::Insert(b"key1" as &[u8], b"value" as &[u8]),
				Operation::Insert(b"key2" as &[u8], b"value2" as &[u8]),
				Operation::Delete(b"key3" as &[u8]),
				Operation::Insert(b"key4" as &[u8], b"value4" as &[u8])
			]
		);
	}

	#[test]
	fn should_detect_corrupted_era() {
		let temp = TempDir::new("test_era_create").unwrap();
		let mut path = temp.path().to_path_buf();
		path.push("file");

		let mut tx = Transaction::new(4);
		tx.insert(b"key1", b"value").unwrap();
		tx.insert(b"key2", b"value").unwrap();
		tx.insert(b"key3", b"value").unwrap();
		tx.insert(b"key2", b"value2").unwrap();
		tx.delete(b"key3").unwrap();
		let _ = JournalEra::create(&path, &tx).unwrap();

		// alter hash
		let mut file = fs::OpenOptions::new().write(true).open(&path).unwrap();
		file.write_all(&mut [1, 2, 3]).unwrap();
		file.flush().unwrap();

		// Try to open era
		assert_eq!(JournalEra::open(&path).unwrap_err().kind(), &ErrorKind::CorruptedJournal(
			path,
			"Expected: [69 53 c1 6d b6 8a 85 9a b9 d8 b3 da 13 1d ba 6b 2a 17 d9 84 8d bf 6e d4 c0 d6 64 5d b3 98 5d 0c], Got: [01 02 03 6d b6 8a 85 9a b9 d8 b3 da 13 1d ba 6b 2a 17 d9 84 8d bf 6e d4 c0 d6 64 5d b3 98 5d 0c]".into()
		));
	}
}
