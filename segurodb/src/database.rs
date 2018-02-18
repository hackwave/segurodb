use std::cmp::Ordering;
use std::collections::{btree_set, BTreeMap};
use std::collections::btree_map::Entry;
use std::io::Write;
use std::path::{PathBuf, Path};
use std::{cmp, fs};
use std::fs::File;

use fs2::FileExt;
use memmap::{Mmap, Protection};
use itertools::Itertools;
use itertools::EitherOrBoth;

use collision::Collision;
use error::{ErrorKind, Result};
use find;
use find::RecordIterator;
use flush::Flush;
use journal::Journal;
use key::Key;
use metadata::{self, Metadata};
use options::{Options, InternalOptions};
use record::Record;
use transaction::{Operation, Transaction};

/// A database record value.
#[derive(Debug, PartialEq)]
pub enum Value<'a> {
	/// Raw (cached/journaled) data
	Raw(&'a [u8]),
	/// DB record
	Record(Record<'a>),
}

impl<'a> Value<'a> {
	/// Allocate a `Vec` with the value.
	pub fn to_vec(&self) -> Vec<u8> {
		match *self {
			Value::Raw(ref slice) => slice.to_vec(),
			Value::Record(ref record) => {
				let mut v = Vec::with_capacity(record.value_len());
				v.resize(record.value_len(), 0);
				record.read_value(&mut v);
				v
			},
		}
	}

	/// Returns value if it is a continuous slice of memory, otherwise returns None.
	pub fn as_slice(&self) -> Option<&[u8]> {
		match *self {
			Value::Raw(ref slice) => Some(slice),
			Value::Record(ref record) => record.value_raw_slice(),
		}
	}
}

impl<'a, T: AsRef<[u8]>> PartialEq<T> for Value<'a> {
	fn eq(&self, other: &T) -> bool {
		match *self {
			Value::Raw(slice) => slice == other.as_ref(),
			Value::Record(ref record) => record.value_is_equal(other.as_ref()),
		}
	}
}

impl<'a> From<Record<'a>> for Value<'a> {
	fn from(record: Record<'a>) -> Value<'a> {
		match record.value_raw_slice() {
			Some(raw) => Value::Raw(raw),
			None => Value::Record(record),
		}
	}
}

/// A top-level database API.
#[derive(Debug)]
pub struct Database {
	path: PathBuf,
	options: InternalOptions,
	journal: Journal,
	metadata: Metadata,
	metadata_mmap: Mmap,
	collisions: BTreeMap<u32, Collision>,
	mmap: Mmap,
	lock_file: File,
}

impl Database {
	const DB_FILE: &'static str = "data.db";
	const META_FILE: &'static str = "meta.db";
	const LOCK_FILE: &'static str = "LOCK";

	fn acquire_lock_file<P: AsRef<Path>>(path: P) -> Result<File> {
		let lock_file_path = path.as_ref().join(Self::LOCK_FILE);
		let lock_file = fs::OpenOptions::new()
			.write(true)
			.create(true)
			.open(&lock_file_path)?;
		lock_file.try_lock_exclusive().map_err(|_| ErrorKind::DatabaseLocked(lock_file_path))?;

		Ok(lock_file)
	}

	/// Creates new database at given location.
	pub fn create<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;

		// Create directories if necessary.
		fs::create_dir_all(&path)?;

		// Create/Acquire Lock file.
		let lock_file = Self::acquire_lock_file(&path)?;

		// Create DB file.
		{
			let db_file_path = path.as_ref().join(Self::DB_FILE);
			let mut file = fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&db_file_path)?;
			file.set_len(options.initial_db_size)?;
			file.flush()?;
		}

		// Create Metadata file.
		{
			let meta_file_path = path.as_ref().join(Self::META_FILE);
			let mut file = fs::OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&meta_file_path)?;
			file.set_len(metadata::bytes::len(options.external.key_index_bits) as u64)?;
			file.flush()?;
		}

		Self::open_internal(path, lock_file, options.external)
	}

	/// Opens an existing DB at given location.
	pub fn open<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
		let lock_file = Self::acquire_lock_file(&path)?;
		Self::open_internal(path, lock_file, options)
	}

	fn open_internal<P: AsRef<Path>>(path: P, lock_file: File, options: Options) -> Result<Self> {
		let options = InternalOptions::from_external(options)?;
		let journal = Journal::open(&path)?;

		let db_file_path = path.as_ref().join(Self::DB_FILE);
		let mut mmap = Mmap::open_path(db_file_path, Protection::ReadWrite)?;

		let meta_file_path = path.as_ref().join(Self::META_FILE);
		let mut metadata_mmap = Mmap::open_path(meta_file_path, Protection::ReadWrite)?;

		let mut metadata = metadata::bytes::read(unsafe { metadata_mmap.as_slice() }, options.external.key_index_bits);

		if let Some(flush) = Flush::open(path.as_ref(), options.external.key_index_bits)? {
			flush.flush(unsafe { mmap.as_mut_slice() }, unsafe { metadata_mmap.as_mut_slice() }, &mut metadata);
			mmap.flush()?;
			metadata_mmap.flush()?;
			flush.delete()?;
		}

		let mut collisions = BTreeMap::new();

		for prefix in metadata.collided_prefixes.prefixes_iter() {
			let collision_file = Collision::open(&path, prefix)?.expect(
				"prefix is declared as collided in metadata; \
				 collision file should exist; qed");

			collisions.insert(prefix, collision_file);
		}

		Ok(Database {
			path: path.as_ref().to_owned(),
			options,
			journal,
			metadata,
			metadata_mmap,
			mmap,
			collisions,
			lock_file,
		})
	}

	/// Create a new transaction.
	pub fn create_transaction(&self) -> Transaction {
		Transaction::new(self.options.external.key_len)
	}

	/// Commits changes in the transaction.
	pub fn commit(&mut self, tx: &Transaction) -> Result<()> {
		self.journal.push(tx)?;
		Ok(())
	}

	/// Flushes up to `max` excessive journal eras to the disk.
	pub fn flush_journal<T: Into<Option<usize>>>(&mut self, max: T) -> Result<()> {
		let len = self.journal.len();
		let max = max.into().unwrap_or(len);

		if len < self.options.external.journal_eras {
			return Ok(())
		}

		let to_flush = cmp::min(len - self.options.external.journal_eras, max);

		let prefix_bits = self.options.external.key_index_bits;
		let collisions = &mut self.collisions;

		for era in self.journal.drain_front(to_flush) {
			let flush = {
				let collided_prefixes = &self.metadata.collided_prefixes;

				// partition operations by whether they affect collided prefixes
				let (collided_operations, operations): (Vec<_>, Vec<_>) =
					era.iter().partition(|op| {
						let key = Key::new(op.key(), prefix_bits);
						collided_prefixes.has(key.prefix).unwrap_or(false)
					});

				// flush operations for collided prefixes to their own collision file
				for op in collided_operations {
					let key = Key::new(op.key(), prefix_bits);
					let collision = collisions.get_mut(&key.prefix).expect(
						"prefix is declared as collided; \
						 collision file should exist in collisions index; qed");

					collision.apply(op)?;
				}

				// create flush to data file for everything else
				Flush::new(
					&self.path,
					&self.options,
					unsafe { self.mmap.as_slice() },
					&self.metadata,
					operations.into_iter(),
				)?
			};

			era.delete()?;

			// TODO: metadata should be a single structure
			// updating self.metadata should happen after all calls
			// which may fail ("?")
			flush.flush(unsafe { self.mmap.as_mut_slice() }, unsafe { self.metadata_mmap.as_mut_slice() }, &mut self.metadata);
			self.mmap.flush()?;
			self.metadata_mmap.flush()?;
			flush.delete()?;
		}

		Ok(())
	}

	/// Lookup a value associated with given `key`.
	pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Value>> {
		let key = key.as_ref();
		if key.len() != self.options.external.key_len {
			return Err(ErrorKind::InvalidKeyLen(self.options.external.key_len, key.len()).into());
		}

		// check if the key-value pair is currently journaled
		if let Some(res) = self.journal.get(key) {
			return Ok(Some(Value::Raw(res)));
		}

		let field_body_size = self.options.field_body_size;
		let value_size = self.options.value_size;

		let key = Key::new(key, self.options.external.key_index_bits);

		// fetch from the collision file if this is a collided prefix
		if self.metadata.collided_prefixes.has(key.prefix).unwrap_or(false) {
			let collision = self.collisions.get(&key.prefix).expect(
				"prefix is declared as collided; \
				 collision file should exist in collisions index; qed");

			return Ok(collision.get(key.key)?.map(Value::Raw))
		}

		// check if there's any data stored on the data file for the given prefix
		if !self.metadata.prefixes.has(key.prefix).unwrap_or(false) {
			return Ok(None);
		}

		let offset = key.prefix as usize * self.options.record_offset;
		let data = unsafe { &self.mmap.as_slice()[offset..] };

		match find::find_record(data, field_body_size, value_size, key.key)? {
			find::RecordResult::Found(record) => Ok(Some(Value::from(record))),
			find::RecordResult::NotFound => Ok(None),
			find::RecordResult::OutOfRange => unimplemented!(),
		}
	}

	/// Returns an iterator over all the database key-value pairs ordered by key.
	pub fn iter(&self) -> Result<DatabaseIterator> {
		let record_collisions_iter = self.record_collisions_iter()?;
		let journal_iter = self.journal.iter();
		let pending = IteratorValue::None;

		Ok(DatabaseIterator { record_collisions_iter, journal_iter, pending })
	}

	/// Returns an iterator over only the database key-value pairs stored in the data file ordered
	/// by key (i.e. it doesn't include data from the journal or collision files).
	fn record_iter(&self) -> Result<RecordIterator> {
		let data = unsafe { &self.mmap.as_slice() };
		let occupied_prefixes_iter = self.metadata.prefixes.prefixes_iter();
		let field_body_size = self.options.field_body_size;
		let key_size = self.options.external.key_len;
		let value_size = self.options.value_size;

		let record_iter = find::iter(
			data,
			occupied_prefixes_iter,
			field_body_size,
			key_size,
			value_size,
		)?;

		Ok(record_iter)
	}

	// TODO: refactor to avoid boxed iterator
	/// Returns an iterator over the database key-value pairs stored in the data file and collision
	/// files.
	fn record_collisions_iter<'a>(&'a self) -> Result<Box<Iterator<Item=Result<(&'a [u8], Value<'a>)>> + 'a>> {
		let collided_records = self.collisions.values()
			.flat_map(|it| it.iter().ok()) // FIXME: swallowing errors here
			.flat_map(|it| it);

		let records = self.record_iter()?;

		Ok(Box::new(records.merge_join_by(collided_records, |r, c| {
			match (r, c) {
				(&Err(_), _) => Ordering::Less,
				(_, &Err(_)) => Ordering::Greater,
				(&Ok(ref r), &Ok(ref c)) => r.key().cmp(&c.0),
			}
		}).map(|either| {
			match either {
				EitherOrBoth::Left(Err(err)) => Err(err.into()),
				EitherOrBoth::Right(Err(err)) => Err(err),
				EitherOrBoth::Left(Ok(r)) => Ok((r.key(), Value::Record(r))),
				EitherOrBoth::Right(Ok(c)) => Ok((c.0, Value::Raw(c.1))),
				EitherOrBoth::Both(_, _) =>
					unreachable!("value exists in collision file; \
								  so cannot exist in data file; qed"),
			}
		})))
	}

	fn collisions(&self) -> Result<BTreeMap<u32, Vec<&[u8]>>> {
		let mut collisions: BTreeMap<u32, Vec<&[u8]>> = BTreeMap::new();

		for record in self.record_iter()? {
			let key = record?.key();

			let prefix = Key::new(key, self.options.external.key_index_bits).prefix;
			match collisions.entry(prefix) {
				Entry::Vacant(entry) => {
					entry.insert(vec![key]);
				},
				Entry::Occupied(mut entry) => {
					entry.get_mut().push(key);
				},
			}
		}

		// drop prefixes that have a number of collisions lower than the allowed threshold
		let collisions: BTreeMap<u32, Vec<&[u8]>> =
			collisions.into_iter().filter(|p| {
				p.1.len() >= self.options.external.max_prefix_collisions
			}).collect();

		Ok(collisions)
	}

	/// Finds prefixes that have a number of collisions higher than the configured threshold and
	/// moves all their data to a separate file (one file for each collided prefix). Returns a
	/// vector of collided prefixes (empty if no collisions have been found).
	pub fn compact(&mut self) -> Result<Vec<u32>> {
		let mut collision_files = Vec::new();
		let mut collided_prefixes = Vec::new();

		let (metadata, flush) = {
			let collisions = self.collisions()?;

			// create collision files and insert data from collided prefixes
			for (prefix, keys) in collisions.iter() {
				let mut collision_file = Collision::create(&self.path, *prefix)?;

				for key in keys {
					// FIXME: store a reference to the value in the return Map from collisions
					let value = self.get(&key)?.expect("The key has been returned by the iterator; qed");
					collision_file.insert(&key, value.as_slice().unwrap_or(&value.to_vec()))?;
				}

				collision_files.push(collision_file);
				collided_prefixes.push(*prefix);
			}

			// clone metadata and update it with collided prefixes but don't persist it
			let mut metadata = self.metadata.clone();
			for prefix in collided_prefixes.iter() {
				metadata.add_prefix_collision(*prefix);
			}

			// prepare flush to delete colliding keys but don't apply it
			let deletions = collisions.values().flat_map(|ks| ks).map(|k| Operation::Delete(k));

			let flush = Flush::new(
				&self.path,
				&self.options,
				unsafe { self.mmap.as_slice() },
				&metadata,
				deletions)?;

			(metadata, flush)
		};

		// persist metadata updated with collided prefixes
		// if we crash after this the flush will be applied on restart and the metadata will
		// already be properly updated
		metadata.as_bytes().copy_to_slice(unsafe { self.metadata_mmap.as_mut_slice() });
		self.metadata_mmap.flush()?;

		// perform the flush and update metadata
		flush.flush(unsafe { self.mmap.as_mut_slice() }, unsafe { self.metadata_mmap.as_mut_slice() }, &mut self.metadata);
		self.mmap.flush()?;
		self.metadata_mmap.flush()?;
		flush.delete()?;

		// update collisions index
		for collision_file in collision_files {
			let prev = self.collisions.insert(collision_file.prefix(), collision_file);
			assert!(prev.is_none());
		}

		Ok(collided_prefixes)
	}
}

impl Drop for Database {
	fn drop(&mut self) {
		let _ = self.lock_file.unlock();
	}
}

#[derive(Debug)]
enum IteratorValue<'a> {
	None,
	Journal(Operation<'a>),
	DB((&'a [u8], Value<'a>)),
}

impl<'a> IteratorValue<'a> {
	fn take(&mut self) -> Self {
		::std::mem::replace(self, IteratorValue::None)
	}
}

pub struct DatabaseIterator<'a> {
	journal_iter: btree_set::IntoIter<Operation<'a>>,
	record_collisions_iter: Box<Iterator<Item=Result<(&'a [u8], Value<'a>)>> + 'a>,
	pending: IteratorValue<'a>,
}

impl<'a> Iterator for DatabaseIterator<'a> {
	type Item = Result<(&'a [u8], Value<'a>)>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let (operation, record) = match self.pending.take() {
				IteratorValue::None => {
					let j = self.journal_iter.next().map_or(IteratorValue::None, IteratorValue::Journal);
					let db = match self.record_collisions_iter.next() {
						None => IteratorValue::None,
						Some(Ok(r)) => IteratorValue::DB(r),
						Some(Err(err)) => {
							self.pending = j;
							return Some(Err(err.into()));
						},
					};

					(j, db)
				},
				j @ IteratorValue::Journal(_) => {
					let db = match self.record_collisions_iter.next() {
						None => IteratorValue::None,
						Some(Ok(r)) => IteratorValue::DB(r),
						Some(Err(err)) => {
							self.pending = j;
							return Some(Err(err.into()));
						},
					};

					(j, db)
				},
				db @ IteratorValue::DB(_) => {
					let j = self.journal_iter.next().map_or(IteratorValue::None, IteratorValue::Journal);

					(j, db)
				},
			};

			#[inline]
			// returns `None` if the operation is a `Delete` and we should skip to the next value
			fn handle_journal_operation<'a>(o: Operation<'a>) -> Option<Result<(&'a [u8], Value<'a>)>> {
				match o {
					Operation::Delete(_) => {
						None
					},
					Operation::Insert(key, value) => {
						Some(Ok((key, Value::Raw(value))))
					},
				}
			}

			match (operation, record) {
				(IteratorValue::Journal(o), IteratorValue::None) => {
					match handle_journal_operation(o) {
						None => continue,
						s => return s,
					};
				},
				(IteratorValue::None, IteratorValue::DB(r)) => {
					return Some(Ok(r))
				},
				(IteratorValue::Journal(o), IteratorValue::DB(r)) => {
					let ord = (*r.0).partial_cmp(o.key()).expect(
						"only returns None when compared keys don't have the same size; \
						 all keys should have the same size; qed");

					match ord {
						Ordering::Equal => {
							match handle_journal_operation(o) {
								None => continue,
								s => return s,
							};
						},
						Ordering::Greater => {
							self.pending = IteratorValue::DB(r);

							match handle_journal_operation(o) {
								None => continue,
								s => return s,
							};
						},
						Ordering::Less => {
							self.pending = IteratorValue::Journal(o);
							return Some(Ok(r))
						},
					};
				},
				(IteratorValue::None, IteratorValue::None) => return None,
				(o, r) => unreachable!("operation: {:?}, record: {:?}", o, r)
			};
		}
	}
}

#[cfg(test)]
mod tests {
	extern crate tempdir;

	use super::{Database, Options};
	use options::ValuesLen;
	use error::ErrorKind;
	use quickcheck::TestResult;

	#[test]
	fn create_insert_and_query() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			value_len: ValuesLen::Constant(3),
			..Default::default()
		}).unwrap();

		let mut tx = db.create_transaction();
		tx.insert("abc", "xyz").unwrap();
		tx.insert("cde", "123").unwrap();

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"xyz");
		assert_eq!(db.get("cde").unwrap().unwrap(), b"123");

		// Another transaction
		let mut tx = db.create_transaction();
		tx.insert("abc", "456").unwrap();
		tx.delete("cde").unwrap();

		db.commit(&tx).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"456");
		assert_eq!(db.get("cde").unwrap(), None); // from DB

		// Flush journal and fetch everything from DB.
		db.flush_journal(2).unwrap();

		assert_eq!(db.get("abc").unwrap().unwrap(), b"456");
		assert_eq!(db.get("cde").unwrap(), None);
	}

	#[test]
	fn validate_key_length_at_insert() {
		let temp = tempdir::TempDir::new("validate_key_length").unwrap();

		let db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			..Default::default()
		}).unwrap();

		let mut tx = db.create_transaction();
		assert_eq!(*tx.insert("abcdef", "456").unwrap_err().kind(), ErrorKind::InvalidKeyLen(3, 6));
	}

	#[test]
	fn should_validate_key_length() {
		let temp = tempdir::TempDir::new("create_insert_and_query").unwrap();

		let db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			..Default::default()
		}).unwrap();

		assert_eq!(*db.get("a").unwrap_err().kind(), ErrorKind::InvalidKeyLen(3, 1));
	}

	#[test]
	fn test_same_key_operation_ordering() {
		let temp = tempdir::TempDir::new("test_fail").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			..Default::default()
		}).unwrap();

		let mut tx = db.create_transaction();
		tx.insert("abc", "123").unwrap();
		tx.delete("abc").unwrap();

		db.commit(&tx).unwrap();
		db.flush_journal(1).unwrap();

		assert_eq!(db.get("abc").unwrap(), None);
	}

	#[test]
	fn test_iter() {
		let temp = tempdir::TempDir::new("test_iter").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			value_len: ValuesLen::Constant(3),
			..Default::default()
		}).unwrap();

		let mut tx1 = db.create_transaction();
		tx1.insert("abc", "123").unwrap();
		tx1.insert("def", "467").unwrap();
		tx1.insert("ghi", "zzz").unwrap();

		db.commit(&tx1).unwrap();
		db.flush_journal(1).unwrap();

		let mut tx2 = db.create_transaction();
		tx2.insert("jkl", "999").unwrap();
		tx2.insert("def", "333").unwrap();
		tx2.insert("pqr", "aaa").unwrap();
		tx2.delete("ghi").unwrap();

		db.commit(&tx2).unwrap();

		let records = db.iter().unwrap().map(|item| {
			let (k, v) = item.unwrap();
			(::std::str::from_utf8(&k).unwrap().to_string(),
			 ::std::str::from_utf8(&v.to_vec()).unwrap().to_string())
		});

		let expected = vec![
			("abc", "123"),
			("def", "333"),
			("jkl", "999"),
			("pqr", "aaa"),
		];

		assert_eq!(
			records.collect::<Vec<_>>(),
			expected.iter().map(|x| (x.0.to_string(), x.1.to_string())).collect::<Vec<_>>()
		);
	}

	#[test]
	fn test_iter_collisions() {
		let temp = tempdir::TempDir::new("test_iter_collisions").unwrap();

		let mut db = Database::create(temp.path(), Options {
			journal_eras: 0,
			key_len: 3,
			value_len: ValuesLen::Constant(3),
			max_prefix_collisions: 3,
			..Default::default()
		}).unwrap();

		let mut tx = db.create_transaction();

		let data = vec![
			("aaa", "001"),
			("aab", "002"),
			("aac", "003"),
			("hhh", "004"),
			("zzz", "005"),
		];

		for record in data.iter() {
			let (k, v) = *record;
			tx.insert(k, v).unwrap();
		}

		db.commit(&tx).unwrap();
		db.flush_journal(1).unwrap();

		assert_eq!(db.compact().unwrap(), vec![97]);

		let records = db.iter().unwrap().map(|item| {
			let (k, v) = item.unwrap();
			(::std::str::from_utf8(&k).unwrap().to_string(),
			 ::std::str::from_utf8(&v.to_vec()).unwrap().to_string())
		});

		assert_eq!(
			records.collect::<Vec<_>>(),
			data.iter().map(|x| (x.0.to_string(), x.1.to_string())).collect::<Vec<_>>()
		);
	}

	#[test]
	fn should_validate_exclusive_access() {
		let temp = tempdir::TempDir::new("exclusive_access").unwrap();

		{

			// Acquire lock
			let _db = Database::create(temp.path(), Default::default());
			// attempt to open again
			assert!(matches!(
				Database::open(temp.path(), Default::default()).unwrap_err().kind(),
				&ErrorKind::DatabaseLocked(_)
			));
		}

		{
			// Acquire lock
			let _db = Database::open(temp.path(), Default::default());
			// attempt to create
			assert!(matches!(
				Database::create(temp.path(), Default::default()).unwrap_err().kind(),
				&ErrorKind::DatabaseLocked(_)
			));
		}

		assert!(Database::open(temp.path(), Default::default()).is_ok());
	}

	quickcheck! {
		fn quickcheck_can_get_inserted_value(key: Vec<u8>, value: Vec<u8>, key_index_bits: u8) -> TestResult {
			// else we get something like:
			// Error(InvalidOptions("key_index_bits", "53 is greater than key length: 24")
			if key_index_bits as usize > key.len() {
				return TestResult::discard();
			}
			// else we get something like:
			// Error(InvalidOptions("key_index_bits", "must not be 0."))
			if key_index_bits == 0 {
				return TestResult::discard();
			}
			// limit search space to prevent tests from taking forever
			if key.len() > 16 {
				return TestResult::discard();
			}
			if value.len() > 16 {
				return TestResult::discard();
			}

			let temp = tempdir::TempDir::new("quickcheck_can_get_inserted_value").unwrap();
			let mut db = Database::create(temp.path(), Options {
				journal_eras: 0,
				key_len: key.len(),
				key_index_bits: key_index_bits,
				value_len: ValuesLen::Constant(value.len()),
				..Default::default()
			}).unwrap();

			let mut tx = db.create_transaction();
			tx.insert(key.clone(), value.clone()).unwrap();

			db.commit(&tx).unwrap();
			db.flush_journal(None).unwrap();

			TestResult::from_bool(db.get(key).unwrap().unwrap() == value)
		}
	}
}
