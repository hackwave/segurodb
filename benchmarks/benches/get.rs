#![feature(test)]
extern crate test;

extern crate lmdb_zero as lmdb;
extern crate segurodb;
extern crate rocksdb;
extern crate tempdir;

use test::Bencher;
use tempdir::TempDir;

#[bench]
fn segurodb_dummy_get(b: &mut Bencher) {
	use segurodb::{Database, ValuesLen, Options, Transaction};

	let temp = TempDir::new("segurodb_dummy_get").unwrap();
	let mut db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: 3,
		value_len: ValuesLen::Constant(3),
		..Default::default()
	}).unwrap();
	let mut tx = db.create_transaction();
	tx.insert("abc", "001");
	db.commit(&tx).unwrap();
	db.flush_journal(1).unwrap();

	b.iter(&mut || {
		db.get("abc").unwrap();
	});
}

#[bench]
fn segurodb_dummy_get_null(b: &mut Bencher) {
	use segurodb::{Database, ValuesLen, Options};

	let temp = TempDir::new("segurodb_dummy_get_null").unwrap();
	let db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: 3,
		value_len: ValuesLen::Constant(3),
		..Default::default()
	}).unwrap();

	b.iter(&mut || {
		db.get("abc").unwrap();
	});
}

#[bench]
fn rocksdb_dummy_get(b: &mut Bencher) {
	use rocksdb::DB;

	let temp = TempDir::new("rocksdb_dummy_get").unwrap();
	let db = DB::open_default(temp.path()).unwrap();
	db.put(b"abc", b"001").unwrap();

	b.iter(&mut || {
		db.get(b"abc").unwrap();
	});
}

#[bench]
fn rocksdb_dummy_get_null(b: &mut Bencher) {
	use rocksdb::DB;

	let temp = TempDir::new("rocksdb_dummy_get_null").unwrap();
	let db = DB::open_default(temp.path()).unwrap();

	b.iter(&mut || {
		db.get(b"abc").unwrap();
	});
}

#[bench]
fn lmdb_dummy_get(b: &mut Bencher) {
	use lmdb::{EnvBuilder, open, Database, DatabaseOptions, WriteTransaction, ReadTransaction, put};

	let temp = TempDir::new("lmdb_dummy_get").unwrap();
	let env_builder = EnvBuilder::new().unwrap();
	let env = unsafe { env_builder.open(&temp.path().to_owned().display().to_string(), open::Flags::empty(), 0o600).unwrap() };
	let db = Database::open(&env, None, &DatabaseOptions::defaults()).unwrap();
	let wt = WriteTransaction::new(&env).unwrap();
	{
		let mut accessor = wt.access();
		accessor.put(&db, b"abc", b"001", put::Flags::empty()).unwrap();
	}
	wt.commit().unwrap();

	b.iter(&mut || {
		let rt = ReadTransaction::new(&env).unwrap();
		{
			let accessor = rt.access();
			let _: &[u8; 3] = accessor.get(&db, b"abc").unwrap();
		}
	});
}

#[bench]
fn lmdb_dummy_get_null(b: &mut Bencher) {
	use lmdb::{EnvBuilder, open, Database, DatabaseOptions, ReadTransaction};

	let temp = TempDir::new("lmdb_dummy_get_null").unwrap();
	let env_builder = EnvBuilder::new().unwrap();
	let env = unsafe { env_builder.open(&temp.path().to_owned().display().to_string(), open::Flags::empty(), 0o600).unwrap() };
	let db = Database::open(&env, None, &DatabaseOptions::defaults()).unwrap();

	b.iter(&mut || {
		let rt = ReadTransaction::new(&env).unwrap();
		{
			let accessor = rt.access();
			let _: Result<&[u8; 3], _> = accessor.get(&db, b"abc");
		}
	});
}
