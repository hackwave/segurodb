extern crate tempdir;
extern crate segurodb;

use std::fs;
use tempdir::TempDir;
use segurodb::{Database, Options, ValuesLen};

#[derive(Debug)]
enum Action {
	Insert(&'static str, &'static str),
	Delete(&'static str),
	CommitAndFlush,
	AssertCompact(&'static [u32]),
	AssertEqual(&'static str, &'static str),
	AssertNone(&'static str),
}

use Action::*;

fn run_actions(db: &mut Database, actions: &[Action]) {
	let mut tx = db.create_transaction();

	for action in actions {
		println!("action: {:?}", action);
		match *action {
			Insert(key, value) => {
				tx.insert(key, value).unwrap();
			},
			Delete(key) => {
				tx.delete(key).unwrap();
			},
			CommitAndFlush => {
				db.commit(&tx).unwrap();
				tx = db.create_transaction();
				db.flush_journal(1).unwrap();
			},
			AssertCompact(expected_prefixes) => {
				assert_eq!(db.compact().unwrap(), expected_prefixes)
			},
			AssertEqual(key, expected_value) => {
				assert_eq!(db.get(key).unwrap().unwrap(), expected_value);
			},
			AssertNone(key) => {
				assert_eq!(db.get(key).unwrap(), None);
			},
		}
	}
}

macro_rules! db_test {
	($name: tt, $($actions: expr),*) => {
		#[test]
		fn $name() {
			let temp = TempDir::new(stringify!($name)).unwrap();

			let mut db = Database::create(temp.path(), Options {
				journal_eras: 0,
				key_len: 3,
				value_len: ValuesLen::Constant(3),
				..Default::default()
			}).unwrap();

			run_actions(&mut db, &[$($actions),*]);
		}
	}
}

db_test!(
	test_database_flush,
	Insert("abc", "001"),
	Insert("abe", "002"),
	Insert("cde", "003"),
	CommitAndFlush,
	AssertEqual("abc", "001"),
	AssertEqual("abe", "002"),
	AssertEqual("cde", "003"),
	Insert("abd", "004"),
	CommitAndFlush,
	AssertEqual("abc", "001"),
	AssertEqual("abe", "002"),
	AssertEqual("abd", "004"),
	AssertEqual("cde", "003"),
	Insert("abd", "005"),
	Delete("cde"),
	Delete("abc"),
	CommitAndFlush,
	AssertNone("abc"),
	AssertEqual("abe", "002"),
	AssertEqual("abd", "005"),
	AssertNone("cde")
);

db_test!(
	test_database_flush_shift_only_required1,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertEqual("bbb", "002"),
	Delete("aaa"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("bbb", "002")
);

db_test!(
	test_database_flush_shift_only_required2,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertEqual("bbb", "002"),
	Delete("aaa"),
	Insert("ccc", "003"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("bbb", "002"),
	AssertEqual("ccc", "003")
);

db_test!(
	test_delete_all1,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	CommitAndFlush,
	Delete("aaa"),
	Delete("bbb"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertNone("bbb")
);

db_test!(
	db_insert_db_start,
	Insert("\x0000", "000"),
	CommitAndFlush,
	AssertEqual("\x0000", "000")
);

db_test!(
	db_delete_and_insert_in_the_same_place1,
	Insert("aaa", "001"),
	CommitAndFlush,
	Insert("aab", "002"),
	Delete("aaa"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("aab", "002")
);

db_test!(
	db_delete_and_insert_in_the_same_place2,
	Insert("aab", "001"),
	CommitAndFlush,
	Insert("aaa", "002"),
	Delete("aab"),
	CommitAndFlush,
	AssertNone("aab"),
	AssertEqual("aaa", "002")
);

db_test!(
	db_delete_and_insert_in_the_same_place3,
	Insert("aaa", "001"),
	CommitAndFlush,
	Insert("aab", "002"),
	Delete("aaa"),
	Insert("bbb", "003"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("aab", "002"),
	AssertEqual("bbb", "003")
);

db_test!(
	db_delete_and_insert_in_the_same_place4,
	Insert("aaa", "001"),
	Insert("bbb", "003"),
	CommitAndFlush,
	Insert("aab", "002"),
	Delete("aaa"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("aab", "002"),
	AssertEqual("bbb", "003")
);

db_test!(
	db_delete_and_insert_in_the_same_place5,
	Insert("aab", "001"),
	CommitAndFlush,
	Insert("aaa", "002"),
	Delete("aab"),
	Insert("bbb", "003"),
	CommitAndFlush,
	AssertNone("aab"),
	AssertEqual("aaa", "002"),
	AssertEqual("bbb", "003")
);

db_test!(
	db_delete_and_insert_in_the_same_place6,
	Insert("aab", "001"),
	Insert("bbb", "003"),
	CommitAndFlush,
	Insert("aaa", "002"),
	Delete("aab"),
	CommitAndFlush,
	AssertNone("aab"),
	AssertEqual("aaa", "002"),
	AssertEqual("bbb", "003")
);

db_test!(
	db_delete_and_insert_after1,
	Insert("aaa", "001"),
	Insert("aab", "002"),
	Insert("aac", "003"),
	Insert("bbb", "004"),
	Insert("ccc", "005"),
	CommitAndFlush,
	Delete("aaa"),
	Delete("aac"),
	Insert("bbb", "006"),
	Insert("bbc", "007"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertEqual("aab", "002"),
	AssertNone("aac"),
	AssertEqual("bbb", "006"),
	AssertEqual("bbc", "007"),
	AssertEqual("ccc", "005")
);

db_test!(
	db_delete_forward_consumes_space1,
	Insert("aaa", "001"),
	Delete("aab"),
	Delete("bbb"),
	Insert("bbc", "002"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertEqual("bbc", "002"),
	AssertNone("aab"),
	AssertNone("bbb")
);

db_test!(
	db_delete_backward_consumes_space1,
	Insert("aaa", "001"),
	Insert("bbb", "002"),
	CommitAndFlush,
	Delete("bbb"),
	Delete("ccc"),
	Insert("ddd", "003"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertNone("bbb"),
	AssertNone("ccc"),
	AssertEqual("ddd", "003")
);

db_test!(
	db_delete_backward_consumes_space2,
	Insert("aaa", "001"),
	Insert("ccc", "002"),
	CommitAndFlush,
	Delete("bbb"),
	Delete("ccc"),
	Insert("ddd", "003"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertNone("bbb"),
	AssertNone("ccc"),
	AssertEqual("ddd", "003")
);

db_test!(
	db_delete_backward_consumes_space3,
	Insert("aaa", "001"),
	Insert("ddd", "002"),
	CommitAndFlush,
	Delete("bbb"),
	Delete("ddd"),
	Insert("fff", "003"),
	CommitAndFlush,
	AssertEqual("aaa", "001"),
	AssertNone("bbb"),
	AssertNone("ccc"),
	AssertNone("ddd"),
	AssertEqual("fff", "003")
);

db_test!(
	db_compact,
	Insert("aaa", "001"),
	Insert("aab", "002"),
	Insert("aac", "003"),
	Insert("aad", "004"),
	Insert("aae", "005"),
	Insert("aaf", "006"),
	Insert("zzz", "007"),
	Insert("ggg", "008"),
	CommitAndFlush,
	AssertCompact(&[97]),
	AssertEqual("aaa", "001"),
	AssertEqual("aab", "002"),
	AssertEqual("aac", "003"),
	AssertEqual("aad", "004"),
	AssertEqual("aae", "005"),
	AssertEqual("aaf", "006"),
	AssertEqual("zzz", "007"),
	AssertEqual("ggg", "008"),
	Insert("aaa", "000"),
	Insert("aag", "006"),
	CommitAndFlush,
	AssertCompact(&[]),
	AssertEqual("aaa", "000"),
	AssertEqual("aag", "006")
);

db_test!(
	db_compact_multiple,
	Insert("aaa", "001"),
	Insert("aab", "002"),
	Insert("aac", "003"),
	Insert("aad", "004"),
	Insert("aae", "005"),
	Insert("aaf", "006"),
	Insert("jaa", "007"),
	Insert("jab", "008"),
	Insert("jac", "009"),
	Insert("jad", "010"),
	Insert("jae", "011"),
	Insert("jaf", "012"),
	Insert("zzz", "013"),
	Insert("ggg", "014"),
	CommitAndFlush,
	AssertCompact(&[97, 106]),
	AssertEqual("aaa", "001"),
	AssertEqual("aab", "002"),
	AssertEqual("aac", "003"),
	AssertEqual("aad", "004"),
	AssertEqual("aae", "005"),
	AssertEqual("aaf", "006"),
	AssertEqual("jaa", "007"),
	AssertEqual("jab", "008"),
	AssertEqual("jac", "009"),
	AssertEqual("jad", "010"),
	AssertEqual("jae", "011"),
	AssertEqual("jaf", "012"),
	AssertEqual("zzz", "013"),
	AssertEqual("ggg", "014"),
	Insert("aaa", "000"),
	Insert("jaa", "000"),
	Insert("aag", "015"),
	Insert("jag", "016"),
	CommitAndFlush,
	AssertCompact(&[]),
	AssertEqual("aaa", "000"),
	AssertEqual("jaa", "000"),
	AssertEqual("aag", "015"),
	AssertEqual("jag", "016")
);

db_test!(
	db_flush_bug,
	Insert("aaa", "001"),
	Insert("aab", "002"),
	Insert("aac", "003"),
	Insert("aad", "004"),
	Insert("ddd", "008"),
	CommitAndFlush,
	Delete("aaa"),
	Delete("aab"),
	Delete("aac"),
	Delete("aad"),
	CommitAndFlush,
	AssertNone("aaa"),
	AssertNone("aab"),
	AssertNone("aac"),
	AssertNone("aad"),
	AssertEqual("ddd", "008")
);

#[test]
fn test_flush_recovery() {
	let temp = TempDir::new("flush_recovery").unwrap();
	// this flush file should apply insertions of 2 records
	// abc -> xyz
	// cde -> 123
	fs::copy("tests/flushes/flush_00.flush", temp.path().join("db.flush")).unwrap();

	let mut db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: 3,
		value_len: ValuesLen::Constant(3),
		..Default::default()
	}).unwrap();

	run_actions(&mut db, &[
		AssertEqual("abc", "xyz"),
		AssertEqual("cde", "123"),
	]);
}
