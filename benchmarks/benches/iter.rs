#![feature(test)]
extern crate test;

extern crate segurodb;
extern crate tempdir;
extern crate ethereum_types;
extern crate rand;

use test::Bencher;
use tempdir::TempDir;
use rand::{Rand, StdRng, SeedableRng};
use segurodb::{Database, ValuesLen, Options, Value};

type Address = ethereum_types::H160;

/// benchmarks a single iteration step (`.next()`) of the iterator
/// returned by database.iter() for a database of `n` entries
fn benchmark_database_iter_step(b: &mut Bencher, n: u32) {
	let temp = TempDir::new("segurodb_iter_step").unwrap();
	let mut db = Database::create(temp.path(), Options {
		journal_eras: 0,
		key_len: Address::len(),
		value_len: ValuesLen::Constant(0),
		key_index_bits: 20,
		..Default::default()
	}).unwrap();

	let mut rng = StdRng::from_seed(&[1, 9, 4]);

	let mut tx = db.create_transaction();
	for i in 0..n {
		let address = Address::rand(&mut rng);
		tx.insert(address, []).unwrap();
	}
	db.commit(&tx).unwrap();

	db.flush_journal(None).unwrap();

	let mut iterator = db.iter().unwrap();
	b.iter(&mut || {
		if let Some(result) = iterator.next() {
			let value = result.unwrap();
			test::black_box(value);
		} else {
			iterator = db.iter().unwrap();
		}
	});
}

#[bench]
fn segurodb_iter_step_for_1000000_entry_db(b: &mut Bencher) {
	benchmark_database_iter_step(b, 1000000);
}
