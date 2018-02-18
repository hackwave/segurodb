#![feature(test)]
extern crate test;

extern crate rand;
extern crate segurodb;
extern crate tempdir;
extern crate ethereum_types;

use test::Bencher;
use tempdir::TempDir;
use rand::{Rand, StdRng, SeedableRng};
use segurodb::{Database, ValuesLen, Options};

type Address = ethereum_types::H160;

/// inserts `insert_count` addresses and then benchmarks getting
/// a different one from a subset of size `get_subset_count`
/// on each benchmark iteration
fn benchmark_getting_subset_of_inserted_addresses(
		b: &mut Bencher,
		insert_count: usize,
		get_subset_count: usize,
		key_index_bits: u8
) {
		assert!(insert_count >= get_subset_count);

		let temp = TempDir::new(
				format!(
						"benchmark_segurodb_get_{}_from_{}_addresses",
						get_subset_count,
						insert_count
						).as_str())
				.unwrap();
		let mut db = Database::create(temp.path(), Options {
				journal_eras: 0,
				key_len: Address::len(),
				value_len: ValuesLen::Constant(0),
				key_index_bits: key_index_bits,
				..Default::default()
		}).unwrap();

		let mut addresses_subset: Vec<Address> = Vec::new();
		let mut rng = StdRng::from_seed(&[1, 9, 4]);

		let mut tx = db.create_transaction();
		for i in 0..insert_count {
				let address = Address::rand(&mut rng);
				tx.insert(address, []).unwrap();
				if i < get_subset_count {
						addresses_subset.push(address);
				}
		}
		db.commit(&tx).unwrap();

		db.flush_journal(1).unwrap();

		let mut i = 0;
		b.iter(&mut || {
				db.get(addresses_subset[i % get_subset_count]).unwrap();
				i += 1;
		});
}

const LARGE_KEY_INDEX_BITS: u8 = 20;

#[bench]
fn segurodb_from_1_inserted_get_1_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 1, 1, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_1000_inserted_get_random_of_1000_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 1000, 1000, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_100000_inserted_get_1_address(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 100000, 1, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_100000_inserted_get_random_of_1000_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 100000, 1000, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_100000_inserted_get_random_of_100000_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 100000, 100000, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_1000000_inserted_get_1_address(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 1000000, 1, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_1000000_inserted_get_random_of_1000_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 1000000, 1000, LARGE_KEY_INDEX_BITS);
}

#[bench]
fn segurodb_from_1000000_inserted_get_random_of_1000000_addresses(b: &mut Bencher) {
		benchmark_getting_subset_of_inserted_addresses(b, 1000000, 1000000, LARGE_KEY_INDEX_BITS);
}
