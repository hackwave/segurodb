#![feature(test)]
extern crate test;

/// benchmarks that compare looking up random existing and not existing
/// elements within n random elements for the segurodb prefix tree
/// and https://github.com/seiflotfy/rust-cuckoofilter.
/// note that this is not intended to show that prefix_tree is
/// "better" than cuckoofilter!
/// it simply has different tradeoffs.
/// the cuckoofilter uses much less space than prefix tree.
/// also the cuckoofilter is a counting filter
/// (https://en.wikipedia.org/wiki/Bloom_filter#Counting_filters)
/// whereas the prefix tree requires additional work to get rid of
/// false positives when elements are removed.
/// the prefix tree is ~20x faster than cuckoofilter
/// when getting a non existing element and ~6x when getting an existing element.
/// the benchmarks for cuckoofilter are kept to document that difference.
/// top priorities for segurodb are fast `get` and `iter`.
/// we are willing to trade in space and some complexity for that.
/// that's why we choose the prefix tree.


extern crate rand;
extern crate segurodb;
extern crate cuckoofilter;
extern crate tempdir;
extern crate itertools;

use test::Bencher;
use rand::{Rand, StdRng, SeedableRng};
use rand::distributions::{IndependentSample, Range};
use segurodb::PrefixTree;
use itertools::Itertools;

fn benchmark_prefix_tree_get(
	b: &mut Bencher,
	count: usize,
	key_index_bits: u8,
	check_number_which_was_not_inserted: bool
) {
	// collect `count` distinct random numbers
	let mut rng = StdRng::from_seed(&[1, 9, 4]);
	let random_range = Range::new(0, 10000000);
	let mut numbers: Vec<u32> = itertools::repeat_call(|| {
		random_range.ind_sample(&mut rng)
	})
	.unique()
	.take(count + if check_number_which_was_not_inserted { 1 } else { 0 })
	.collect();

	let mut prefix_tree = PrefixTree::new(key_index_bits);

	let number_which_was_not_inserted = if check_number_which_was_not_inserted {
		Some(numbers.pop().unwrap())
	} else {
		None
	};
	assert_eq!(numbers.len(), count);

	for n in numbers.iter() {
		prefix_tree.insert(*n);
	}

	if let Some(number) = number_which_was_not_inserted {
		b.iter(&mut || {
			prefix_tree.has(number).unwrap()
		});
	} else {
		let mut i = 0;
		b.iter(&mut || {
			i += 1;
			prefix_tree.has(numbers[i % numbers.len()]).unwrap()
		});
	}
}

#[bench]
fn prefix_tree_get_random_inserted_in_1(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 1, 32, false);
}

#[bench]
fn prefix_tree_get_random_inserted_in_10000(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 10000, 32, false);
}

#[bench]
fn prefix_tree_get_random_inserted_in_1000000(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 1000000, 32, false);
}

#[bench]
fn prefix_tree_get_not_inserted_in_1(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 1, 32, true);
}

#[bench]
fn prefix_tree_get_not_inserted_in_10000(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 10000, 32, true);
}

#[bench]
fn prefix_tree_get_not_inserted_in_1000000(b: &mut Bencher) {
	benchmark_prefix_tree_get(b, 1000000, 32, true);
}

fn benchmark_cuckoofilter_get(
	b: &mut Bencher,
	count: usize,
	check_number_which_was_not_inserted: bool
) {
	// collect `count` distinct random numbers
	let mut rng = StdRng::from_seed(&[1, 9, 4]);
	let random_range = Range::new(0, 10000000);
	let mut numbers: Vec<u32> = itertools::repeat_call(|| {
		random_range.ind_sample(&mut rng)
	})
	.unique()
	.take(count + if check_number_which_was_not_inserted { 1 } else { 0 })
	.collect();

	let mut filter = cuckoofilter::CuckooFilter::new();

	let number_which_was_not_inserted = if check_number_which_was_not_inserted {
		Some(numbers.pop().unwrap())
	} else {
		None
	};
	assert_eq!(numbers.len(), count);

	for n in numbers.iter() {
		filter.add(n);
	}

	if let Some(number) = number_which_was_not_inserted {
		b.iter(&mut || {
			filter.contains(&number)
		});
	} else {
		let mut i = 0;
		b.iter(&mut || {
			i += 1;
			filter.contains(&numbers[i % numbers.len()])
		});
	}
}

#[bench]
fn cuckoofilter_get_random_inserted_in_1(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 1, false);
}

#[bench]
fn cuckoofilter_get_random_inserted_in_10000(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 10000, false);
}

#[bench]
fn cuckoofilter_get_random_inserted_in_1000000(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 1000000, false);
}

#[bench]
fn cuckoofilter_get_not_inserted_in_1(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 1, true);
}

#[bench]
fn cuckoofilter_get_not_inserted_in_10000(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 10000, true);
}

#[bench]
fn cuckoofilter_get_not_inserted_in_1000000(b: &mut Bencher) {
	benchmark_cuckoofilter_get(b, 1000000, true);
}
