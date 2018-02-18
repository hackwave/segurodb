use bit_vec::BitVec;

/// Represents a tree of occupied prefixes.
/// Each leaf in the tree is occupancy state of one of the possible prefixes.
///
/// The structure allows you to efficiently:
/// - check if some prefix is occupied
/// - iterate over occupied prefixes
#[derive(Debug, Clone)]
pub struct PrefixTree {
	tree: BitVec<u8>,
	prefix_bits: u8,
}

impl PrefixTree {
	#[inline]
	fn leaf_index(leaf: u32, prefix_bits: u8) -> usize {
		leaf as usize + (1 << prefix_bits)
	}

	#[inline]
	/// size of the byte slice the function `leaves` will return
	pub fn leaf_data_len(prefix_bits: u8) -> usize {
		((1 << prefix_bits) + 7) >> 3
	}

	/// Creates empty `PrefixTree` for given `prefix_bits`.
	pub fn new(prefix_bits: u8) -> Self {
		let size = 2 << prefix_bits;
		let mut tree = BitVec::default();
		tree.grow(size, false);

		PrefixTree {
			tree,
			prefix_bits,
		}
	}

	/// Re-constructs the tree from leaves (a bit vector of occupied prefixes).
	pub fn from_leaves(data: &[u8], prefix_bits: u8) -> Self {
		assert_eq!(data.len(), Self::leaf_data_len(prefix_bits));
		let mut tree = Self::new(prefix_bits);
		for (idx, byte) in data.iter().enumerate() {
			let mut current = 1;
			for i in 0..8 {
				if byte & current == current {
					tree.insert((idx * 8 + i) as u32);
				}
				current <<= 1;
			}
		}
		tree
	}

	/// Tests if given prefix is occupied.
	/// Returns `None` in case the prefix is out of range.
	pub fn has(&self, prefix: u32) -> Option<bool> {
		self.tree.get(Self::leaf_index(prefix, self.prefix_bits))
	}

	/// Marks given `prefix` as occupied.
	/// Panics in case `prefix` is out of range.
	pub fn insert(&mut self, prefix: u32) {
		let mut idx = Self::leaf_index(prefix, self.prefix_bits);
		self.tree.set(idx, true);

		while idx > 1 {
			idx = idx >> 1;
			self.tree.set(idx, true);
		}
	}

	/// Marks given `prefix` as empty.
	/// Panics in case `prefix` is out of range.
	pub fn remove(&mut self, prefix: u32) {
		let mut idx = Self::leaf_index(prefix, self.prefix_bits);
		self.tree.set(idx, false);

		loop {
			if idx <= 1 { break; };
			let sibling_idx = if idx % 2 == 0 { idx + 1 } else { idx - 1 };
			let sibling_set = self.tree.get(sibling_idx).unwrap_or(false);
			if sibling_set { break; };

			idx = idx >> 1;
			self.tree.set(idx, false);
		}
	}

	/// Returns bytes representation of the entire tree.
	pub fn bytes(&self) -> &[u8] {
		self.tree.storage()
	}

	/// Returns bytes representation of leaves (a bit vector of occupied prefixes).
	pub fn leaves(&self) -> &[u8] {
		let leaf_index = Self::leaf_index(0, self.prefix_bits);
		&self.bytes()[leaf_index / 8..]
	}

	/// Returns an iterator of occupied prefixes.
	pub fn prefixes_iter(&self) -> OccupiedPrefixesIterator {
		OccupiedPrefixesIterator {
			tree: &self.tree,
			idx: 0,
			first_leaf_idx: Self::leaf_index(0, self.prefix_bits),
		}
	}

	/// Returns current prefix bits.
	pub fn prefix_bits(&self) -> u8 {
		self.prefix_bits
	}
}

/// An occupied prefixes iterator.
/// Quickly traverses the tree and returns only prefixes that are occupied.
pub struct OccupiedPrefixesIterator<'a> {
	tree: &'a BitVec<u8>,
	idx: usize,
	first_leaf_idx: usize,
}

impl<'a> OccupiedPrefixesIterator<'a> {
	fn next_idx(&self, mut idx: usize) -> Option<usize> {
		let mut go_back = false;
		if idx % 2 == 1 {
			go_back = true;
		} else {
			idx += 1;
		}

		loop {
			if !go_back {
				if idx >= self.first_leaf_idx {
					// found leaf
					if self.tree.get(idx).unwrap() {
						return Some(idx);
					} else if idx % 2 == 0 {
						return Some(idx + 1);
					}
				} else {
					if self.tree.get(idx).unwrap() {
						// descent
						idx <<= 1;
						continue;
					} else if idx % 2 == 0 {
						// check other branch
						idx += 1;
						continue;
					}
				}
			}

			// go back
			while idx % 2 == 1 {
				idx >>= 1;
			}
			idx += 1;
			go_back = false;

			// returned back to the root so it means there is nothing left.
			if idx == 1 {
				return None;
			}
		}
	}
}

impl<'a> Iterator for OccupiedPrefixesIterator<'a> {
	type Item = u32;

	fn next(&mut self) -> Option<Self::Item> {
		let next_idx = self.next_idx(self.idx);

		match next_idx {
			Some(idx) => {
				self.idx = idx;
				Some((idx - self.first_leaf_idx) as u32)
			},
			None => None,
		}
	}
}


#[cfg(test)]
mod tests {
	use super::PrefixTree;

	#[test]
	fn test_prefix_tree() {
		let prefix_bits = 3;

		let mut tree = PrefixTree::new(prefix_bits);
		assert_eq!(tree.has(0), Some(false));
		assert_eq!(tree.has(1), Some(false));
		assert_eq!(tree.has(2), Some(false));
		assert_eq!(tree.has(3), Some(false));
		assert_eq!(tree.has(4), Some(false));
		assert_eq!(tree.has(5), Some(false));
		assert_eq!(tree.has(6), Some(false));
		assert_eq!(tree.has(7), Some(false));
		assert_eq!(tree.has(8), None);

		tree.insert(0);
		assert_eq!(tree.has(0), Some(true));

		assert_eq!(tree.bytes(), [0b00010110, 0b00000001]);
	}

	#[test]
	fn test_reading_prefix_tree() {
		let prefix_bits = 4;
		let data = [0b01111110, 0b00011111, 0b01010101, 0b00000001];

		let tree = PrefixTree::from_leaves(&data[2..], prefix_bits);
		for i in 0..16 {
			assert_eq!(tree.has(i), Some(i <= 8 && i % 2 == 0));
		}

		assert_eq!(tree.bytes(), data);
	}

	#[test]
	fn test_writing_and_reading_prefix_tree() {
		let prefix_bits = 4;

		let mut tree = PrefixTree::new(prefix_bits);
		for i in 0..16 {
			if i <= 8 && i % 2 == 0 {
				tree.insert(i);
			}
		}

		let bytes = tree.bytes();
		let tree = PrefixTree::from_leaves(&bytes[2..], prefix_bits);
		for i in 0..16 {
			assert_eq!(tree.has(i), Some(i <= 8 && i % 2 == 0));
		}
		assert_eq!(tree.bytes(), bytes);
	}

	#[test]
	fn test_prefixes_iterator() {
		let prefix_bits = 4;
		let data = [0b01010101, 0b00000001];
		let tree = PrefixTree::from_leaves(&data, prefix_bits);

		let mut it = tree.prefixes_iter();

		assert_eq!(it.next(), Some(0));
		assert_eq!(it.next(), Some(2));
		assert_eq!(it.next(), Some(4));
		assert_eq!(it.next(), Some(6));
		assert_eq!(it.next(), Some(8));
		assert_eq!(it.next(), None);
	}

	#[test]
	fn test_prefixes_iterator2() {
		let prefix_bits = 5;
		let mut tree = PrefixTree::new(prefix_bits);
		tree.insert(0);
		tree.insert(6);
		tree.insert(7);
		tree.insert(8);
		tree.insert(19);
		tree.insert(24);
		tree.insert(31);

		let mut it = tree.prefixes_iter();

		assert_eq!(it.next(), Some(0));
		assert_eq!(it.next(), Some(6));
		assert_eq!(it.next(), Some(7));
		assert_eq!(it.next(), Some(8));
		assert_eq!(it.next(), Some(19));
		assert_eq!(it.next(), Some(24));
		assert_eq!(it.next(), Some(31));
		assert_eq!(it.next(), None);
	}

	#[test]
	fn test_removing() {
		let prefix_bits = 4;
		let data = [0b01010101, 0b00000001];

		let mut tree = PrefixTree::from_leaves(&data, prefix_bits);
		tree.remove(0);
		tree.remove(2);
		tree.remove(4);
		tree.remove(6);
		assert_eq!(tree.bytes(), [0b01001010, 0b00010000, 0b0, 0b1]);

		tree.remove(8);
		assert_eq!(tree.bytes(), [0; 4]);
	}

	#[test]
	fn test_removing2() {
		let prefix_bits = 2;
		let data = vec![0; PrefixTree::leaf_data_len(prefix_bits)];
		let mut tree = PrefixTree::from_leaves(&data, prefix_bits);

		tree.insert(0);
		tree.insert(2);
		tree.remove(2);

		assert_eq!(
			tree.prefixes_iter().collect::<Vec<_>>(),
			vec![0]);
	}
}
