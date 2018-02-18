use prefix_tree::PrefixTree;

/// A structure holding database metadata information.
///
/// Currently we store a prefix tree for fast lookups and iterations
/// and number of bytes occupied by records for determining if
/// key prefix should be increased.
#[derive(Debug, Clone)]
pub struct Metadata {
	/// Database version
	pub db_version: u16,
	/// Number of bytes occupied by records
	/// NOTE: it does not include field headers!
	pub occupied_bytes: u64,
	/// Number of bits from the key used for prefix
	pub prefix_bits: u8,
	/// Prefix tree
	pub prefixes: PrefixTree,
	/// Prefixes with too many collisions that are stored separately
	pub collided_prefixes: PrefixTree,
}

impl Metadata {
	pub const DB_VERSION: u16 = 0;

	/// Notify that record was inserted.
	pub fn insert_record(&mut self, prefix: u32, len: usize) {
		self.occupied_bytes += len as u64;
		self.prefixes.insert(prefix);
	}

	/// Notify that record was removed.
	///
	/// We can't simply remove prefix from db, cause there might be
	/// more records with the same prefix in the database.
	pub fn remove_record(&mut self, len: usize) {
		self.occupied_bytes -= len as u64;
	}

	/// Notify that record was overwritten.
	pub fn update_record_len(&mut self, old_len: usize, new_len: usize) {
		self.occupied_bytes -= old_len as u64;
		self.occupied_bytes += new_len as u64;
	}

	/// Notify that a given prefix was marked as collided.
	///
	/// The prefix is added to `collided_prefixes` and removed from `prefixes`.
	pub fn add_prefix_collision(&mut self, prefix: u32) {
		self.collided_prefixes.insert(prefix);
		self.prefixes.remove(prefix);
	}

	/// Returns bytes representation of `Metadata`.
	pub fn as_bytes(&self) -> bytes::Metadata {
		bytes::Metadata::new(self)
	}
}

/// Metadata bytes manipulations.
pub mod bytes {
	use byteorder::{LittleEndian, ByteOrder};

	use prefix_tree::PrefixTree;

	/// Bytes representation of `Metadata`.
	pub struct Metadata<'a> {
		metadata: &'a super::Metadata,
	}

	impl<'a> Metadata<'a> {
		const VERSION_SIZE: usize = 2;
		const OCCUPIED_SIZE: usize = 8;

		/// Create new.
		pub fn new(metadata: &'a super::Metadata) -> Self {
			Metadata { metadata }
		}

		/// Copy bytes to given slice.
		/// Panics if the length are not matching.
		pub fn copy_to_slice(&self, data: &mut [u8]) {
			let prefix_leaves_offset = prefix_leaves_offset();
			let collided_prefix_leaves_offset = collided_prefix_leaves_offset(self.metadata.prefix_bits);

			let prefix_leaves = self.metadata.prefixes.leaves();
			data[prefix_leaves_offset..collided_prefix_leaves_offset].copy_from_slice(prefix_leaves);

			let collided_prefix_leaves = self.metadata.collided_prefixes.leaves();
			data[collided_prefix_leaves_offset..].copy_from_slice(collided_prefix_leaves);

			LittleEndian::write_u16(data, self.metadata.db_version);
			LittleEndian::write_u64(&mut data[Self::VERSION_SIZE..], self.metadata.occupied_bytes);
		}

		/// Return bytes length of the `Metadata`.
		pub fn len(&self) -> usize {
			len(self.metadata.prefix_bits)
		}
	}

	#[inline]
	pub fn prefix_leaves_offset() -> usize {
		Metadata::VERSION_SIZE + Metadata::OCCUPIED_SIZE
	}

	#[inline]
	pub fn collided_prefix_leaves_offset(prefix_bits: u8) -> usize {
		prefix_leaves_offset() + PrefixTree::leaf_data_len(prefix_bits)
	}

	/// Returns expected `Metadata` bytes len given prefix bits.
	pub fn len(prefix_bits: u8) -> usize {
		collided_prefix_leaves_offset(prefix_bits) + PrefixTree::leaf_data_len(prefix_bits)
	}

	/// Read `Metadata` from given slice.
	pub fn read(data: &[u8], prefix_bits: u8) -> super::Metadata {
		let db_version = LittleEndian::read_u16(&data[..Metadata::VERSION_SIZE]);
		let occupied_bytes = LittleEndian::read_u64(&data[Metadata::VERSION_SIZE..]);

		let prefix_leaves_offset = prefix_leaves_offset();
		let collided_prefix_leaves_offset = collided_prefix_leaves_offset(prefix_bits);

		let prefixes = PrefixTree::from_leaves(&data[prefix_leaves_offset..collided_prefix_leaves_offset], prefix_bits);
		let collided_prefixes = PrefixTree::from_leaves(&data[collided_prefix_leaves_offset..], prefix_bits);

		assert_eq!(db_version, super::Metadata::DB_VERSION);

		super::Metadata {
			db_version,
			occupied_bytes,
			prefix_bits,
			prefixes,
			collided_prefixes,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::bytes;
	use quickcheck::TestResult;

	quickcheck! {
		fn quickcheck_empty_metadata_roundtrips_from_and_to_buffer(
			key_index_bits: u8
		) -> TestResult {
			// key_index_bits == 0 is not allowed
			if key_index_bits == 0 {
				return TestResult::discard();
			}
			// limit search space to prevent test from running a really long time
			if key_index_bits > 16 {
				return TestResult::discard();
			}

			let initial_zeroed_buf: Vec<u8> = vec![0; bytes::len(key_index_bits)];
			let metadata = bytes::read(&initial_zeroed_buf[..], key_index_bits);
			assert_eq!(metadata.db_version, 0);
			assert_eq!(metadata.occupied_bytes, 0);

			let metadata_bytes = metadata.as_bytes();
			assert_eq!(metadata_bytes.len(), bytes::len(key_index_bits));

			let mut serialized_buf: Vec<u8> = vec![0; bytes::len(key_index_bits)];
			metadata_bytes.copy_to_slice(&mut serialized_buf[..]);

			assert_eq!(initial_zeroed_buf, serialized_buf);

			TestResult::passed()
		}
	}
}
