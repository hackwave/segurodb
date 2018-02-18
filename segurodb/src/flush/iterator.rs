//! Flush file iterator

use byteorder::{ByteOrder, LittleEndian};

/// Idempotent operation can be applied multiple times without changing
/// the result beyond initial application.
#[derive(Debug, PartialEq)]
pub struct IdempotentOperation<'a> {
	pub offset: usize,
	pub data: &'a [u8],
}

/// Iterates over flush file's idempotent operations.
#[derive(Debug)]
pub struct IdempotentOperationIterator<'a> {
	data: &'a [u8],
}

impl<'a> IdempotentOperationIterator<'a> {
	pub fn new(data: &'a [u8]) -> Self {
		IdempotentOperationIterator {
			data,
		}
	}
}

impl<'a> Iterator for IdempotentOperationIterator<'a> {
	type Item = IdempotentOperation<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let offset = LittleEndian::read_u64(&self.data[0..8]) as usize;
		let data_len = LittleEndian::read_u32(&self.data[8..12]) as usize;
		let end = 12 + data_len;
		let result = IdempotentOperation {
			offset,
			data: &self.data[12..end],
		};

		self.data = &self.data[end..];
		Some(result)
	}
}

#[cfg(test)]
mod tests {
	use super::{IdempotentOperation, IdempotentOperationIterator};

	#[test]
	fn test_positive_operations_iterator1() {
		let data = &[
			5, 0, 0, 0, 0, 0, 0, 0,
			6, 0, 0, 0,
			1, 2, 3, 4, 5, 6,
		];

		let expected = IdempotentOperation {
			offset: 5,
			data: &[1, 2, 3, 4, 5, 6],
		};

		let mut iterator = IdempotentOperationIterator::new(data);
		assert_eq!(expected, iterator.next().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_positive_operations_iterator2() {
		let data = &[
			5, 0, 0, 0, 0, 0, 0, 0,
			6, 0, 0, 0,
			1, 2, 3, 4, 5, 6,
			20, 0, 0, 0, 0, 0, 0, 0,
			7, 0, 0, 0,
			1, 2, 3, 4, 5, 6, 7
		];

		let expected1 = IdempotentOperation {
			offset: 5,
			data: &[1, 2, 3, 4, 5, 6],
		};

		let expected2 = IdempotentOperation {
			offset: 20,
			data: &[1, 2, 3, 4, 5, 6, 7],
		};

		let mut iterator = IdempotentOperationIterator::new(data);
		assert_eq!(expected1, iterator.next().unwrap());
		assert_eq!(expected2, iterator.next().unwrap());
		assert!(iterator.next().is_none());
	}
}
