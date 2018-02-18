use byteorder::{LittleEndian, ByteOrder};

use field::view::FieldsView;

/// Optional size of header for variable-len records.
pub const HEADER_SIZE: usize = 4;

/// Value size
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ValueSize {
	/// Variable record size (needs to be read from header).
	Variable,
	/// Constant record size.
	Constant(usize),
}

/// A view onto database record.
#[derive(Debug, PartialEq)]
pub struct Record<'a> {
	key: &'a [u8],
	value: FieldsView<'a>,
	len: usize,
}

impl<'a> Record<'a> {
	/// Creates new record given the data slice, field body and value and key size.
	pub fn new(data: &'a [u8], field_body_size: usize, value_size: ValueSize, key_size: usize) -> Self {
		assert!(key_size <= field_body_size);

		let view = FieldsView::new(data, field_body_size);
		let (key, rest) = view.split_at(key_size);
		let key = key.raw_slice().expect("only returns None when addressed value isn't stored in a single field; \
										  keys are always stored in a single field; qed");

		match value_size {
			ValueSize::Constant(value_size) => {
				let (value, _) = rest.split_at(value_size);

				Record { key, value, len: value_size }
			},
			ValueSize::Variable => {
				let (header, rest) = rest.split_at(HEADER_SIZE);
				let value_len = Self::read_value_len(header) as usize;
				let (value, _) = rest.split_at(value_len);

				Record { key, value, len: value_len }
			}
		}
	}

	#[inline]
	pub(crate) fn extract_key(data: &'a [u8], field_body_size: usize, key_size: usize) -> FieldsView<'a> {
		FieldsView::with_options(data, field_body_size, 0, key_size)
	}

	fn read_value_len(field: FieldsView<'a>) -> u32 {
		let mut data = [0; HEADER_SIZE];
		field.copy_to_slice(&mut data);
		LittleEndian::read_u32(&data)
	}

	/// Returns record's key.
	pub fn key(&self) -> &'a [u8] {
		self.key
	}

	/// Returns true of record value is equal to given slice.
	pub fn value_is_equal(&self, slice: &[u8]) -> bool {
		self.value == slice
	}

	/// Returns underlying value if it is a continuous slice of memory,
	/// otherwise returns None.
	pub fn value_raw_slice(&self) -> Option<&'a [u8]> {
		self.value.raw_slice()
	}

	/// Reads value to given slice.
	/// Panics if the size does not match.
	pub fn read_value(&self, slice: &mut [u8]) {
		self.value.copy_to_slice(slice);
	}

	/// Returns record value length.
	pub fn value_len(&self) -> usize {
		self.len
	}
}

#[cfg(test)]
mod tests {
	use super::{Record, ValueSize};
	use field;

	#[test]
	fn test_extract_key() {
		let body_size = 8;
		let key_size = 3;
		let data = [
			1, 0xfa, 0xfb, 0xfc, 1, 2, 3, 4, 5,
			1, 0xfd, 0xfe, 0xff, 6, 7, 8, 9, 10,
		];

		assert_eq!(Record::extract_key(&data, body_size, key_size), &[0xfa, 0xfb, 0xfc]);
		assert_eq!(Record::extract_key(&data[body_size + field::HEADER_SIZE..], body_size, key_size), &[0xfd, 0xfe, 0xff]);
	}

	#[test]
	fn test_constant_size_record() {
		let body_size = 8;
		let value_size = ValueSize::Constant(5);
		let key_size = 3;
		let data = [
			1, 0xfa, 0xfb, 0xfc, 1, 2, 3, 4, 5,
			1, 0xfd, 0xfe, 0xff, 6, 7, 8, 9, 10,
		];

		let record = Record::new(&data, body_size, value_size, key_size);
		let key = record.key();
		assert_eq!(key, [0xfa, 0xfb, 0xfc]);

		let mut value = [0; 5];
		assert_eq!(record.value_len(), 5);
		record.read_value(&mut value);
		assert_eq!(value, [1, 2, 3, 4, 5]);

		let record = Record::new(&data[body_size + field::HEADER_SIZE..], body_size, value_size, key_size);
		let key = record.key();
		assert_eq!(key, [0xfd, 0xfe, 0xff]);

		assert_eq!(record.value_len(), 5);
		record.read_value(&mut value);
		assert_eq!(value, [6, 7, 8, 9, 10]);
	}

	#[test]
	fn test_variable_size_record() {
		let body_size = 10;
		let value_size = ValueSize::Variable;
		let key_size = 2;
		let data = [
			1, 0xfa, 0xfb, 3, 0, 0, 0, 1, 2, 3, 99,
			1, 0xfc, 0xfd, 1, 0, 0, 0, 4, 0, 0, 0,
		];
		let mut value1 = [0; 3];
		let mut value2 = [0; 1];

		let record1 = Record::new(&data, body_size, value_size, key_size);
		let key1 = record1.key();
		assert_eq!(key1, [0xfa, 0xfb]);

		assert_eq!(record1.value_len(), 3);
		record1.read_value(&mut value1);
		assert_eq!(value1, [1, 2, 3]);

		let record2 = Record::new(&data[body_size + field::HEADER_SIZE..], body_size, value_size, key_size);
		let key2 = record2.key();
		assert_eq!(key2, [0xfc, 0xfd]);

		assert_eq!(record2.value_len(), 1);
		record2.read_value(&mut value2);
		assert_eq!(value2, [4]);
	}
}
