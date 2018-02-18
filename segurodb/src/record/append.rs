use std::{slice, io};
use std::io::Read;
use byteorder::{LittleEndian, ByteOrder};
use field::{Header, field_size};

struct RawRecordIterator<'a> {
	key: slice::Iter<'a, u8>,
	value: slice::Iter<'a, u8>,
	value_len: Option<io::Bytes<io::Cursor<[u8; 4]>>>,
}

impl<'a> RawRecordIterator<'a> {
	fn new(key: &'a [u8], value: &'a [u8], const_value: bool) -> Self {
		let value_len = if const_value {
			None
		} else {
			let mut value_len = [0u8; 4];
			LittleEndian::write_u32(&mut value_len, value.len() as u32);
			Some(io::Cursor::new(value_len).bytes())
		};

		RawRecordIterator {
			key: key.iter(),
			value: value.iter(),
			value_len,
		}
	}
}

impl<'a> Iterator for RawRecordIterator<'a> {
	type Item = u8;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(item) = self.key.next() {
			return Some(*item);
		}

		if let Some(ref mut value_len) = self.value_len {
			if let Some(item) = value_len.next() {
				return Some(item.expect("io::Bytes<io::Cursor<[u8; 4]>> should never return error; qed"));
			}
		}

		self.value.next().map(|i| *i)
	}
}

struct RecordIterator<T> {
	inner: T,
	position: usize,
	peeked: Option<u8>,
	field_size: usize,
	header: Header,
}

impl<T> RecordIterator<T> {
	fn new_inserted(inner: T, field_size: usize) -> Self {
		RecordIterator {
			inner,
			position: 0,
			peeked: None,
			field_size,
			header: Header::Inserted,
		}
	}
}

impl<T: Iterator<Item = u8>> Iterator for RecordIterator<T> {
	type Item = u8;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(peeked) = self.peeked.take() {
			self.position += 1;
			return Some(peeked);
		}

		if self.position == 0 {
			self.peeked = self.inner.next();
			if self.peeked.is_some() {
				self.position += 1;
				Some(self.header as u8)
			} else {
				None
			}
		} else if self.position % self.field_size == 0 {
			self.peeked = self.inner.next();
			if self.peeked.is_some() {
				self.position += 1;
				Some(Header::Continued as u8)
			} else {
				None
			}
		} else {
			self.position += 1;
			match self.inner.next() {
				Some(i) => Some(i),
				None => Some(0)
			}
		}
	}
}

pub fn append_record(buffer: &mut Vec<u8>, key: &[u8], value: &[u8], field_body_size: usize, const_value: bool) {
	let raw_record = RawRecordIterator::new(key, value, const_value);
	buffer.extend(RecordIterator::new_inserted(raw_record, field_size(field_body_size)));
}

#[cfg(test)]
mod tests {
	use super::{append_record};

	#[test]
	fn test_append_record_const1() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 3;
		let const_value = true;
		let expected = b"\x01key\x02val\x02ue\x00";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}

	#[test]
	fn test_append_record_const2() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 8;
		let const_value = true;
		let expected = b"\x01keyvalue";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}

	#[test]
	fn test_append_record_const3() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 10;
		let const_value = true;
		let expected = b"\x01keyvalue\x00\x00";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}

	#[test]
	fn test_append_record_variadic1() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 3;
		let const_value = false;
		let expected = b"\x01key\x02\x05\x00\x00\x02\x00va\x02lue";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}

	#[test]
	fn test_append_record_variadic2() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 12;
		let const_value = false;
		let expected = b"\x01key\x05\x00\x00\x00value";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}

	#[test]
	fn test_append_record_variadic3() {
		let mut buffer = Vec::new();
		let key = b"key";
		let value = b"value";
		let field_body_size = 14;
		let const_value = false;
		let expected = b"\x01key\x05\x00\x00\x00value\x00\x00";

		append_record(&mut buffer, key, value, field_body_size, const_value);
		assert_eq!(expected as &[u8], &buffer as &[u8]);
	}
}
