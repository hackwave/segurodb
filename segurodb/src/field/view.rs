use std::cmp;

use field::header::HEADER_SIZE;
use field::field_size;

macro_rules! on_body_slice {
	($self:expr, $slice:expr, $fn:ident) => {
		let field_body_size = $self.field_body_size;
		let mut ours = $self.offset + HEADER_SIZE * $self.offset / field_body_size;
		let mut theirs = 0;

		if ($self.offset % field_body_size) != 0 {
			let rem = cmp::min($slice.len(), field_body_size - ($self.offset % field_body_size));
			ours += HEADER_SIZE;

			$fn!($self.data[ours..ours + rem], $slice[theirs..theirs + rem]);

			theirs += rem;
			ours += rem;
		}

		let fields = ($slice.len() - theirs) / field_body_size;
		for _ in 0..fields {
			ours += HEADER_SIZE;

			$fn!($self.data[ours..ours + field_body_size], $slice[theirs..theirs + field_body_size]);

			theirs += field_body_size;
			ours += field_body_size;
		}

		if theirs != $self.len {
			let rem = $self.len - theirs;
			ours += HEADER_SIZE;

			$fn!($self.data[ours..ours + rem], $slice[theirs..]);
		}
	}
}

/// A view onto multiple consecutive fields
#[derive(Debug)]
pub struct FieldsView<'a> {
	data: &'a [u8],
	field_body_size: usize,
	offset: usize,
	len: usize,
}

impl<'a, T: AsRef<[u8]>> PartialEq<T> for FieldsView<'a> {
	fn eq(&self, slice: &T) -> bool {
		let slice = slice.as_ref();
		if slice.len() != self.len {
			return false;
		}

		macro_rules! eq {
			($a: expr, $b: expr) => {
				if $a != $b {
					return false;
				}
			}
		}

		on_body_slice!(self, slice, eq);

		true
	}
}

impl<'a, 'b> PartialEq<FieldsView<'b>> for FieldsView<'a> {
	fn eq(&self, other: &FieldsView<'b>) -> bool {
		if self.len != other.len {
			return false;
		}

		let mut it1 = self.iter();
		let mut it2 = other.iter();

		loop {
			match (it1.next(), it2.next()) {
				(Some(a), Some(b)) if a == b => {},
				(None, None) => return true,
				_ => return false
			}
		}
	}
}

impl<'a, T: AsRef<[u8]>> PartialOrd<T> for FieldsView<'a> {
	fn partial_cmp(&self, slice: &T) -> Option<cmp::Ordering> {
		let slice = slice.as_ref();
		if slice.len() != self.len {
			return None;
		}

		macro_rules! partial_cmp {
			($a: expr, $b: expr) => {
				match $a.cmp(&$b) {
					cmp::Ordering::Equal => {},
					cmp::Ordering::Less => return Some(cmp::Ordering::Less),
					cmp::Ordering::Greater => return Some(cmp::Ordering::Greater),
				}
			}
		}

		on_body_slice!(self, slice, partial_cmp);

		Some(cmp::Ordering::Equal)
	}
}

impl<'a> FieldsView<'a> {
	/// Creates new `FieldsView` with no offset
	pub fn new(data: &'a [u8], field_body_size: usize) -> Self {
		assert!(field_body_size > 0, "field body size can't be zero.");

		FieldsView {
			data,
			field_body_size,
			offset: 0,
			len: data.len() * field_body_size / field_size(field_body_size),
		}
	}

	/// Create new `FieldsView` with an offset. Useful, when reading record body.
	pub fn with_options(data: &'a [u8], field_body_size: usize, offset: usize, len: usize) -> Self {
		FieldsView {
			data,
			field_body_size,
			offset,
			len,
		}
	}

	/// Returns an iterator over data this `FieldsView` spans.
	pub fn iter(&self) -> Bytes<'a> {
		Bytes {
			data: self.data,
			field_body_size: self.field_body_size,
			offset: self.offset,
			len: self.len,
		}
	}

	/// Returns underlying value if it is a continuous slice of memory,
	/// otherwise returns None.
	pub fn raw_slice(&self) -> Option<&'a [u8]> {
		if self.len == 0 {
			return Some(&[]);
		}
		let field_size = field_size(self.field_body_size);
		let start = self.offset + HEADER_SIZE * self.offset / self.field_body_size + HEADER_SIZE;
		let end = start + self.len;
		let start_page = start / field_size;
		let end_page = (end - 1) / field_size;
		if start_page == end_page {
			Some(&self.data[start..end])
		} else {
			None
		}
	}

	/// Copy field content to given slice.
	///
	/// Panics if the lengths don't match.
	pub fn copy_to_slice(&self, slice: &mut [u8]) {
		assert_eq!(self.len, slice.len(), "slice must have the same size");

		macro_rules! copy_to_slice {
			($a: expr, $b: expr) => {
				$b.copy_from_slice(&$a);
			}
		}

		on_body_slice!(self, slice, copy_to_slice);
	}

	/// Split this field view into two at given position.
	///
	/// The first returned `FieldView` will contain elements from `[0, pos)`
	/// and the second `[pos, len)``
	pub fn split_at(self, pos: usize) -> (Self, Self) {
		assert!(self.len >= pos, "Cannot split beyond length: {} < {} ", self.len, pos);
		assert!(self.data.len() >= self.offset + pos, "Cannot split beyond data length: {} < {}", self.data.len(), self.offset + pos);

		let left = FieldsView::with_options(self.data, self.field_body_size, self.offset, pos);
		let right = FieldsView::with_options(self.data, self.field_body_size, self.offset + pos, self.len - pos);
		(left, right)
	}
}

#[derive(Debug)]
pub struct Bytes<'a> {
	data: &'a [u8],
	field_body_size: usize,
	offset: usize,
	len: usize,
}

impl<'a> Iterator for Bytes<'a> {
	type Item = u8;

	fn next(&mut self) -> Option<Self::Item> {
		if self.len == 0 {
			return None;
		}

		// Skip headers
		if (self.offset - HEADER_SIZE * self.offset / self.field_body_size) % self.field_body_size == 0 {
			self.offset += HEADER_SIZE;
		}

		let byte = self.data[self.offset];

		// move forward
		self.offset += 1;
		self.len -= 1;

		Some(byte)
	}
}

#[cfg(test)]
mod tests {
	use std::cmp;

	use super::FieldsView;

	#[test]
	#[should_panic(expected = "field body size can't be zero.")]
	fn test_zero_body_size() {
		let body_size = 0;
		let data = [0, 1, 2, 3];
		FieldsView::new(&data, body_size);
	}

	#[test]
	fn test_fields_view_copy_to() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected = [1, 2, 3, 4, 5, 6];

		let mut result = [0u8; 6];
		let fv = FieldsView::new(&data, body_size);
		fv.copy_to_slice(&mut result);
		assert_eq!(expected, result);
	}

	#[test]
	fn test_fields_view_split_at_short() {
		let body_size = 5;
		let data = [0, 1, 2, 3, 4, 5];
		let expected_key = [1, 2];
		let expected_value = [3];
		let expected_rest = [4, 5];

		let mut result_key = [0u8; 2];
		let mut result_value = [0u8; 1];
		let mut result_rest = [0u8; 2];

		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(2);
		let (value, rest) = value.split_at(1);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		rest.copy_to_slice(&mut result_rest);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(rest, &expected_rest);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
		assert_eq!(expected_rest, result_rest);
	}

	#[test]
	fn test_fields_view_split_at() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected_key = [1, 2];
		let expected_value = [3, 4, 5, 6];

		let mut result_key = [0u8; 2];
		let mut result_value = [0u8; 4];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(2);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at2() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let expected_key = [1, 2, 3];
		let expected_value = [4, 5, 6];

		let mut result_key = [0u8; 3];
		let mut result_value = [0u8; 3];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(3);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at3() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9, 0, 10, 11];
		let expected_key = [1, 2, 3, 4];
		let expected_value = [5, 6, 7, 8, 9, 10, 11];

		let mut result_key = [0u8; 4];
		let mut result_value = [0u8; 7];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(4);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_split_at4() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9, 0, 10, 11, 12, 0, 13];
		let expected_key = [1, 2, 3, 4, 5, 6];
		let expected_value = [7, 8, 9, 10, 11, 12, 13];

		let mut result_key = [0u8; 6];
		let mut result_value = [0u8; 7];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(6);
		key.copy_to_slice(&mut result_key);
		value.copy_to_slice(&mut result_value);
		assert_eq!(key, &expected_key);
		assert_eq!(value, &expected_value);
		assert_eq!(expected_key, result_key);
		assert_eq!(expected_value, result_value);
	}

	#[test]
	fn test_fields_view_iter() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];

		let fv = FieldsView::new(&data, body_size);
		let mut it = fv.iter();

		assert_eq!(it.next(), 1.into());
		assert_eq!(it.next(), 2.into());
		assert_eq!(it.next(), 3.into());
		assert_eq!(it.next(), 4.into());
		assert_eq!(it.next(), 5.into());
		assert_eq!(it.next(), 6.into());
		assert_eq!(it.next(), None);
	}

	#[test]
	fn test_fields_view_partial_eq() {
		let body_size = 3;
		let body_size2 = 5;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let data2 = [0, 1, 2, 3, 4, 5, 0, 6, 7];

		let fv1 = FieldsView::new(&data, body_size);
		let fv2 = FieldsView::with_options(&data2, body_size2, 0, 6);

		assert_eq!(fv1, fv2);
	}

	#[test]
	fn test_partial_cmp() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6, 0, 7, 8, 9, 0, 10, 11, 12, 0, 13];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(fv.partial_cmp(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]), None);
		assert_eq!(fv.partial_cmp(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]), Some(cmp::Ordering::Equal));
		assert_eq!(fv.partial_cmp(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14]), Some(cmp::Ordering::Less));
		assert_eq!(fv.partial_cmp(&[2, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]), Some(cmp::Ordering::Less));
		assert_eq!(fv.partial_cmp(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12]), Some(cmp::Ordering::Greater));
		assert_eq!(fv.partial_cmp(&[1, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]), Some(cmp::Ordering::Greater));
	}

	#[test]
	fn test_raw_slice1() {
		let body_size = 3;
		let data = [0, 1, 2, 3];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(Some(&[1u8, 2, 3] as &[u8]), fv.raw_slice());
	}

	#[test]
	fn test_raw_slice2() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(None, fv.raw_slice());
		let (fv1, fv2) = fv.split_at(3);
		assert_eq!(Some(&[1u8, 2, 3] as &[u8]), fv1.raw_slice());
		assert_eq!(Some(&[4u8, 5, 6] as &[u8]), fv2.raw_slice());
	}

	#[test]
	fn test_raw_slice3() {
		let body_size = 3;
		let data = [0, 1, 2, 3, 0, 4, 5, 6];
		let fv = FieldsView::new(&data, body_size);
		let (key, value) = fv.split_at(2);
		let (value, rest) = value.split_at(1);

		assert_eq!(Some(&[1u8, 2] as &[u8]), key.raw_slice());
		assert_eq!(Some(&[3u8] as &[u8]), value.raw_slice());
		assert_eq!(Some(&[4u8, 5, 6] as &[u8]), rest.raw_slice());
	}

	#[test]
	fn test_raw_slice4() {
		let body_size = 3;
		let data = [0, 1, 2];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(Some(&[1u8, 2] as &[u8]), fv.raw_slice());
	}

	#[test]
	fn test_raw_slice_empty_array() {
		let body_size = 3;
		let data = [];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(Some(&[] as &[u8]), fv.raw_slice());
	}

	#[test]
	fn test_raw_slice_empty_body() {
		let body_size = 3;
		let data = [0];
		let fv = FieldsView::new(&data, body_size);

		assert_eq!(Some(&[] as &[u8]), fv.raw_slice());
	}
}
