//! Iterator over database spaces

use error::{ErrorKind, Result};
use field::{self, field_size, Header};
use field::iterator::FieldHeaderIterator;

macro_rules! try_next {
	($t: expr) => {
		match $t {
			Ok(ok) => ok,
			Err(err) => return Some(Err(err.into())),
		}
	}
}

#[derive(Debug, PartialEq, Clone)]
pub struct OccupiedSpace<'a> {
	/// Offset from the beginning of iteration slice
	pub offset: usize,
	pub data: &'a [u8],
}

#[derive(Debug, PartialEq, Clone)]
pub struct EmptySpace {
	pub offset: usize,
	pub len: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Space<'a> {
	Occupied(OccupiedSpace<'a>),
	Empty(EmptySpace),
}

#[derive(Debug)]
pub struct SpaceIterator<'a> {
	data: &'a [u8],
	field_body_size: usize,
	offset: usize,
}

impl<'a> SpaceIterator<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize, offset: usize) -> Self {
		SpaceIterator {
			data,
			field_body_size,
			offset,
		}
	}

	/// Move iterator forward
	pub fn move_offset_forward(&mut self, offset: usize) {
		if offset > self.offset {
			self.offset = offset;
		}
	}

	/// Peek next value
	pub fn peek(&mut self) -> Option<Result<Space<'a>>> {
		// save offset
		let offset = self.offset;
		let next = self.next();
		// move back
		self.offset = offset;
		next
	}
}

impl<'a> Iterator for SpaceIterator<'a> {
	type Item = Result<Space<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data[self.offset..].is_empty() {
			return None;
		}

		let mut first_header = None;
		let mut start = self.offset;
		let field_size = field_size(self.field_body_size);
		let mut inner = try_next!(FieldHeaderIterator::new(&self.data[self.offset..], self.field_body_size));
		while let Some(header) = inner.next() {
			let header = try_next!(header);
			match header {
				Header::Continued => match first_header {
					// omit continued fields at the beginning
					None => {
						start += field_size;
						self.offset += field_size;
						continue;
					},
					Some(Header::Inserted) => {
						self.offset += field_size;
					},
					Some(Header::Continued) | Some(Header::Uninitialized) => {
						unreachable!();
					},
				},
				Header::Inserted => match first_header {
					Some(Header::Inserted) => return Some(Ok(Space::Occupied(OccupiedSpace {
						offset: start,
						data: &self.data[start..self.offset],
					}))),
					None => {
						self.offset += field_size;
					},
					Some(Header::Continued) | Some(Header::Uninitialized) => {
						unreachable!();
					},
				},
				Header::Uninitialized => match first_header {
					// inserted is unreachable
					Some(Header::Inserted) => return Some(Ok(Space::Occupied(OccupiedSpace {
						offset: start,
						data: &self.data[start..self.offset],
					}))),
					Some(Header::Continued) | Some(Header::Uninitialized) => {
						unreachable!();
					},
					None => {
						self.offset += field_size;
						return Some(Ok(Space::Empty(EmptySpace {
							offset: start,
							len: self.offset - start,
						})))
					},
				}
			}

			if first_header.is_none() && header != Header::Continued {
				first_header = Some(header);
			}
		}

		if first_header.is_none() {
			// continuation was called
			return Some(Err(ErrorKind::Field(field::ErrorKind::InvalidHeader).into()))
		}

		first_header.map(|header| match header {
			Header::Inserted => Ok(Space::Occupied(OccupiedSpace {
				offset: start,
				data: &self.data[start..self.offset],
			})),
			Header::Uninitialized => Ok(Space::Empty(EmptySpace {
				offset: start,
				len: self.offset - start,
			})),
			Header::Continued => {
				unreachable!();
			},
		})
	}
}

#[cfg(test)]
mod tests {
	use super::{SpaceIterator, Space, EmptySpace, OccupiedSpace};

	#[test]
	fn test_empty_space_iterator() {
		let data = &[];
		let field_body_size = 3;
		let offset = 0;

		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_one_uninitialized_element() {
		let data = &[0, 1, 1, 1];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_one_initialized_element() {
		let data = &[1, 1, 1, 1];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Occupied(OccupiedSpace { offset, data });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_two_different_spaces1() {
		let data = &[1, 1, 1, 1, 0, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Occupied(OccupiedSpace { offset, data: &data[0..4] });
		let second_elem = Space::Empty(EmptySpace { offset: offset + 4, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert_eq!(second_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_two_different_spaces2() {
		let data = &[0, 0, 0, 0, 1, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset, len: 4 });
		let second_elem = Space::Occupied(OccupiedSpace { offset: offset + 4, data: &data[4..8] });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert_eq!(second_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_two_inserts() {
		let data = &[1, 0, 0, 0, 1, 2, 2, 2];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Occupied(OccupiedSpace { offset, data: &data[0..4] });
		let second_elem = Space::Occupied(OccupiedSpace { offset: 4, data: &data[4..8] });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert_eq!(second_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_one_long_space1() {
		let data = &[1, 0, 0, 0, 2, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Occupied(OccupiedSpace { offset, data });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_one_long_space2() {
		let data = &[0, 0, 0, 0, 0, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset, len: 4 });
		let second_elem = Space::Empty(EmptySpace { offset: 4, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert_eq!(second_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_start_from_continued1() {
		let data = &[2, 0, 0, 0, 0, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset: 4, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_start_from_continued2() {
		let data = &[
			2, 0, 0, 0,
			2, 0, 0, 0,
			0, 0, 0, 0
		];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset: 8, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}

	#[test]
	fn test_space_iterator_continued_error() {
		let data = &[0, 0, 0, 0, 2, 0, 0, 0];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Empty(EmptySpace { offset, len: 4 });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().unwrap().is_err());
	}

	#[test]
	fn test_space_iterator_short_insert_after_long_insert() {
		let data = &[
			1, 0, 0, 0,
			2, 0, 0, 0,
			1, 0, 0, 0
		];
		let field_body_size = 3;
		let offset = 0;

		let first_elem = Space::Occupied(OccupiedSpace { offset, data: &data[0..8] });
		let second_elem = Space::Occupied(OccupiedSpace { offset: 8, data: &data[8..12] });
		let mut iterator = SpaceIterator::new(data, field_body_size, offset);
		assert_eq!(first_elem, iterator.next().unwrap().unwrap());
		assert_eq!(second_elem, iterator.next().unwrap().unwrap());
		assert!(iterator.next().is_none());
	}
}
