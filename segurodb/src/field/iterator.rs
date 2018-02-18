use field::error::{Result, ErrorKind};
use field::field_size;
use field::header::Header;

#[derive(Clone)]
pub struct FieldHeaderIterator<'a> {
	data: &'a [u8],
	field_size: usize,
}

impl<'a> FieldHeaderIterator<'a> {
	pub fn new(data: &'a [u8], field_body_size: usize) -> Result<Self> {
		let field_size = field_size(field_body_size);
		if (data.len() % field_size) != 0 {
			return Err(ErrorKind::InvalidLength.into());
		}

		Ok(FieldHeaderIterator {
			data,
			field_size,
		})
	}
}

impl<'a> Iterator for FieldHeaderIterator<'a> {
	type Item = Result<Header>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let (next_field, new_data) = self.data.split_at(self.field_size);
		self.data = new_data;
		Some(Header::from_u8(next_field[0]))
	}
}
