use field::error::{Result, ErrorKind};

pub const HEADER_SIZE: usize = 1;

/// `Header` is a first byte of database field.
#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Header {
	/// Indicates that field hasn't been initialized yet.
	Uninitialized = Header::UNINITIALIZED,
	/// Indicates that the field is the beginning of the record.
	Inserted = Header::INSERTED,
	/// Indicates that the field is continuation of other field which is either `Inserted` or `Deleted`.
	Continued = Header::CONTINUED,
}

impl Header {
	const UNINITIALIZED: u8 = 0;
	const INSERTED: u8 = 1;
	const CONTINUED: u8 = 2;

	/// Converts `u8` into Header.
	pub fn from_u8(byte: u8) -> Result<Header> {
		match byte {
			Self::UNINITIALIZED => Ok(Header::Uninitialized),
			Self::INSERTED => Ok(Header::Inserted),
			Self::CONTINUED => Ok(Header::Continued),
			_ => Err(ErrorKind::InvalidHeader.into()),
		}
	}
}

#[cfg(test)]
mod test {
	use super::Header;

	#[test]
	fn test_from_u8() {
		assert_eq!(Header::Uninitialized, Header::from_u8(Header::Uninitialized as u8).unwrap());
		assert_eq!(Header::Inserted, Header::from_u8(Header::Inserted as u8).unwrap());
		assert_eq!(Header::Continued, Header::from_u8(Header::Continued as u8).unwrap());
		assert!(Header::from_u8(100u8).is_err());
	}
}
