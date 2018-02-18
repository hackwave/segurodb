use field;

pub struct Key<'a> {
	pub key: &'a [u8],
	pub prefix: u32,
}

impl<'a> Key<'a> {
	pub fn new(key: &'a [u8], prefix_bits: u8) -> Self {
		assert!(prefix_bits <= 32u8);
		let prefix = Self::read_prefix(key, prefix_bits);
		Key {
			key,
			prefix,
		}
	}

	pub fn offset(&self, field_body_size: usize) -> usize {
		self.prefix as usize * field::field_size(field_body_size)
	}

	fn read_prefix(key: &'a [u8], prefix_bits: u8) -> u32 {
		let mut prefix = 0u32;
		let pos = prefix_bits as usize / 8;
		let bits = prefix_bits % 8;

		for i in 0..pos {
			prefix <<= 8;
			prefix |= key[i] as u32;
		}

		if bits > 0 {
			prefix <<= bits;
			prefix |= key[pos] as u32 >> (8 - bits);
		}

		prefix
	}
}

#[cfg(test)]
mod tests {
	use super::Key;

	#[test]
	fn should_read_prefix_correctly() {
		let k = vec![0xff, 0xfe, 0xdc, 0xba];

		assert_eq!(Key::new(&k, 0).prefix, 0x0);
		assert_eq!(Key::new(&k, 1).prefix, 0x1);
		assert_eq!(Key::new(&k, 2).prefix, 0x3);
		assert_eq!(Key::new(&k, 4).prefix, 0xf);
		assert_eq!(Key::new(&k, 8).prefix, 0xff);
		assert_eq!(Key::new(&k, 16).prefix, 0xfffe);
		assert_eq!(Key::new(&k, 20).prefix, 0xfffed);
		assert_eq!(Key::new(&k, 24).prefix, 0xfffedc);
		assert_eq!(Key::new(&k, 26).prefix, 0x3fffb72);
		assert_eq!(Key::new(&k, 32).prefix, 0xfffedcba);
	}
}
