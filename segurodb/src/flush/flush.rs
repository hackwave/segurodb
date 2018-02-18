use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::{mem, fs};

use hex_slice::AsHex;
use memmap::{Mmap, Protection};
use tiny_keccak::{sha3_256, Keccak};

use error::{ErrorKind, Result};
use flush::iterator::IdempotentOperationIterator;
use flush::writer::OperationWriter;
use metadata::{self, Metadata};
use options::InternalOptions;
use transaction::Operation;

/// Stores transaction operations as a set of idempotent operations.
#[derive(Debug)]
pub struct Flush {
	path: PathBuf,
	mmap: Mmap,
	prefix_bits: u8,
	metadata: Metadata,
}

impl Flush {
	const FILE_NAME: &'static str = "db.flush";
	const CHECKSUM_SIZE: usize = 32;

	/// Creates memmap which is a set of only idempotent operations.
	pub fn new<'a, I, P>(
		dir: P,
		options: &InternalOptions,
		db: &[u8],
		metadata: &Metadata,
		operations: I,
	) -> Result<Flush>
		where I: IntoIterator<Item = Operation<'a>>, P: AsRef<Path> {

		let mut metadata = metadata.clone();

		let flush_data = OperationWriter::new(
			operations.into_iter(),
			db,
			&mut metadata,
			options.field_body_size,
			options.external.key_index_bits,
			options.external.value_len.is_const(),
		).run()?;

		let path = dir.as_ref().join(Flush::FILE_NAME);

		let mut file = fs::OpenOptions::new()
			.write(true)
			.read(true)
			.create_new(true)
			.open(&path)?;
		file.set_len(flush_data.len() as u64 + Self::CHECKSUM_SIZE as u64)?;
		file.flush()?;

		let mut mmap = Mmap::open(&file, Protection::ReadWrite)?;
		Keccak::sha3_256(&flush_data, unsafe { &mut mmap.as_mut_slice()[..Self::CHECKSUM_SIZE] });
		unsafe { &mut mmap.as_mut_slice()[Self::CHECKSUM_SIZE..] }.write_all(&flush_data)?;
		mmap.flush()?;

		Ok(Flush {
			path,
			mmap,
			metadata,
			prefix_bits: options.external.key_index_bits,
		})
	}

	/// Open flush file if it exists. It it does not, returns None.
	pub fn open<P: AsRef<Path>>(dir: P, prefix_bits: u8) -> Result<Option<Flush>> {
		let path = dir.as_ref().join(Self::FILE_NAME);
		let mmap = match Mmap::open_path(&path, Protection::Read) {
			Ok(mmap) => mmap,
			Err(ref err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(err) => return Err(err.into()),
		};

		{
			let checksum = unsafe { &mmap.as_slice()[..Self::CHECKSUM_SIZE] };
			let data = unsafe { &mmap.as_slice()[Self::CHECKSUM_SIZE..] };
			let hash = sha3_256(data);
			if hash != checksum {
				return Err(ErrorKind::CorruptedFlush(
					path,
					format!(
						"Expected: {:02x}, Got: {:02x}",
						hash.as_hex(),
						checksum.as_hex(),
					)
				).into());
			}
		}

		let meta_offset = mmap.len() - metadata::bytes::len(prefix_bits);
		let metadata = metadata::bytes::read(unsafe { &mmap.as_slice()[meta_offset..] }, prefix_bits);
		Ok(Some(Flush {
			path,
			mmap,
			prefix_bits,
			metadata,
		}))
	}

	/// Flushes idempotent operations to the database.
	pub fn flush(&self, db: &mut [u8], raw_metadata: &mut [u8], metadata: &mut Metadata) {
		let meta_offset = self.mmap.len() - metadata::bytes::len(self.prefix_bits);
		let operations = unsafe { &self.mmap.as_slice()[Self::CHECKSUM_SIZE..meta_offset] };
		let operations = IdempotentOperationIterator::new(operations);

		for o in operations {
			db[o.offset..o.offset + o.data.len()].copy_from_slice(o.data);
		}

		let meta = unsafe { &self.mmap.as_slice()[meta_offset..] };
		raw_metadata.copy_from_slice(meta);
		mem::swap(&mut self.metadata.clone(), metadata);
	}

	/// Delete flush file. Should be called only after database has been successfully flushed.
	pub fn delete(self) -> Result<()> {
		fs::remove_file(self.path)?;
		Ok(())
	}
}
