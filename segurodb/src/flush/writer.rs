//! Flush operations writer

use std::iter::Peekable;

use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

use error::Result;
use flush::decision::{decision, Decision, is_min_offset_for_space, min_offset_for_space};
use key::Key;
use metadata::Metadata;
use record::{append_record};
use space::{SpaceIterator, Space};
use transaction::Operation;

#[inline]
fn write_insert_operation(buffer: &mut Vec<u8>, key: &[u8], value: &[u8], field_body_size: usize, const_value: bool) -> usize {
	let buffer_len = buffer.len();
	append_record(buffer, key, value, field_body_size, const_value);
	buffer.len() - buffer_len
}

#[inline]
fn write_empty_bytes(buffer: &mut Vec<u8>, len: usize) {
	let buffer_len = buffer.len();
	buffer.resize(buffer_len + len, 0);
}

#[derive(Debug, PartialEq, Default)]
struct OperationBuffer {
	inner: Vec<u8>,
	denoted_operation_start: Option<usize>,
}

impl OperationBuffer {
	#[inline]
	fn as_raw_mut(&mut self) -> &mut Vec<u8> {
		&mut self.inner
	}

	#[inline]
	fn denote_operation_start(&mut self, offset: u64) {
		if self.denoted_operation_start.is_none() {
			self.denoted_operation_start = Some(self.inner.len());
			self.inner.write_u64::<LittleEndian>(offset).unwrap();
			// reserve space for len
			self.inner.extend_from_slice(&[0; 4]);
		}
	}

	#[inline]
	fn finish_operation(&mut self) {
		if let Some(operation_start) = self.denoted_operation_start.take() {
			let len = self.inner.len() - (operation_start + 12);
			LittleEndian::write_u32(&mut self.inner[operation_start + 8..operation_start + 12], len as u32);
		}
	}
}

enum OperationWriterStep {
	Stepped,
	Finished
}

/// Writes transactions as a set of idempotent operations
pub struct OperationWriter<'db, I: Iterator> {
	operations: Peekable<I>,
	spaces: SpaceIterator<'db>,
	metadata: &'db mut Metadata,
	buffer: OperationBuffer,
	field_body_size: usize,
	prefix_bits: u8,
	const_value: bool,
	/// shift is always increased or decreased by a len of inserted/deleted
	/// record or an empty field. inserted and deleted records are always
	/// aligned by function append_record from src/record/append.rs.
	shift: isize,
}

impl<'op, 'db, I: Iterator<Item = Operation<'op>>> OperationWriter<'db, I> {
	/// Creates new operations writer. All operations needs to be ordered by key.
	pub fn new(
		operations: I,
		database: &'db [u8],
		metadata: &'db mut Metadata,
		field_body_size: usize,
		prefix_bits: u8,
		const_value: bool,
	) -> Self {
		OperationWriter {
			operations: operations.peekable(),
			spaces: SpaceIterator::new(database, field_body_size, 0),
			metadata,
			buffer: OperationBuffer::default(),
			field_body_size,
			prefix_bits,
			const_value,
			shift: 0,
		}
	}

	fn last_step(&mut self) -> Result<()> {
		for space in &mut self.spaces {
			if self.shift == 0 { break; }
			match space? {
				Space::Empty(space) => {
					if self.shift > 0 {
						self.shift -= space.len as isize;
					}
				},
				Space::Occupied(space) => {
					if self.shift > 0 {
						self.buffer.as_raw_mut().extend_from_slice(space.data);
					} else {
						if is_min_offset_for_space(space.offset, self.shift, space.data, self.prefix_bits, self.field_body_size) {
							self.buffer.as_raw_mut().extend_from_slice(space.data);
						} else {
							let min_offset = min_offset_for_space(space.data, self.prefix_bits, self.field_body_size) as isize;
							let diff = space.offset as isize - (-self.shift) - min_offset;
							if diff < 0 {
								write_empty_bytes(self.buffer.as_raw_mut(), (-diff) as usize);
								self.buffer.as_raw_mut().extend_from_slice(space.data);
								self.shift += -diff;
							} else {
								write_empty_bytes(self.buffer.as_raw_mut(), (-self.shift + diff) as usize);
								self.buffer.as_raw_mut().extend_from_slice(space.data);
								self.shift = diff;
							}
						}
					}
				},
			}
		}

		if self.shift < 0 {
			write_empty_bytes(self.buffer.as_raw_mut(), (-self.shift) as usize);
		}

		// write the len of previous operation
		self.buffer.finish_operation();
		Ok(())
	}

	fn step(&mut self) -> Result<OperationWriterStep> {
		let operation = match self.operations.peek().cloned() {
			Some(operation) => operation,
			None => {
				self.last_step()?;
				return Ok(OperationWriterStep::Finished)
			}
		};

		let prefixed_key = Key::new(operation.key(), self.prefix_bits);

		if self.shift == 0 {
			// write the len of previous operation
			self.buffer.finish_operation();
			self.spaces.move_offset_forward(prefixed_key.offset(self.field_body_size));
		}

		let space = self.spaces.peek().expect("TODO: db end?")?;
		let d = decision(operation, space, self.shift, self.field_body_size, self.prefix_bits);
		match d {
			Decision::InsertOperationIntoEmptySpace { key, value, offset, space_len } => {
				// advance iterators
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = write_insert_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value);
				self.shift += written as isize - space_len as isize;
				// insert metadata
				self.metadata.insert_record(prefixed_key.prefix, written);
			},
			Decision::InsertOperationBeforeOccupiedSpace { key, value, offset } => {
				// advance iterators
				let _ = self.operations.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = write_insert_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value);
				self.shift += written as isize;
				// insert metadata
				self.metadata.insert_record(prefixed_key.prefix, written);
			},
			Decision::OverwriteOperation { key, value, offset, old_len } => {
				// advance iterators
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				let written = write_insert_operation(self.buffer.as_raw_mut(), key, value, self.field_body_size, self.const_value);
				self.shift += written as isize - old_len as isize;
				// update metadata
				self.metadata.update_record_len(old_len, written);
			},
			Decision::SeekSpace => {
				// advance iterator
				let _ = self.spaces.next();
			},
			Decision::IgnoreOperation => {
				// ignore this operation
				let _ = self.operations.next();
			},
			Decision::ConsumeEmptySpace { len } => {
				let _ = self.spaces.next();
				self.shift -= len as isize;
			},
			Decision::ShiftOccupiedSpace { data } => {
				// advance iterators
				let _ = self.spaces.next();
				// rewrite the space to a buffer
				self.buffer.as_raw_mut().extend_from_slice(data);
			},
			Decision::FinishBackwardShift => {
				// do not advance iterator
				// finish shift backwards
				assert!(self.shift < 0, "we are in delete mode");
				write_empty_bytes(self.buffer.as_raw_mut(), (-self.shift) as usize);
				self.shift = 0;
			},
			Decision::DeleteOperation { offset, len } => {
				// advance operations
				let _ = self.operations.next();
				let _ = self.spaces.next();

				// denote operation start
				self.buffer.denote_operation_start(offset as u64);
				self.shift -= len as isize;
				// update metadata
				self.metadata.remove_record(len);
			},
		}

		Ok(OperationWriterStep::Stepped)
	}

	#[inline]
	pub fn run(mut self) -> Result<Vec<u8>> {
		while let OperationWriterStep::Stepped = self.step()? {}
		let mut result = self.buffer.inner;
		let meta = self.metadata.as_bytes();
		let old_len = result.len();
		result.resize(old_len + meta.len(), 0);
		meta.copy_to_slice(&mut result[old_len..]);
		Ok(result)
	}
}
