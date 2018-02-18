#![allow(unknown_lints)]
#![allow(missing_docs)]

use std::{io, num};
use std::path::PathBuf;

use field;

error_chain! {
	links {
		Field(field::Error, field::ErrorKind);
	}

	foreign_links {
		Io(io::Error);
		Num(num::ParseIntError);
	}

	errors {
		InvalidKeyLen(expected: usize, got: usize) {
			description("Invalid key length")
			display("Invalid key length. Expected: {}, got: {}", expected, got),
		}
		CorruptedFlush(path: PathBuf, msg: String) {
			description("Hash of flush data is invalid"),
			display("Database flush corruption detected in file at {}. {}", path.display(), msg),
		}
		CorruptedJournal(path: PathBuf, msg: String) {
			description("Hash of journal data is invalid"),
			display("Database journal corruption detected in file at {}. {}", path.display(), msg),
		}
		InvalidJournalLocation(path: PathBuf) {
			description("Path to journal is a file"),
			display("Expected a directory at {}, got file.", path.display()),
		}
		JournalEraMissing(idx: u64) {
			description("Eras are not consecutive"),
			display("Missing era file with index {}", idx),
		}
		InvalidOptions(field: &'static str, error: String) {
			description("Invalid options were provided"),
			display("Invalid value of `{}`: {}", field, error),
		}
		DatabaseLocked(path: PathBuf) {
			description("Database file lock is currently acquired"),
			display("Could not acquire database file lock: {}. \
					 If you're sure that no other process is using \
					 the database you can delete this file.", path.display()),
		}
	}
}

impl PartialEq for ErrorKind {
	fn eq(&self, other: &Self) -> bool {
		use self::ErrorKind::*;

		match (self, other) {
			(&InvalidKeyLen(expected, got), &InvalidKeyLen(expected2, got2))
				if expected == expected2 && got == got2 => true,
			(&CorruptedJournal(ref path, ref msg), &CorruptedJournal(ref path2, ref msg2))
				if path == path2 && msg == msg2 => true,
			(&InvalidJournalLocation(ref path), &InvalidJournalLocation(ref path2))
				if path == path2 => true,
			(&JournalEraMissing(idx), &JournalEraMissing(idx2))
				if idx == idx2 => true,
			(&InvalidOptions(field, ref error), &InvalidOptions(field2, ref error2))
				if field == field2 && error == error2 => true,
			_ => false,
		}
	}
}
