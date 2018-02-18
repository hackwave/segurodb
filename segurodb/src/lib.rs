//! Custom database for trading engine accounts
//!
//! Assumptions:
//!
//! - key-value database
//!
//! - with blazingly fast reads
//!
//! - and blazingly fast iteration by record's key
//!
//! - not so fast inserts
//!
//! - neither deletes
//!
//! - guaranteed ACID (atomicity, consistency, isolation and durability)
//!
//! Each record consists of key, value and optionally value len.
//!
//! ```text
//!  key  (value_len)  value
//!   /   /          /
//! |...|...|...........|
//! ```
//!
//! The database consist of array of contant-size fields.
//! Record might be stored in one or more consecutive fields.
//!
//! ```text
//!  record_x            record_o
//!   /                    /
//! |xxxx|xxxx|xx..|....|oooo|o...|
//!  1234 1235 1236 1237 1238 1239
//! ```
//!
//! Each field has it's own header and body.
//!
//! ```text
//!  header    body
//!   /         /
//! |...|...........|
//! ```
//!
//! A header is always a single byte which indicates what the body is.
//!
//! ```text
//! 0 - uninitialized
//! 1 - insert
//! 2 - continuation of the record
//! ```
//!
//! The index of the field for a record is determined using the first X bytes of the key.

#![warn(missing_docs)]

extern crate bit_vec;
extern crate byteorder;
#[macro_use]
extern crate error_chain;
extern crate fs2;
extern crate hex_slice;
extern crate itertools;
extern crate memmap;
extern crate parking_lot;
extern crate tiny_keccak;
#[cfg(test)]
#[macro_use]
extern crate matches;
#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod collision;
mod database;
mod error;
mod field;
mod find;
mod flush;
mod journal;
mod key;
mod metadata;
mod options;
mod prefix_tree;
mod record;
mod space;
mod transaction;

pub use database::{Database, Value};
pub use error::{Error, Result, ErrorKind};
pub use options::{Options, ValuesLen};
pub use record::Record;
pub use transaction::Transaction;
#[doc(hidden)]
pub use prefix_tree::PrefixTree;
