extern crate clap;
extern crate segurodb;

use clap::{Arg, ArgMatches, App, SubCommand};
use segurodb::{Database, Error, Options};

fn read_parameters<'a>(matches: &'a ArgMatches) -> Result<(&'a str, &'a str, Option<&'a str>), ()>{
	match (matches.value_of("DB"), matches.value_of("KEY")) {
		(Some(db), Some(key)) => {
			Ok((db, key, matches.value_of("VALUE")))
		},
		_ => {
			Err(())
		}
	}
}

fn do_get(db: &str, key: &str) -> Result<(), Error> {
	let db = Database::open(db, Options::default())?;
	let ret = db.get(key);
	match ret {
		Ok(Some(value)) => {
			println!("value: {:?}", value);
		},
		Ok(None) => {
			println!("value not found.");
		},
		Err(err) => {
			println!("no value found for this key with error: {:?}.", err);
		}
	}
	Ok(())
}

fn do_insert(db: &str, key: &str, value: &str) -> Result<(), Error> {
	let mut db = Database::open(db, Options::default())
				.or(Database::create(db, Options::default()))?;
	let mut tx = db.create_transaction();
	tx.insert(key, value)?;
	db.commit(&tx)?;
	db.flush_journal(1)?;
	Ok(())
}

fn do_delete(db: &str, key: &str) -> Result<(), Error> {
	let mut db = Database::open(db, Options::default())?;
	let mut tx = db.create_transaction();
	tx.delete(key)?;
	db.commit(&tx)?;
	db.flush_journal(1)?;
	Ok(())
}

fn main() {
	let matches =
		App::new("segurodb-cli")
			.version("0.1.0")
			.author("Database designed for trading engine")
			.about("A simple command line interface for SeguroDB")
			.subcommand(SubCommand::with_name("get")
				.about("Get value from the specified key in database")
				.arg(Arg::with_name("KEY")
					.short("k")
					.long("key")
					.takes_value(true))
				.arg(Arg::with_name("DB")
					.short("d")
					.long("db")
					.takes_value(true)))
			.subcommand(SubCommand::with_name("insert")
				.about("Insert key to database")
				.arg(Arg::with_name("KEY")
					.short("k")
					.long("key")
					.takes_value(true))
				.arg(Arg::with_name("VALUE")
					.short("v")
					.long("value")
					.takes_value(true))
				.arg(Arg::with_name("DB")
					.short("d")
					.long("db")
					.takes_value(true)))
			.subcommand(SubCommand::with_name("delete")
				.about("Delete key in database")
				.arg(Arg::with_name("KEY")
					.short("k")
					.long("key")
					.takes_value(true))
				.arg(Arg::with_name("DB")
					.short("d")
					.long("db")
					.takes_value(true)))
			.get_matches();

	match matches.subcommand() {
		("get", Some(sub_m)) => {
			if let Ok((db, key, _)) = read_parameters(&sub_m) {
				do_get(db, key).expect("execute get error");
			} else {
				println!("errors for get.");
			}
		},
		("insert", Some(sub_m)) => {
			if let Ok((db, key, Some(value))) = read_parameters(&sub_m) {
				do_insert(db, key, value).expect("execute insert error.");
			} else {
				println!("errors for insert.");
			}
		},
		("delete", Some(sub_m)) => {
			if let Ok((db, key, _)) = read_parameters(&sub_m) {
				do_delete(db, key).expect("execute delete error.");
			} else {
				println!("errors for delete");
			}
		},
		_ => {}
	}
}
