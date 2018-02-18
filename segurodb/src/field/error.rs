#![allow(unknown_lints)]
#![allow(missing_docs)]

error_chain! {
	types {
		Error, ErrorKind, ResultExt, Result;
	}

	errors {
		InvalidHeader {
			description("invalid header"),
			display("invalid header"),
		}
		InvalidLength {
			description("invalid length"),
			display("invalid length"),
		}
	}
}
