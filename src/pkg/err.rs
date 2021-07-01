use std::error::Error;
use std::result;

pub type Result<T> = result::Result<T, Box<dyn Error>>;
