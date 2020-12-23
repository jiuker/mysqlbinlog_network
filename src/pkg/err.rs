use std::error::Error;
use core::result;

pub type Result<T> = result::Result<T, Box<dyn Error>>;

