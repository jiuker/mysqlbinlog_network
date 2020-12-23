use crate::event::event::Event;
use std::io::{Write, BufWriter};
use crate::pkg::err::Result;
use crate::pkg::end_dian::u64lit;
use crate::pkg::vec::get_vec;

pub struct RowsQueryEvent {
    query:Vec<u8>,
}
impl <T:Write+Sized> Event<T> for RowsQueryEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!("Query: {}\n", String::from_utf8(self.query.clone()).unwrap()));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        self.query = get_vec(&data,1,0)?;
        Ok(())
    }
}