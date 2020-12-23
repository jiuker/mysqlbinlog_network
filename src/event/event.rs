use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::error::Error;
use crate::pkg::end_dian::u16lit;

pub trait Event <T:Write+Sized> {
    //dump Event, format like python-mysql-replication
    fn dump(&self,w:BufWriter<T>);

    fn decode(&mut self,data:Vec<u8>)->Result<(),Box<dyn Error>>;
}

pub struct FormatDescriptionEvent{
    version:u16,
    //len = 50
    server_version:String, // Vec u8
    create_timestamp:u32,
    event_header_length:u8,
    event_type_header_lengths:Vec<u8>,
    // 0 is off, 1 is for CRC32, 255 is undefined
    checksum_algorithm:u8,
}

impl <T:Write+Sized> Event<T> for FormatDescriptionEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!("Version: {}\n", self.version));
        w.write_fmt(format_args!("Server version: {}\n", self.server_version));
        w.write_fmt(format_args!("Checksum algorithm: {}\n", self.checksum_algorithm));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self,data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let pos = 0;
        Ok(())
    }
}