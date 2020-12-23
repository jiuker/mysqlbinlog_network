use std::io::{ BufWriter, Write};
use std::error::Error;
use crate::pkg::end_dian::{u16lit, u32lit, u64lit};
use crate::pkg::vec::{get_vec, set_to_vec};
use crate::pkg::err::Result;
use crate::none;


pub trait Event <T:Write+Sized> {
    //dump Event, format like python-mysql-replication
    fn dump(&self,w:BufWriter<T>);

    fn decode(&mut self,data:Vec<u8>)->Result<()>;
}

pub struct FormatDescriptionEvent{
    version:u16,
    //len = 50
    server_version:Vec<u8>, // Vec u8
    create_timestamp:u32,
    event_header_length:u8,
    event_type_header_lengths:Vec<u8>,
    // 0 is off, 1 is for CRC32, 255 is undefined
    checksum_algorithm:u8,
}
const EVENT_HEADER_SIZE:u8            = 19;
const CHECKSUM_VERSION_PRODUCT_MYSQL:i32 = 329217;
const CHECKSUM_VERSION_PRODUCT_MARIA_DB:i32 = 328448;
const BINLOG_CHECKSUM_ALG_UNDEF:u8 = 255; // special value to tag undetermined yet checksum
impl <T:Write+Sized> Event<T> for FormatDescriptionEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!("Version: {}\n", self.version));
        w.write_fmt(format_args!("Server version: {}\n", String::from_utf8(self.server_version.clone()).unwrap()));
        w.write_fmt(format_args!("Checksum algorithm: {}\n", self.checksum_algorithm));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self,data: Vec<u8>) -> Result<()> {
        let mut pos = 0;
        self.version = u16lit(get_vec(&data,pos,0)?.as_slice());
        pos+=2;

        self.server_version = vec![0;50];
        set_to_vec(&mut self.server_version,pos,data.clone());
        pos+=50;

        self.create_timestamp = u32lit(get_vec(&data,pos,0)?.as_slice());
        pos+4;

        self.event_header_length = *none!(data.get(pos));
        pos+=1;

        if self.event_header_length!=EVENT_HEADER_SIZE{
            return Err(Box::from(format!("invalid event header length {}, must 19",self.event_header_length)));
        }

        let server = String::from_utf8(self.server_version.clone())?;

        let mut checksum_product = CHECKSUM_VERSION_PRODUCT_MYSQL;
        if server.to_lowercase().contains("mariadb"){
            checksum_product = CHECKSUM_VERSION_PRODUCT_MARIA_DB;
        }

        if calc_version_product(String::from_utf8(self.server_version.clone())?) >= checksum_product{
            self.checksum_algorithm = *none!(data.get(data.len()-5));
            self.event_type_header_lengths = get_vec(&data,pos,data.len()-5)?;
        }else{
            self.checksum_algorithm = BINLOG_CHECKSUM_ALG_UNDEF;
            self.event_type_header_lengths = get_vec(&data,pos,0)?;
        }
        Ok(())
    }
}
fn calc_version_product(server:String)->i32{
    let mut rsl = 0;
    let mut i =0;
    for s in server.split("."){
        if i == 2{
            let mut s_ = "".to_string();
            for s__ in s.as_bytes(){
                if s__.is_ascii_digit(){
                   s_.push((*s__)as char);
                }else{
                    break;
                }
            }
            rsl = rsl*256+s_.parse::<i32>().unwrap();
        }else{
            rsl = rsl*256+s.parse::<i32>().unwrap();
        }
        i+=1;
    };
    rsl
}

#[test]
fn test_calc_version_product(){
    assert_eq!(CHECKSUM_VERSION_PRODUCT_MYSQL, calc_version_product("5.6.1log".to_string()));
    assert_eq!(CHECKSUM_VERSION_PRODUCT_MARIA_DB, calc_version_product("5.3.0log".to_string()));
}

pub struct RotateEvent {
    position:u64,
    next_log_name:Vec<u8>,
}
impl <T:Write+Sized> Event<T> for RotateEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!("Position: {}\n", self.position));
        w.write_fmt(format_args!("Next log name: {}\n", String::from_utf8(self.next_log_name.clone()).unwrap()));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        self.position = u64lit(data.as_slice());
        self.next_log_name = get_vec(&data,8,0)?;
        Ok(())
    }
}