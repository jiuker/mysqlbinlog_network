use crate::event::event::Event;
use std::io::{Write, BufWriter};
use crate::pkg::err::Result;
use crate::pkg::end_dian::{u64lit, u16lit, u64big, u16big, u32big};
use std::fs::read;

pub struct RowsQueryEvent {
    query:Vec<u8>,
}
impl <T:Write+Sized> Event<T> for RowsQueryEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!("Query: {}\n", String::from_utf8(self.query.clone()).unwrap()));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        self.query = data[1..].to_vec();
        Ok(())
    }
}
pub struct TableMapEvent{
    flavor:String,
    table_idsize:usize,

    table_id:u64,

    flags:u16,

    schema:Vec<u8>,
    table:Vec<u8>,

    column_count:u64,
    column_type:Vec<u8>,
    column_meta:Vec<u16>,

    //len = (column_count + 7) / 8
    null_bitmap:Vec<u8>,

    /*
        The followings are available only after MySQL-8.0.1 or MariaDB-10.5.0
        see:
            - https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
            - https://mysqlhighavailability.com/more-metadata-is-written-into-binary-log/
            - https://jira.mariadb.org/browse/MDEV-20477
    */

    // signedness_bitmap stores signedness info for numeric columns.
    signedness_bitmap:Vec<u8>,

    // default_charset/column_charset stores collation info for character columns.

    // default_charset[0] is the default collation of character columns.
    // For character columns that have different charset,
    // (character column index, column collation) pairs follows
    default_charset:Vec<u64>,
    // column_charset contains collation sequence for all character columns
    column_charset:Vec<u64>,

    // set_str_value stores values for set columns.
    set_str_value:Vec<Vec<Vec<u8>>>,
    set_str_value_string:Vec<Vec<String>>,

    // enum_str_value stores values for enum columns.
    enum_str_value:Vec<Vec<Vec<u8>>>,
    enum_str_value_string:Vec<Vec<String>>,

    // column_name list all column names.
    column_name:Vec<Vec<u8>>,
    column_name_string:Vec<String>,// the same as column_name in string type, just for reuse

    // geometry_type stores real type for geometry columns.
    geometry_type:Vec<u64>,

    // primary_key is a sequence of column indexes of primary key.
    primary_key:Vec<u64>,

    // primary_key_prefix is the prefix length used for each column of primary key.
    // 0 means that the whole column length is used.
    primary_key_prefix:Vec<u64>,

    // enum_set_default_charset/enum_set_column_charset is similar to default_charset/column_charset but for enum/set columns.
    enum_set_default_charset:Vec<u64>,
    enum_set_column_charset:Vec<u64>,
}

impl <T:Write+Sized> Event<T> for TableMapEvent {
    fn dump(&self, w: BufWriter<T>) {
    }

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        let mut pos = 0;
        self.table_id = fixed_length_int(Vec::from(&data[0..self.table_idsize]));
        pos+=self.table_idsize;
        self.flags = u16lit(&data[pos..]);

        let schema_length = data[pos];
        pos+=1;
        self.schema = data[pos..pos + schema_length as usize].to_owned();
        pos+= schema_length as usize;
        pos+=1;

        let table_length = data[pos];
        pos+=1;
        self.table = data[pos..pos + table_length as usize].to_owned();
        pos+= table_length as usize;
        pos+=1;

        let mut n = 0;
        let (column_count, _, rn) = length_encoded_int(&data[pos..]);
        self.column_count = column_count;
        n=rn;
        pos += n;

        self.column_type = data[pos..pos + self.column_count as usize].to_owned();
        pos += self.column_count as usize;

        Ok(())
    }
}
pub fn fixed_length_int(buf:Vec<u8>) -> u64 {
    let mut num = 0;
    let mut i =0;
    for b in buf.iter(){
        num|= b<< i*8;
        i+=1;
    }
    num.into()
}

pub fn length_encoded_int(b:&[u8]) -> (u64,bool,usize) {
    if b.len()==0{
        return (0,true,1)
    }
    return match b[0] {
        0xfb => {
            (0, true, 1)
        }
        0xfc => {
            (u16big(&b[1..]) as u64, false, 3)
        }
        0xfd => {
            (u32big(&b[1..]) as u64, false, 3)
        }
        0xfe => {
            (u64big(&b[1..]), false, 9)
        }
        _ => {
            (b[0] as u64, false, 1)
        }
    }
}