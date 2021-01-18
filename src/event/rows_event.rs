use crate::event::event::Event;
use crate::pkg::end_dian::{u16big, u16lit, u32big, u32lit, u64big, u64lit};
use crate::pkg::err::Result;
use crate::pkg::vec::set_to_vec;
use std::collections::HashMap;
use std::fs::read;
use std::io::{BufWriter, Write};

const MYSQL_TYPE_JSON: u8 = 245;
const MYSQL_TYPE_NEWDECIMAL: u8 = 246;
const MYSQL_TYPE_ENUM: u8 = 247;
const MYSQL_TYPE_SET: u8 = 248;
const MYSQL_TYPE_TINY_BLOB: u8 = 249;
const MYSQL_TYPE_MEDIUM_BLOB: u8 = 250;
const MYSQL_TYPE_LONG_BLOB: u8 = 251;
const MYSQL_TYPE_BLOB: u8 = 252;
const MYSQL_TYPE_VAR_STRING: u8 = 253;
const MYSQL_TYPE_STRING: u8 = 254;
const MYSQL_TYPE_GEOMETRY: u8 = 255;

const MYSQL_TYPE_DECIMAL: u8 = 0;
const MYSQL_TYPE_TINY: u8 = 1;
const MYSQL_TYPE_SHORT: u8 = 2;
const MYSQL_TYPE_LONG: u8 = 3;
const MYSQL_TYPE_FLOAT: u8 = 4;
const MYSQL_TYPE_DOUBLE: u8 = 5;
const MYSQL_TYPE_NULL: u8 = 6;
const MYSQL_TYPE_TIMESTAMP: u8 = 7;
const MYSQL_TYPE_LONGLONG: u8 = 8;
const MYSQL_TYPE_INT24: u8 = 9;
const MYSQL_TYPE_DATE: u8 = 10;
const MYSQL_TYPE_TIME: u8 = 11;
const MYSQL_TYPE_DATETIME: u8 = 12;
const MYSQL_TYPE_YEAR: u8 = 13;
const MYSQL_TYPE_NEWDATE: u8 = 14;
const MYSQL_TYPE_VARCHAR: u8 = 15;
const MYSQL_TYPE_BIT: u8 = 16;

//mysql 5.6
const MYSQL_TYPE_TIMESTAMP2: u8 = 17;
const MYSQL_TYPE_DATETIME2: u8 = 18;
const MYSQL_TYPE_TIME2: u8 = 19;

pub struct RowsQueryEvent {
    query: Vec<u8>,
}
impl<T: Write + Sized> Event<T> for RowsQueryEvent {
    fn dump(&self, mut w: BufWriter<T>) {
        w.write_fmt(format_args!(
            "Query: {}\n",
            String::from_utf8(self.query.clone()).unwrap()
        ));
        w.write_fmt(format_args!("\n"));
    }

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        self.query = data[1..].to_vec();
        Ok(())
    }
}
pub struct TableMapEvent {
    flavor: String,
    table_idsize: usize,

    table_id: u64,

    flags: u16,

    schema: Vec<u8>,
    table: Vec<u8>,

    column_count: u64,
    column_type: Vec<u8>,
    column_meta: Vec<u16>,

    //len = (column_count + 7) / 8
    null_bitmap: Vec<u8>,

    /*
        The followings are available only after MySQL-8.0.1 or MariaDB-10.5.0
        see:
            - https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
            - https://mysqlhighavailability.com/more-metadata-is-written-into-binary-log/
            - https://jira.mariadb.org/browse/MDEV-20477
    */
    // signedness_bitmap stores signedness info for numeric columns.
    signedness_bitmap: Vec<u8>,

    // default_charset/column_charset stores collation info for character columns.

    // default_charset[0] is the default collation of character columns.
    // For character columns that have different charset,
    // (character column index, column collation) pairs follows
    default_charset: Vec<u64>,
    // column_charset contains collation sequence for all character columns
    column_charset: Vec<u64>,

    // set_str_value stores values for set columns.
    set_str_value: Vec<Vec<Vec<u8>>>,
    set_str_value_string: Vec<Vec<String>>,

    // enum_str_value stores values for enum columns.
    enum_str_value: Vec<Vec<Vec<u8>>>,
    enum_str_value_string: Vec<Vec<String>>,

    // column_name list all column names.
    column_name: Vec<Vec<u8>>,
    column_name_string: Vec<String>, // the same as column_name in string type, just for reuse

    // geometry_type stores real type for geometry columns.
    geometry_type: Vec<u64>,

    // primary_key is a sequence of column indexes of primary key.
    primary_key: Vec<u64>,

    // primary_key_prefix is the prefix length used for each column of primary key.
    // 0 means that the whole column length is used.
    primary_key_prefix: Vec<u64>,

    // enum_set_default_charset/enum_set_column_charset is similar to default_charset/column_charset but for enum/set columns.
    enum_set_default_charset: Vec<u64>,
    enum_set_column_charset: Vec<u64>,
}

impl<T: Write + Sized> Event<T> for TableMapEvent {
    fn dump(&self, w: BufWriter<T>) {}

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        let mut pos = 0;
        self.table_id = fixed_length_int(&data[0..self.table_idsize]);
        pos += self.table_idsize;
        self.flags = u16lit(&data[pos..]);

        let schema_length = data[pos];
        pos += 1;
        self.schema = data[pos..pos + schema_length as usize].to_owned();
        pos += schema_length as usize;
        pos += 1;

        let table_length = data[pos];
        pos += 1;
        self.table = data[pos..pos + table_length as usize].to_owned();
        pos += table_length as usize;
        pos += 1;

        let mut n = 0;
        let (column_count, _, rn) = length_encoded_int(&data[pos..]);
        self.column_count = column_count;
        n = rn;
        pos += n;

        self.column_type = data[pos..pos + self.column_count as usize].to_owned();
        pos += self.column_count as usize;

        let (meta_data, _, n) = length_encoded_string(&data[pos..])?;

        self.decode_meta(meta_data)?;

        pos += n as usize;

        let null_bitmap_size = bitmap_byte_size(self.column_count);
        if data[pos..].len() < null_bitmap_size as usize {
            return Err(Box::from("io.EOF"));
        }

        self.null_bitmap = data[pos..pos + null_bitmap_size as usize].to_owned();

        pos += null_bitmap_size as usize;

        // todo decodeOptionalMeta

        Ok(())
    }
}
impl TableMapEvent {
    pub fn decode_meta(&mut self, data: Vec<u8>) -> Result<()> {
        let mut pos = 0;
        self.column_meta = vec![0; self.column_count as usize];
        let mut i = 0;
        for t in self.column_type.iter() {
            match *t {
                MYSQL_TYPE_STRING | MYSQL_TYPE_NEWDECIMAL => {
                    let mut x = ((data[pos].clone() as u16) << 8 as u16) as u16; //real type
                    x += data[pos + 1].clone() as u16; //pack or field length
                    self.column_meta[i] = x;
                    pos += 2;
                }
                MYSQL_TYPE_VAR_STRING | MYSQL_TYPE_VARCHAR | MYSQL_TYPE_BIT => {
                    self.column_meta[i] = u16lit(&data[pos..]);
                    pos += 2;
                }
                MYSQL_TYPE_BLOB
                | MYSQL_TYPE_DOUBLE
                | MYSQL_TYPE_FLOAT
                | MYSQL_TYPE_GEOMETRY
                | MYSQL_TYPE_JSON
                | MYSQL_TYPE_TIME2
                | MYSQL_TYPE_DATETIME2
                | MYSQL_TYPE_TIMESTAMP2 => {
                    self.column_meta[i] = data[pos].clone() as u16;
                    pos += 1;
                }
                MYSQL_TYPE_NEWDATE
                | MYSQL_TYPE_ENUM
                | MYSQL_TYPE_SET
                | MYSQL_TYPE_TINY_BLOB
                | MYSQL_TYPE_MEDIUM_BLOB
                | MYSQL_TYPE_LONG_BLOB => return Err(Box::from("不支持的数据类型!")),
                _ => {
                    self.column_meta[i] = 0;
                }
            };
            i += 1;
        }
        Ok(())
    }
}
pub fn fixed_length_int(buf: &[u8]) -> u64 {
    let mut num = 0;
    let mut i = 0;
    for b in buf.iter() {
        num |= b << i * 8;
        i += 1;
    }
    num.into()
}

pub fn length_encoded_int(b: &[u8]) -> (u64, bool, usize) {
    if b.len() == 0 {
        return (0, true, 1);
    }
    return match b[0] {
        0xfb => (0, true, 1),
        0xfc => (u16big(&b[1..]) as u64, false, 3),
        0xfd => (u32big(&b[1..]) as u64, false, 3),
        0xfe => (u64big(&b[1..]), false, 9),
        _ => (b[0] as u64, false, 1),
    };
}
pub fn length_encoded_string(b: &[u8]) -> Result<(Vec<u8>, bool, i32)> {
    let (num, isNull, mut n) = length_encoded_int(b);
    if num < 1 {
        return Ok((b[n..n].to_vec(), isNull, n as i32));
    }
    n += num as usize;
    if b.len() >= n {
        let mut rsl = vec![0; n];
        set_to_vec(&mut rsl, n - num as usize, b);
        return Ok((rsl, false, n as i32));
    }
    return Err(Box::from("io.Error"));
}

pub fn bitmap_byte_size(column_count: u64) -> u64 {
    return (column_count + 7) as u64 / 8;
}
pub enum RowsEventRowType {
    I64(i64),
    F64(f64),
    Bool(bool),
    VecByte(Vec<u8>),
    Str(String),
}
pub struct RowsEvent {
    version: i32,
    table_idsize: i32,
    tables: HashMap<String, TableMapEvent>,
    need_bitmap2: bool,
    table: Option<TableMapEvent>,
    table_id: u64,
    flags: u16,
    //if version == 2
    extra_data: Vec<u8>,
    //lenenc_int
    column_count: u64,
    //len = (column_count + 7) / 8
    column_bitmap1: Vec<u8>,
    //if UPDATE_ROWS_EVENTv1 or v2
    //len = (column_count + 7) / 8
    column_bitmap2: Vec<u8>,
    //rows: invalid: int64, float64, bool, []byte, string
    // Rows [][]interface{}
    Rows: Vec<Vec<RowsEventRowType>>,
    parse_time: bool,
    use_decimal: bool,
    ignore_jsondecode_err: bool,
}
impl<T: Write + Sized> Event<T> for RowsEvent {
    fn dump(&self, w: BufWriter<T>) {}

    fn decode(&mut self, data: Vec<u8>) -> Result<()> {
        let mut pos = 0;
        self.table_id = fixed_length_int(&data[0..self.table_idsize as usize]);
        pos += self.table_idsize as usize;

        self.flags = u16lit(&data[pos..]);
        pos += 2;

        if self.version == 2 {
            let dataLen = u16lit(&data[pos..]) as usize;
            pos += 2;
            self.extra_data = data[pos..pos + dataLen - 2].to_owned();
            pos += dataLen - 2;
        }

        Ok(())
    }
}
