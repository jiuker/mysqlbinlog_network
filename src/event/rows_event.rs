use crate::event::event::Event;
use crate::pkg::end_dian::{u16big, u16lit, u24lit, u32big, u32lit, u64big, u64lit};
use crate::pkg::err::Result;
use crate::pkg::vec::set_to_vec;
use std::collections::HashMap;
use std::fs::{read, read_to_string};
use std::io::{BufWriter, Bytes, Write};
use std::thread::sleep;
use std::time::SystemTime;
use std::fmt::Debug;

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

const TABLE_MAP_OPT_META_SIGNEDNESS: u8 = 1;
const TABLE_MAP_OPT_META_DEFAULT_CHARSET: u8 = 2;
const TABLE_MAP_OPT_META_COLUMN_CHARSET: u8 = 3;
const TABLE_MAP_OPT_META_COLUMN_NAME: u8 = 4;
const TABLE_MAP_OPT_META_SET_STR_VALUE: u8 = 5;
const TABLE_MAP_OPT_META_ENUM_STR_VALUE: u8 = 6;
const TABLE_MAP_OPT_META_GEOMETRY_TYPE: u8 = 7;
const TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY: u8 = 8;
const TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX: u8 = 9;
const TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET: u8 = 10;
const TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET: u8 = 11;

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

        self.decode_optional_meta(&data[pos..])?;

        Ok(())
    }
}
impl TableMapEvent {
    pub fn decode_str_value(&mut self, v: &[u8]) -> Result<Vec<Vec<Vec<u8>>>> {
        let mut pos = 0;
        let mut ret = vec![];
        while pos < v.len() {
            let (n_val, _, n) = length_encoded_int(&v[pos..]);
            pos += n;
            let mut vals = vec![vec![0u8]];
            let mut i = 0;
            while i < n_val {
                let (val, _, n) = length_encoded_string(&v[pos..])?;
                pos += n as usize;
                vals.push(val);
                i += 1;
            }
            ret.push(vals);
        }
        Ok(ret)
    }
    pub fn decode_int_seq(&mut self, v: &[u8]) -> Result<Vec<u64>> {
        let mut pos = 0;
        let mut ret = vec![];
        while pos < v.len() {
            let (i, _, n) = length_encoded_int(&v[pos..]);
            pos += n;
            ret.push(i);
        }
        Ok(ret)
    }
    pub fn decode_default_charset(&mut self, v: &[u8]) -> Result<Vec<u64>> {
        let ret = self.decode_int_seq(v)?;
        if ret.len() % 2 != 1 {
            return Err(Box::from("Expect odd item in DefaultCharset but got %d"));
        }
        return Ok(ret);
    }
    pub fn decode_column_names(&mut self, v: &[u8]) -> Result<()> {
        let mut pos = 0;
        let a = [[1, 2]];
        self.column_name = Default::default();
        while pos < v.len() {
            let n = *v.get(pos).unwrap() as usize;
            pos += 1;
            let _tmp = &v[pos..pos + n];
            self.column_name.push(Vec::from(_tmp));
            pos += n;
        }
        if self.column_name.len() != self.column_count as usize {
            return Err(Box::from("Expect %d column names but got %d"));
        }
        Ok(())
    }
    pub fn decode_simple_primary_key(&mut self, v: &[u8]) -> Result<()> {
        let mut pos = 0;
        while pos < v.len() {
            let (i, _, n) = length_encoded_int(&v[pos..]);
            self.primary_key.push(i);
            self.primary_key_prefix.push(0);
            pos += n;
        }
        Ok(())
    }
    pub fn decode_primary_key_with_prefix(&mut self, v: &[u8]) -> Result<()> {
        let mut pos = 0;
        while pos < v.len() {
            let (i, _, n) = length_encoded_int(&v[pos..]);
            self.primary_key.push(i);
            pos += n;
            let (i, _, n) = length_encoded_int(&v[pos..]);
            self.primary_key_prefix.push(i);
            pos += n;
        }
        Ok(())
    }
    pub fn decode_optional_meta(&mut self, data: &[u8]) -> Result<()> {
        let mut pos = 0;
        while pos < data.len() {
            let t = *data.get(pos).unwrap();
            pos += 1;
            let (l, _, n) = length_encoded_int(&data[pos..]);
            pos += n;
            let v = &data[pos..pos + (l as usize)];
            pos += l as usize;
            match t {
                TABLE_MAP_OPT_META_SIGNEDNESS => {
                    self.signedness_bitmap = v.to_owned();
                }
                TABLE_MAP_OPT_META_DEFAULT_CHARSET => {
                    self.default_charset = self.decode_default_charset(v)?;
                }
                TABLE_MAP_OPT_META_COLUMN_CHARSET => {
                    self.column_charset = self.decode_int_seq(v)?
                }
                TABLE_MAP_OPT_META_COLUMN_NAME => self.decode_column_names(v)?,

                TABLE_MAP_OPT_META_SET_STR_VALUE => {
                    self.set_str_value = self.decode_str_value(v)?;
                }
                TABLE_MAP_OPT_META_ENUM_STR_VALUE => {
                    self.enum_str_value = self.decode_str_value(v)?;
                }
                TABLE_MAP_OPT_META_GEOMETRY_TYPE => {
                    self.geometry_type = self.decode_int_seq(v)?;
                }
                TABLE_MAP_OPT_META_SIMPLE_PRIMARY_KEY => {
                    self.decode_simple_primary_key(v)?;
                }
                TABLE_MAP_OPT_META_PRIMARY_KEY_WITH_PREFIX => {
                    self.decode_primary_key_with_prefix(v)?;
                }
                TABLE_MAP_OPT_META_ENUM_AND_SET_DEFAULT_CHARSET => {
                    self.enum_set_default_charset = self.decode_default_charset(v)?;
                }
                TABLE_MAP_OPT_META_ENUM_AND_SET_COLUMN_CHARSET => {
                    self.enum_set_column_charset = self.decode_int_seq(v)?;
                }
                _ => {}
            }
        }
        Ok(())
    }
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
    Nil,
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    VecByte(Vec<u8>),
    Str(String),
}
pub struct RowsEvent {
    version: i32,
    table_idsize: i32,
    tables: HashMap<u64, TableMapEvent>,
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
    rows: Vec<Vec<RowsEventRowType>>,
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
        // var n int
        let mut n = 0;
        let (column_count, _, n) = length_encoded_int(&data[pos..]);
        self.column_count = column_count;
        pos += n;
        let bit_count = bitmap_byte_size(self.column_count);
        self.column_bitmap1 = data[pos..pos + bit_count as usize].to_vec();
        pos += bit_count as usize;
        if self.need_bitmap2 {
            self.column_bitmap2 = data[pos..pos + bit_count as usize].to_vec();
            pos += bit_count as usize;
        }

        if self.tables.get(&self.table_id).is_none() {
            return if self.tables.len() > 0 {
                Err(Box::from(format!(
                    "invalid table id {}, no corresponding table map event",
                    self.table_id
                )))
            } else {
                Err(Box::from(format!(
                    "errMissingTableMapEvent;table id {}",
                    self.table_id
                )))
            };
        }
        let mut rows_len = self.column_count;

        if self.need_bitmap2 {
            rows_len += self.column_count;
        }
        self.rows = Default::default();
        while pos < data.len() {
            let n = self.decode_rows(&data[pos..], &self.column_bitmap1.clone())?;
            pos += n;
            if self.need_bitmap2 {
                let n = self.decode_rows(&data[pos..], &self.column_bitmap2.clone())?;
                pos += n;
            }
        }
        Ok(())
    }
}

pub fn is_bit_set(bitmap: &[u8], i: usize) -> bool {
    bitmap[i >> 3] & (1 << (i as u32 & 7)) > 0
}
impl RowsEvent {
    pub fn parse_frac_time(v:FracTime)->String{
        v.string()
    }
    pub fn decode_rows(&mut self, data: &[u8], bitmap: &[u8]) -> Result<usize> {
        let mut row = vec![];
        // let mut row = vec![RowsEventRowType::Nil; self.column_count as usize];
        let mut pos = 0;
        let mut count = 0;
        for i in 0..self.column_count {
            if is_bit_set(bitmap, i as usize) {
                count += 1;
            }
        }
        count = (count + 7) / 8;
        // nullBitmap := data[pos : pos+count]
        let nullBitmap = data[pos..pos + count].to_vec();
        // pos += count
        pos += count;
        // nullbitIndex := 0
        let mut nullbitIndex = 0;
        // var n int
        let n = 0;
        // var err error
        for i in 0..self.column_count {
            if !is_bit_set(bitmap, i as usize) {
                continue;
            }
            let isNull =
                ((nullBitmap[nullbitIndex / 8]) as u32 >> (nullbitIndex % 8) as u32) & 0x01;
            nullbitIndex += 1;
            if isNull > 0 {
                row.push(RowsEventRowType::Nil);
                continue;
            }
            let (d, n) = self.decode_value(
                &data[pos..],
                *self
                    .table
                    .as_ref()
                    .unwrap()
                    .column_type
                    .get(i as usize)
                    .unwrap(),
                *self
                    .table
                    .as_ref()
                    .unwrap()
                    .column_meta
                    .get(i as usize)
                    .unwrap(),
            )?;
            row.push(d);
            pos += n as usize;
        }
        self.rows.push(row);
        Ok(pos)
    }
    pub fn decode_value(
        &mut self,
        data: &[u8],
        tp: u8,
        meta: u16,
    ) -> Result<(RowsEventRowType, i32)> {
        let mut tp = tp;
        // var length int = 0
        let mut length = 0;
        //
        // 	if tp == MYSQL_TYPE_STRING {
        // 		if meta >= 256 {
        // 			b0 := uint8(meta >> 8)
        // 			b1 := uint8(meta & 0xFF)
        //
        // 			if b0&0x30 != 0x30 {
        // 				length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
        // 				tp = byte(b0 | 0x30)
        // 			} else {
        // 				length = int(meta & 0xFF)
        // 				tp = b0
        // 			}
        // 		} else {
        // 			length = int(meta)
        // 		}
        // 	}
        //
        if tp == MYSQL_TYPE_STRING {
            if meta >= 256 {
                let b0 = (meta >> 8) as u8;
                let b1 = (meta >> 0xFF) as u8;
                if b0 & 0x30 != 0x30 {
                    length = (b1 as u16 | ((b0 & 0x30) ^ 0x30) << 4) as usize;
                    tp = (b0 | 0x30) as u8;
                } else {
                    length = (meta & 0xFF) as usize;
                    tp = b0;
                }
            }
        } else {
            length = meta as usize;
        }
        let mut n;
        let mut v;
        match tp {
            MYSQL_TYPE_NULL => Ok((RowsEventRowType::Nil, 0)),
            MYSQL_TYPE_TINY => {
                n = 1;
                v = RowsEventRowType::I64(parse_binary_int8(&data) as i64)
            }
            MYSQL_TYPE_SHORT => {
                n = 2;
                v = RowsEventRowType::I64(parse_binary_int16(&data) as i64)
            }
            MYSQL_TYPE_INT24 => {
                n = 3;
                v = RowsEventRowType::I64(parse_binary_int24(&data) as i64)
            }
            MYSQL_TYPE_LONG => {
                n = 4;
                v = RowsEventRowType::I64(parse_binary_int32(&data) as i64);
            }
            MYSQL_TYPE_LONGLONG => {
                n = 8;
                v = RowsEventRowType::I64(parse_binary_int64(&data) as i64);
            }
            MYSQL_TYPE_NEWDECIMAL => {
                let prec = (meta >> 8) as u8;
                let scale = (meta & 0xFF) as u8;
                let (v_, n_) = decode_decimal(&data, prec as i32, scale as i32, self.use_decimal)?;
                v = v_;
                n = n_;
            }
            MYSQL_TYPE_FLOAT => {
                n = 4;
                v = RowsEventRowType::F64(parse_binary_float32(&data) as f64);
            }
            MYSQL_TYPE_DOUBLE => {
                n = 8;
                v = RowsEventRowType::F64(parse_binary_float64(&data) as f64);
            }
            MYSQL_TYPE_BIT => {
                let nbits = ((meta >> 8) * 8) + (meta & 0xFF);
                n = (nbits + 7) as i32 / 8;
                v = RowsEventRowType::I64(decode_bit(&data,nbits as i32,n)?);
            }
            MYSQL_TYPE_TIMESTAMP=>{
                n = 4;
                let t = u32lit(data);
                if  t == 0{
                    v = RowsEventRowType::Str(format_zero_time(0,0));
                }else{
                    v = RowsEventRowType::Str(self.parse_frac_time(FracTime{
                        time_s: time::OffsetDateTime::from_unix_timestamp_nanos(t as i128).unwrap().time(),
                        dec: 0
                    }))
                }
            }
            MYSQL_TYPE_TIMESTAMP2=>{
                let (v_, n_) = decode_timestamp2(&data,meta)?;
                v = RowsEventRowType::Str(self.parse_frac_time(v_));
                n = n_;
            }
            MYSQL_TYPE_DATETIME=>{
                n = 8;
                let i64 = u64lit(&data);
                if i64 == 0{
                    v = RowsEventRowType::Str(format_zero_time(0,0));
                }else{
                    let d = i64/1000000;
                    let t = i64 % 1000000;
                    v = RowsEventRowType::Str(self.parse_frac_time(FracTime{ time_s: time::OffsetDateTime::from_unix_timestamp_nanos(i64 as i128).unwrap().time(), dec: 0 }))
                }
            }
            MYSQL_TYPE_DATETIME2=>{
            }
            _ => {}
        }
        // 	switch tp {
        // 	case MYSQL_TYPE_DATETIME2:
        // 		v, n, err = decodeDatetime2(data, meta)
        // 		v = e.parseFracTime(v)
        // 	case MYSQL_TYPE_TIME:
        // 		n = 3
        // 		i32 := uint32(FixedLengthInt(data[0:3]))
        // 		if i32 == 0 {
        // 			v = "00:00:00"
        // 		} else {
        // 			sign := ""
        // 			if i32 < 0 {
        // 				sign = "-"
        // 			}
        // 			v = fmt.Sprintf("%s%02d:%02d:%02d", sign, i32/10000, (i32%10000)/100, i32%100)
        // 		}
        // 	case MYSQL_TYPE_TIME2:
        // 		v, n, err = decodeTime2(data, meta)
        // 	case MYSQL_TYPE_DATE:
        // 		n = 3
        // 		i32 := uint32(FixedLengthInt(data[0:3]))
        // 		if i32 == 0 {
        // 			v = "0000-00-00"
        // 		} else {
        // 			v = fmt.Sprintf("%04d-%02d-%02d", i32/(16*32), i32/32%16, i32%32)
        // 		}
        //
        // 	case MYSQL_TYPE_YEAR:
        // 		n = 1
        // 		year := int(data[0])
        // 		if year == 0 {
        // 			v = year
        // 		} else {
        // 			v = year + 1900
        // 		}
        // 	case MYSQL_TYPE_ENUM:
        // 		l := meta & 0xFF
        // 		switch l {
        // 		case 1:
        // 			v = int64(data[0])
        // 			n = 1
        // 		case 2:
        // 			v = int64(binary.LittleEndian.Uint16(data))
        // 			n = 2
        // 		default:
        // 			err = fmt.Errorf("Unknown ENUM packlen=%d", l)
        // 		}
        // 	case MYSQL_TYPE_SET:
        // 		n = int(meta & 0xFF)
        // 		nbits := n * 8
        //
        // 		v, err = littleDecodeBit(data, nbits, n)
        // 	case MYSQL_TYPE_BLOB:
        // 		v, n, err = decodeBlob(data, meta)
        // 	case MYSQL_TYPE_VARCHAR,
        // 		MYSQL_TYPE_VAR_STRING:
        // 		length = int(meta)
        // 		v, n = decodeString(data, length)
        // 	case MYSQL_TYPE_STRING:
        // 		v, n = decodeString(data, length)
        // 	case MYSQL_TYPE_JSON:
        // 		// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/master/src/main/java/com/github/shyiko/mysql/binlog/event/deserialization/AbstractRowsEventDataDeserializer.java#L404
        // 		length = int(FixedLengthInt(data[0:meta]))
        // 		n = length + int(meta)
        // 		v, err = e.decodeJsonBinary(data[meta:n])
        // 	case MYSQL_TYPE_GEOMETRY:
        // 		// MySQL saves Geometry as Blob in binlog
        // 		// Seem that the binary format is SRID (4 bytes) + WKB, outer can use
        // 		// MySQL GeoFromWKB or others to create the geometry data.
        // 		// Refer https://dev.mysql.com/doc/refman/5.7/en/gis-wkb-functions.html
        // 		// I also find some go libs to handle WKB if possible
        // 		// see https://github.com/twpayne/go-geom or https://github.com/paulmach/go.geo
        // 		v, n, err = decodeBlob(data, meta)
        // 	default:
        // 		err = fmt.Errorf("unsupport type %d in binlog and don't know how to handle", tp)
        // 	}
        Ok((RowsEventRowType::Nil, 0))
    }
}
struct FracTime {
    pub time_s:time::Time,
    // Dec must in [0, 6]
    pub dec:i32,
}

impl FracTime{
    pub fn string(&self)->String{
        self.time_s.to_string()
    }
}

pub fn parse_binary_int8(data: &[u8]) -> i8 {

    *data[0] as i8
}
pub fn parse_binary_uint8(data: &[u8]) -> u8 {
    *data[0]
}
pub fn parse_binary_int16(data: &[u8]) -> i16 {
    u16lit(data) as i16
}

pub fn parse_binary_uint16(data: &[u8]) -> u16 {
    u16lit(data)
}
pub fn parse_binary_int24(data: &[u8]) -> i32 {
    let mut u32 = parse_binary_uint24(data);
    if u32 & 0x00800000 != 0 {
        u32 |= 0xFF000000
    }
    return u32 as i32;
}

pub fn parse_binary_uint24(data: &[u8]) -> u32 {
    u24lit(data)
}
pub fn parse_binary_int32(data: &[u8]) -> i32 {
    u32lit(data) as i32
}
pub fn parse_binary_uint32(data: &[u8]) -> u32 {
    u32lit(data)
}
pub fn parse_binary_int64(data: &[u8]) -> i64 {
    u64lit(data) as i64
}
pub fn parse_binary_uint64(data: &[u8]) -> u64 {
    u64lit(data)
}
pub fn parse_binary_float32(data: &[u8]) -> f32 {
    u32lit(data) as f32
}
pub fn parse_binary_float64(data: &[u8]) -> f64 {
    u64lit(data) as f64
}
const digitsPerInteger: i32 = 9;
const compressedBytes: Vec<u8> = vec![0, 1, 1, 2, 2, 3, 3, 4, 4, 4];
pub fn decode_decimal(
    data: &[u8],
    precision: i32,
    decimals: i32,
    use_decimal: bool,
) -> Result<(RowsEventRowType, i32)> {
    let integral = precision - decimals;
    let uncomp_integral = (integral / digitsPerInteger);
    let uncomp_fractional = (decimals / digitsPerInteger);
    let comp_integral = integral - (uncomp_integral * digitsPerInteger);
    let comp_fractional = decimals - (uncomp_fractional * digitsPerInteger);

    let bin_size = uncomp_integral * 4
        + compressedBytes[comp_integral] as i32
        + uncomp_fractional * 4
        + compressedBytes[comp_fractional] as i32;
    let buf = Vec::from(&data[0..bin_size as usize]);
    let mut data = buf.clone();
    let value = data[0] as u32;
    let mut res = vec![];
    let mut mask = 0;
    if value & 0x80 == 0 {
        mask = ((1 << 32) - 1) as u32;
        res.extend(format!("{}", "-").as_bytes());
    }
    data[0] ^= 0x80;
    let (mut pos, mut value) = decode_decimal_decompress_value(comp_integral, &data, mask as u8);

    res.extend(value.to_be_bytes());

    for i in 0..uncomp_integral {
        value = u32big(&data[pos..]) ^ mask;
        pos += 4;
        res.extend(format!("{:09}", value).as_bytes());
    }
    res.extend(format!("{}", ".").as_bytes());
    for i in 0..uncomp_fractional {
        value = u32big(&data[pos..]) ^ mask;
        pos += 4;
        res.extend(format!("{:09}", value).as_bytes());
    }
    let (size, value) = decode_decimal_decompress_value(comp_fractional, &data[pos..], mask as u8);
    if size > 0 {
        res.extend(format!("{value:0width$}", width=(compFractional as usize), value=value).as_bytes());
        pos += size;
    }
    if use_decimal {
        // f, err := decimal.NewFromString(hack.String(res.Bytes()))
        // return f, pos, err
        todo!("如果使用decimal 需要处理!");
    }
    let f = String::from_utf8(res).unwrap().parse::<f64>()?;
    Ok((RowsEventRowType::F64(f), pos as i32))
}
pub fn decode_decimal_decompress_value(comp_indx: i32, data: &[u8], mask: u8) -> (usize, u32) {
    let size = compressedBytes[comp_indx];
    let mut databuff = vec![0u8; size as usize];
    for i in 0..size {
        databuff[i] = data[i] ^ mask;
    }
    let value = (bfixed_length_int(&databuff)) as u32;
    return (size, value);
}

pub fn bfixed_length_int(buf: &[u8]) -> u64 {
    let mut num = 0;
    for i in 0..buf.len() {
        num |= (b as u64) << (((len(buf) - i - 1) as u32 * 8) as u64);
    }
    return num;
}
pub fn decode_bit(data: &[u8], nbits: i32, length: i32) -> Result<i64> {
    let mut value = 0;
    if nbits > 1 {
        match length {
            1 => {
                value = data[0] as i64;
            }
            2 => {
                value = u16big(&data) as i64;
            }
            3 => {
                value = bfixed_length_int(&data[0..3]) as i64;
            }
            4 => {
                value = u32big(data) as i64;
            }
            5 | 6 | 7 => value = int64(bfixed_length_int(&data[0..length as usize])),
            8 => {
                value = u64big(data) as i64;
            }
            _ => return Err(Box::from(format!("invalid bit length {}", length))),
        }
    }else{
        if  length !=1{
            return Err(Box::from(format!("invalid bit length {}", length)))

        }else{
            value = data[0] as i64;
        }
    };
    Ok(value)
}

pub fn format_zero_time(frac:i32, dec:i32)->String {
    if dec==0{
        return String::from("0000-00-00 00:00:00")
    }
    let s = format!("0000-00-00 00:00:00.{:06}",frac);
    return s[0:s.len()-(6-dec)];
}
pub fn decode_timestamp2(data:&[u8], dec:u16)->Result<(FracTime, i32)> {
    // n := int(4 + (dec+1)/2)
    let n = (4+(dec+1)/2) as i32;
    // 	sec := int64(binary.BigEndian.Uint32(data[0:4]))
    let sec = u32big(&data[0..4]);
    let mut usec = 0;
    // 	usec := int64(0)
    match dec {
        1|2=>{
            usec = (*data[4])*10000
        }
        3|4=>{
            usec = u16big(&data[pos..]) as i64 *100
        }
        5|6=>{
            usec = bfixed_length_int(&data[4..7]) as i64;
        }
        _ => {}
    }
    if sec == 0{
        return Ok((FracTime{ time_s: time::Time::MIDNIGHT, dec: 0 },n));
    }
    return Ok((FracTime{
        time_s:time::OffsetDateTime::from_unix_timestamp_nanos((sec * 1000000000 + usec * 1000) as i128).unwrap().time(),
        dec:dec as i32,
    },n))
}
pub fn decode_datetime2(){

}