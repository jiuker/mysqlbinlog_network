/// MySQL uses a bizarro custom encoding that they call JSONB (no relation to the PostgreSQL column
/// type) for JSON values. No, I don't know why they didn't just use BSON or CBOR. I think they
/// might just hate me.
use std::io::Cursor;
use std::iter::FromIterator;

use base64;
use byteorder::{LittleEndian, ReadBytesExt};
use serde_json::map::Map as JsonMap;
use serde_json::Value as JsonValue;

use crate::mysql_binlog::column_types::ColumnType;
use crate::mysql_binlog::errors::JsonbParseError;
use crate::mysql_binlog::packet_helpers;

enum FieldType {
    SmallObject,
    LargeObject,
    SmallArray,
    LargeArray,
    Literal,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int64,
    Uint64,
    Double,
    JsonString,
    Custom,
}

impl FieldType {
    fn from_byte(u: u8) -> Result<Self, JsonbParseError> {
        Ok(match u {
            0x00 => FieldType::SmallObject,
            0x01 => FieldType::LargeObject,
            0x02 => FieldType::SmallArray,
            0x03 => FieldType::LargeArray,
            0x04 => FieldType::Literal,
            0x05 => FieldType::Int16,
            0x06 => FieldType::Uint16,
            0x07 => FieldType::Int32,
            0x08 => FieldType::Uint32,
            0x09 => FieldType::Int64,
            0x0a => FieldType::Uint64,
            0x0b => FieldType::Double,
            0x0c => FieldType::JsonString,
            0x0f => FieldType::Custom,
            i => return Err(JsonbParseError::InvalidTypeByte(i)),
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CompoundSize {
    Small,
    Large,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CompoundType {
    Object,
    Array,
}

pub fn parse(blob: Vec<u8>) -> Result<JsonValue, JsonbParseError> {
    let mut cursor = Cursor::new(blob);
    parse_any(&mut cursor)
}

#[derive(Debug)]
enum OffsetOrInline {
    Inline(JsonValue),
    Offset(u32),
}

fn parse_maybe_inlined_value(
    cursor: &mut Cursor<Vec<u8>>,
    compound_size: CompoundSize,
) -> Result<(u8, OffsetOrInline), JsonbParseError> {
    let t = cursor.read_u8()?;
    let inlined_value = match FieldType::from_byte(t) {
        Ok(FieldType::Literal) => match cursor.read_u16::<LittleEndian>()? {
            0x00 => JsonValue::Null,
            0x01 => JsonValue::Bool(true),
            0x02 => JsonValue::Bool(false),
            i => return Err(JsonbParseError::InvalidLiteral(i).into()),
        },
        Ok(FieldType::Uint16) => JsonValue::from(cursor.read_u16::<LittleEndian>()?),
        Ok(FieldType::Int16) => JsonValue::from(cursor.read_i16::<LittleEndian>()?),
        Ok(FieldType::Uint32) => JsonValue::from(cursor.read_u32::<LittleEndian>()?),
        Ok(FieldType::Int32) => JsonValue::from(cursor.read_i32::<LittleEndian>()?),
        Ok(_) | Err(_) => {
            return Ok((
                t,
                OffsetOrInline::Offset(match compound_size {
                    CompoundSize::Small => u32::from(cursor.read_u16::<LittleEndian>()?),
                    CompoundSize::Large => cursor.read_u32::<LittleEndian>()?,
                }),
            ));
        }
    };
    Ok((t, OffsetOrInline::Inline(inlined_value)))
}

fn parse_compound(
    mut cursor: &mut Cursor<Vec<u8>>,
    compound_size: CompoundSize,
    compound_type: CompoundType,
) -> Result<JsonValue, JsonbParseError> {
    let data_length = cursor.get_ref().len();
    let offset_size = match compound_size {
        CompoundSize::Small => 2,
        CompoundSize::Large => 4,
    };
    if data_length < offset_size {
        return Ok(JsonValue::Null);
    }
    let (count, size) = match compound_size {
        CompoundSize::Small => (
            u32::from(cursor.read_u16::<LittleEndian>()?) as usize,
            u32::from(cursor.read_u16::<LittleEndian>()?) as usize,
        ),
        CompoundSize::Large => (
            cursor.read_u32::<LittleEndian>()? as usize,
            cursor.read_u32::<LittleEndian>()? as usize,
        ),
    };
    if data_length < size as usize {
        return Ok(JsonValue::Null);
    }
    let (key_entry_size, value_entry_size) = match compound_size {
        CompoundSize::Small => (4, 3),
        CompoundSize::Large => (6, 5),
    };
    let mut header_size = 2 * offset_size + count * value_entry_size;
    header_size += match compound_type {
        CompoundType::Array => 0,
        CompoundType::Object => count * key_entry_size,
    };
    if header_size > size as usize {
        return Ok(JsonValue::Null);
    }
    let keys = match compound_type {
        CompoundType::Array => None,
        CompoundType::Object => {
            let mut rsl = vec![];
            for i in 0..count {
                let entry_offset = 2 * offset_size + key_entry_size * i;
                cursor.set_position(entry_offset as u64 + 1);
                let key_offset = match compound_size {
                    CompoundSize::Small => cursor.read_u16::<LittleEndian>()? as usize,
                    CompoundSize::Large => cursor.read_u32::<LittleEndian>()? as usize,
                };
                let key_length = cursor.read_u16::<LittleEndian>()? as usize;
                if data_length < (key_offset) as usize + key_length {
                    return Ok(JsonValue::Null);
                }
                cursor.set_position(key_offset as u64 + 1);
                let key = packet_helpers::read_nbytes(&mut cursor, key_length)?;
                let key = String::from_utf8_lossy(&key).into_owned();
                rsl.push(key);
            }
            Some(rsl)
        }
    };
    let values = {
        let mut rsl = vec![];
        for i in 0..count {
            let mut entry_offset = 2 * offset_size + value_entry_size * i;
            // if isObject {
            //     entryOffset += keyEntrySize * count
            // }
            entry_offset += match compound_type {
                CompoundType::Array => 0,
                CompoundType::Object => key_entry_size * count,
            };
            let tp_data = cursor.get_ref().get(entry_offset + 1).unwrap();
            let tp_data_c = (*tp_data).clone();
            let tp = FieldType::from_byte(*tp_data)?;
            let is_inline = match tp {
                FieldType::Uint16 | FieldType::Int16 | FieldType::Literal => true,
                FieldType::Int32 | FieldType::Uint32 => !match compound_size {
                    CompoundSize::Small => true,
                    CompoundSize::Large => false,
                },
                _ => false,
            };
            if is_inline {
                let data = &cursor.get_ref()[entry_offset + 1..entry_offset + value_entry_size + 1];
                let mut cur = Cursor::new(data.to_vec());
                let value = parse_any(&mut cur)?;
                rsl.push(value);
                continue;
            }
            cursor.set_position(entry_offset as u64 + 2);
            let value_offset = match compound_size {
                CompoundSize::Small => u32::from(cursor.read_u16::<LittleEndian>()?) as usize,
                CompoundSize::Large => u32::from(cursor.read_u32::<LittleEndian>()?) as usize,
            };
            if data_length < value_offset {
                return Ok(JsonValue::Null);
            }
            let mut data = vec![tp_data_c];
            data.extend_from_slice(&cursor.get_ref()[value_offset + 1..data_length]);
            let mut cur = Cursor::new(data);
            let value = parse_any(&mut cur)?;
            rsl.push(value);
        }
        rsl
    };
    Ok(if let Some(keys) = keys {
        let map = JsonMap::from_iter(keys.into_iter().zip(values.into_iter()));
        JsonValue::Object(map)
    } else {
        JsonValue::Array(values)
    })
}

fn parse_any(cursor: &mut Cursor<Vec<u8>>) -> Result<JsonValue, JsonbParseError> {
    let type_indicator = FieldType::from_byte(cursor.read_u8()?)?;
    parse_any_with_type_indicator(cursor, type_indicator)
}

fn parse_any_with_type_indicator(
    mut cursor: &mut Cursor<Vec<u8>>,
    type_indicator: FieldType,
) -> Result<JsonValue, JsonbParseError> {
    match type_indicator {
        FieldType::Literal => Ok(match cursor.read_u8()? {
            0x00 => JsonValue::Null,
            0x01 => JsonValue::Bool(true),
            0x02 => JsonValue::Bool(false),
            i => return Err(JsonbParseError::InvalidLiteral(u16::from(i)).into()),
        }),
        FieldType::Int16 => {
            let val = cursor.read_i16::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Uint16 => {
            let val = cursor.read_u16::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Int32 => {
            let val = cursor.read_i32::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Uint32 => {
            let val = cursor.read_u32::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Int64 => {
            let val = cursor.read_i64::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Uint64 => {
            let val = cursor.read_u64::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::Double => {
            let val = cursor.read_f64::<LittleEndian>()?;
            Ok(JsonValue::from(val))
        }
        FieldType::JsonString => {
            let val = packet_helpers::read_variable_length_string(&mut cursor)?;
            Ok(JsonValue::from(val))
        }
        FieldType::SmallObject => {
            parse_compound(&mut cursor, CompoundSize::Small, CompoundType::Object)
        }
        FieldType::LargeObject => {
            parse_compound(&mut cursor, CompoundSize::Large, CompoundType::Object)
        }
        FieldType::SmallArray => {
            parse_compound(&mut cursor, CompoundSize::Small, CompoundType::Array)
        }
        FieldType::LargeArray => {
            parse_compound(&mut cursor, CompoundSize::Large, CompoundType::Array)
        }
        FieldType::Custom => {
            /* augh apparently MySQL has this "neat" feature where it can encode any MySQL type
             * inside JSON.
             *
             * easiest way to trigger it is with INSERT...SELECT
             *
             * it looks like it's only implemented for DECIMAL and the various time types as of
             * MySQL 8.0
             */
            let raw_mysql_column_type = cursor.read_u8()?;
            let column_type = ColumnType::from_byte(raw_mysql_column_type);
            let payload = packet_helpers::read_variable_length_bytes(&mut cursor)?;
            match column_type {
                ColumnType::NewDecimal(..)
                | ColumnType::Date
                | ColumnType::Time
                | ColumnType::Timestamp
                | ColumnType::DateTime
                | ColumnType::DateTime2(..)
                | ColumnType::Time2(..)
                | ColumnType::Timestamp2(..) => {
                    let mut cursor = Cursor::new(payload);
                    let column_type = column_type.read_metadata(&mut cursor)?;
                    let value = column_type.read_value(&mut cursor)?;
                    Ok(value.as_value()?.into_owned())
                }
                _ => {
                    let serialized_payload = base64::encode(&payload);
                    let mut m = JsonMap::with_capacity(2);
                    m.insert(
                        "column_type".to_owned(),
                        JsonValue::from(raw_mysql_column_type),
                    );
                    m.insert(
                        "base64_payload".to_owned(),
                        JsonValue::from(serialized_payload),
                    );
                    Ok(JsonValue::from(m))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::parse;

    #[test]
    pub fn test_i16() {
        let blob = vec![5u8, 1, 0];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!(1));
    }

    #[test]
    pub fn test_string() {
        let blob = vec![12u8, 3, 102, 111, 111];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!("foo"));
    }

    #[test]
    pub fn test_nested() {
        let blob = vec![
            0u8, 1, 0, 46, 0, 11, 0, 1, 0, 2, 12, 0, 97, 4, 0, 34, 0, 5, 1, 0, 5, 2, 0, 12, 16, 0,
            0, 22, 0, 5, 116, 104, 114, 101, 101, 1, 0, 12, 0, 11, 0, 1, 0, 5, 4, 0, 52,
        ];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!({"a":[1,2,"three",{"4":4}]}));
    }

    #[test]
    pub fn test_inline_null() {
        let blob = vec![0u8, 1, 0, 12, 0, 11, 0, 1, 0, 4, 0, 0, 97];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!({ "a": null }));
    }

    #[test]
    pub fn test_inline_false() {
        let blob = vec![0, 1, 0, 12, 0, 11, 0, 1, 0, 4, 2, 0, 97];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!({"a": false}));
    }

    #[test]
    pub fn test_array() {
        let blob = vec![
            2, 5, 0, 21, 0, 4, 1, 0, 4, 2, 0, 4, 0, 0, 5, 0, 0, 12, 19, 0, 1, 48,
        ];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!([true, false, null, 0, "0"]));
    }

    #[test]
    pub fn test_opaque_decimal() {
        let blob = vec![15, 246, 3, 2, 2, 138];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(parsed, json!({"Decimal":"0.10"}));
    }

    #[test]
    pub fn test_opaque_times() {
        let blob = vec![
            0, 4, 0, 97, 0, 32, 0, 4, 0, 36, 0, 4, 0, 40, 0, 8, 0, 48, 0, 9, 0, 15, 57, 0, 15, 67,
            0, 15, 77, 0, 15, 87, 0, 100, 97, 116, 101, 116, 105, 109, 101, 100, 97, 116, 101, 116,
            105, 109, 101, 116, 105, 109, 101, 115, 116, 97, 109, 112, 10, 8, 0, 0, 0, 0, 0, 188,
            159, 25, 11, 8, 0, 0, 0, 64, 218, 0, 0, 0, 12, 8, 0, 0, 0, 64, 218, 188, 159, 25, 7, 8,
            0, 0, 0, 77, 218, 188, 159, 25,
        ];
        let parsed = parse(blob).expect("should parse");
        assert_eq!(
            parsed,
            json!({"date": null,"datetime":{"DateTime":{"day":7,"hour":82,"minute":69,"month":78,"second":44,"subsecond":0,"year":184640201}},"time":{"Time":{"hours":0,"minutes":0,"seconds":0,"subseconds":0}},"timestamp":{"Timestamp":{"subsecond":0,"unix_time":1291845632}}})
        );
    }
}
