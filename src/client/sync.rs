use crate::mysql_binlog;
use crate::mysql_binlog::event::EventData::{EventHeader, FormatDescriptionEvent};
use crate::mysql_binlog::event::{ChecksumAlgorithm, EVENT_HEADER_SIZE};
use crate::mysql_binlog::table_map::TableMap;
use crate::none;
use crate::none_ref;
use crate::pkg::event::Event;
use crate::pkg::mysql_gtid::Gtid;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use mysql::consts::Command;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};
use std::error::Error;
use std::io::{Cursor, Read, Write};
use std::ops::{Deref, DerefMut};
use std::result;
use std::str::FromStr;
pub struct OffsetConfig {
    pub pos: Option<(String, u32)>,
    pub gtid: Option<Gtid>,
}
type Result<T> = result::Result<T, Box<dyn Error>>;
pub struct Runner {
    conn: Conn,
    opt: Opts,
    server_id: u32,
    table_map: TableMap,
    binlog_checksum_length: usize, // if checksum , length = 4
}
impl Deref for Runner {
    type Target = Conn;
    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}
impl DerefMut for Runner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}
impl Runner {
    pub fn new(url: &str, server_id: u32) -> Result<Self> {
        let opt = mysql::Opts::from_url(url)?;
        let conn = mysql::Conn::new(opt.clone())?;
        Ok(Runner {
            conn,
            opt,
            server_id,
            table_map: TableMap::new(),
            binlog_checksum_length: 0,
        })
    }
    fn prepare(&mut self) -> Result<()> {
        self.register_slave()?;
        self.write_register_slave_command()?;
        self.enable_semi_sync()
    }
    fn register_slave(&mut self) -> Result<()> {
        let rsl: Vec<(String, String)> =
            self.query("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")?;
        if rsl.len() == 1 {
            let (_, s) = *none_ref!(rsl.get(0));
            if !s.is_empty() {
                self.query_drop("SET @master_binlog_checksum='NONE'")?;
            }
        }
        self.query_drop("SET @master_heartbeat_period=30000000000;")?;
        Ok(())
    }
    fn write_register_slave_command(&mut self) -> Result<()> {
        let h_name = sys_info::hostname()?;
        let mut data = vec![0u8; 0];
        // slave
        data.write_u32::<LittleEndian>(self.server_id)?;
        data.write_u8(h_name.len() as u8)?;
        data.write_all(h_name.as_bytes())?;
        data.write_u8(none!(self.opt.get_user()).len() as u8)?;
        data.write_all(none!(self.opt.get_user()).as_bytes())?;
        data.write_u8(none!(self.opt.get_pass()).len() as u8)?;
        data.write_all(none!(self.opt.get_pass()).as_bytes())?;
        data.write_u16::<LittleEndian>(self.opt.get_tcp_port())?;
        data.write_u32::<LittleEndian>(0)?;
        data.write_u32::<LittleEndian>(0)?;
        self.write_command(Command::COM_REGISTER_SLAVE, data.as_slice())?;
        let rsl = self.read_packet()?;
        println!("{:?}", rsl);
        Ok(())
    }
    fn enable_semi_sync(&mut self) -> Result<()> {
        let _: Vec<(String, String)> =
            self.query("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';")?;
        self.query_drop("SET @rpl_semi_sync_slave = 1;")?;
        Ok(())
    }
    pub fn start_sync(&mut self, offset: OffsetConfig) -> Result<()> {
        if offset.pos.is_some() {
            self.prepare()?;
            let mut data = vec![0u8; 0];
            data.write_u32::<LittleEndian>(none_ref!(offset.pos).1)?;
            data.write_u16::<LittleEndian>(0)?;
            data.write_u32::<LittleEndian>(self.server_id)?;
            data.write_all(none_ref!(offset.pos).0.as_bytes())?;
            self.write_command(Command::COM_BINLOG_DUMP, data.as_slice())?;
        } else if offset.gtid.is_some() {
            self.prepare()?;
            let mut data = vec![0u8; 0];
            data.write_u16::<LittleEndian>(0)?;
            data.write_u32::<LittleEndian>(self.server_id)?;
            data.write_u32::<LittleEndian>("".len() as u32)?;
            data.write_all("".as_bytes())?;
            data.write_u64::<LittleEndian>(4)?;
            let gtid = none_ref!(offset.gtid);
            let gtiddata = gtid.encode()?;
            data.write_u32::<LittleEndian>(gtiddata.len() as u32)?;
            data.write_all(gtiddata.as_slice())?;
            self.write_command(Command::COM_BINLOG_DUMP_GTID, data.as_slice())?;
        } else {
            return Err(Box::from("show set gtid or fileName/offset"));
        }
        Ok(())
    }
    fn parse_event(&mut self, data: Vec<u8>) -> Result<Event> {
        let header: mysql_binlog::event::EventData;
        // parse Header
        match mysql_binlog::event::EventData::parse_header(&data[1..])? {
            Some(EventHeader {
                timestamp,
                event_type: typ,
                server_id,
                event_size,
                log_pos,
                flags,
            }) => {
                header = mysql_binlog::event::EventData::EventHeader {
                    timestamp,
                    event_type: typ,
                    server_id,
                    event_size,
                    log_pos,
                    flags,
                };
                match typ {
                    mysql_binlog::event::TypeCode::XidEvent => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        Ok(Event { header, event })
                    }
                    mysql_binlog::event::TypeCode::RotateEvent => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        Ok(Event { header, event })
                    }
                    mysql_binlog::event::TypeCode::QueryEvent => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        Ok(Event { header, event })
                    }
                    mysql_binlog::event::TypeCode::TableMapEvent => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        if let Some(ref e) = event {
                            match e {
                                mysql_binlog::event::EventData::TableMapEvent {
                                    table_id: d1,
                                    schema_name: d2,
                                    table_name: d3,
                                    columns: d4,
                                    ..
                                } => self
                                    .table_map
                                    .handle(*d1, d2.clone(), d3.clone(), d4.clone()),
                                _ => {
                                    println!("nop")
                                }
                            }
                        }
                        Ok(Event { header, event })
                    }
                    mysql_binlog::event::TypeCode::UpdateRowsEventV2
                    | mysql_binlog::event::TypeCode::WriteRowsEventV2
                    | mysql_binlog::event::TypeCode::DeleteRowsEventV2 => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        Ok(Event { header, event })
                    }
                    mysql_binlog::event::TypeCode::FormatDescriptionEvent => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len()],
                            Some(&self.table_map),
                        )?;
                        if let Some(FormatDescriptionEvent {
                            checksum_algorithm: ca,
                            ..
                        }) = event.as_ref()
                        {
                            match ca {
                                ChecksumAlgorithm::None => self.binlog_checksum_length = 0,
                                ChecksumAlgorithm::CRC32 => self.binlog_checksum_length = 4,
                                ChecksumAlgorithm::Other(size) => {
                                    self.binlog_checksum_length = *size as usize
                                }
                            }
                        }
                        Ok(Event { header, event })
                    }
                    _ => {
                        let event = mysql_binlog::event::EventData::from_data(
                            typ,
                            &data[EVENT_HEADER_SIZE + 1..data.len() - self.binlog_checksum_length],
                            Some(&self.table_map),
                        )?;
                        Ok(Event { header, event })
                    }
                }
            }
            None => Err(Box::from("invalid event header")),
            _ => Err(Box::from("parse event error")),
        }
    }
    fn handle_error_packet(&mut self, data: Vec<u8>) -> Result<(u16, String, String)> {
        let mut cursor = Cursor::new(data);
        cursor.set_position(2);
        let code = cursor.read_u16::<LittleEndian>()?;
        let _ = cursor.read_u8()?;
        let mut state = String::with_capacity(5);
        cursor.read_to_string(&mut state)?;
        let mut message = "".to_string();
        cursor.read_to_string(&mut message)?;
        Ok((code, state, message))
    }
    pub fn get_event(&mut self) -> Result<Event> {
        loop {
            match self.read_packet() {
                Ok(data) => match none!(data.get(0)) {
                    0 => return self.parse_event(data),
                    0xff => {
                        let (code, state, message) = self.handle_error_packet(data)?;
                        return Err(Box::from(format!("{}{}:{}", code, state, message)));
                    }
                    &_ => continue,
                },
                Err(e) => return Err(Box::from(e.to_string())),
            }
        }
    }
}

#[test]
fn test_conn_progress() {
    let mut runner = Runner::new("mysql://root:123456@127.0.0.1:3306", 1111).unwrap();
    runner.prepare().unwrap();
    runner
        .start_sync(OffsetConfig {
            pos: Some(("mysql-bin.000132".to_string(), 194)),
            gtid: None,
        })
        .unwrap();
    runner.get_event();
}
