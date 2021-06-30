use crate::none;
use crate::none_ref;
use byteorder::{LittleEndian, WriteBytesExt};
use mysql::consts::Command;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};
use mysql_binlog::event::EventData::{EventHeader, FormatDescriptionEvent};
use mysql_binlog::event::{ChecksumAlgorithm, EventData, EVENT_HEADER_SIZE};
use mysql_binlog::table_map::TableMap;
use std::error::Error;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::result;
use std::str::FromStr;

pub struct OffsetConfig<'a> {
    pub pos: Option<(String, u32)>,
    pub gtid: Option<Vec<(&'a str, Vec<(i64, i64)>)>>,
}
type Result<T> = result::Result<T, Box<dyn Error>>;
pub struct Runner {
    conn: Conn,
    opt: Opts,
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
    pub fn new(url: &str) -> Result<Self> {
        // mysql://us%20r:p%20w@localhost:3308
        let opt = mysql::Opts::from_url(url)?;
        let conn = mysql::Conn::new(opt.clone())?;
        Ok(Runner { conn, opt })
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
        data.write_u32::<LittleEndian>(789)?;
        data.write_u8(h_name.len() as u8)?;
        data.write_all(h_name.as_bytes())?;
        data.write_u8(none!(self.opt.get_user()).len() as u8)?;
        data.write_all(none!(self.opt.get_user()).as_bytes())?;
        data.write_u8(none!(self.opt.get_pass()).len() as u8)?;
        data.write_all(none!(self.opt.get_pass()).as_bytes())?;
        data.write_u16::<LittleEndian>(3306)?;
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
    pub fn write_dump_cmd(&mut self, offset: OffsetConfig) -> Result<()> {
        if offset.pos.is_some() {
            self.prepare()?;
            let mut data = vec![0u8; 0];
            data.write_u32::<LittleEndian>(none_ref!(offset.pos).1)?;
            data.write_u16::<LittleEndian>(0)?;
            data.write_u32::<LittleEndian>(789)?;
            data.write_all(none_ref!(offset.pos).0.as_bytes())?;
            self.write_command(Command::COM_BINLOG_DUMP, data.as_slice())?;
        } else if offset.gtid.is_some() {
            self.prepare()?;
            let mut data = vec![0u8; 0];
            data.write_u16::<LittleEndian>(0)?;
            data.write_u32::<LittleEndian>(789)?;
            data.write_u32::<LittleEndian>("".len() as u32)?;
            data.write_all("".as_bytes())?;
            data.write_u64::<LittleEndian>(4)?;
            let gtid = none_ref!(offset.gtid);
            let mut gtiddata = vec![0u8; 0];
            gtiddata.write_u64::<LittleEndian>(gtid.len() as u64)?;
            for i in 0..gtid.len() {
                let item = none!(gtid.get(i));
                let uid = uuid::Uuid::from_str(&item.0)?;
                gtiddata.write_all(uid.as_bytes())?;
                gtiddata.write_i64::<LittleEndian>(item.1.len() as i64)?;
                for ii in 0..item.1.len() {
                    let i_item = none!(item.1.get(ii));
                    gtiddata.write_i64::<LittleEndian>(i_item.0)?;
                    gtiddata.write_i64::<LittleEndian>(i_item.1)?;
                }
            }
            data.write_u32::<LittleEndian>(gtiddata.len() as u32)?;
            data.write_all(gtiddata.as_slice())?;
            self.write_command(Command::COM_BINLOG_DUMP_GTID, data.as_slice())?;
        } else {
            return Err(Box::from("show set gtid or fileName/offset"));
        }
        Ok(())
    }
    pub fn get_event(&mut self) -> Result<()> {
        let mut table_map = TableMap::new();
        let mut binlog_checksum_length = 0;
        loop {
            match self.read_packet() {
                Ok(data) => {
                    // parse Header
                    match mysql_binlog::event::EventData::parse_header(&data[1..]).unwrap() {
                        Some(EventHeader {
                            event_type: typ, ..
                        }) => match typ {
                            mysql_binlog::event::TypeCode::XidEvent => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1
                                        ..data.len() - binlog_checksum_length],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event);
                            }
                            mysql_binlog::event::TypeCode::RotateEvent => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1..data.len()],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event);
                            }
                            mysql_binlog::event::TypeCode::QueryEvent => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1
                                        ..data.len() - binlog_checksum_length],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event);
                            }
                            mysql_binlog::event::TypeCode::TableMapEvent => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1..],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event);
                                if let Some(e) = event {
                                    match e {
                                        mysql_binlog::event::EventData::TableMapEvent {
                                            table_id: d1,
                                            schema_name: d2,
                                            table_name: d3,
                                            columns: d4,
                                            ..
                                        } => table_map.handle(d1, d2, d3, d4),
                                        _ => {
                                            println!("nop")
                                        }
                                    }
                                }
                            }
                            mysql_binlog::event::TypeCode::UpdateRowsEventV2
                            | mysql_binlog::event::TypeCode::WriteRowsEventV2
                            | mysql_binlog::event::TypeCode::DeleteRowsEventV2 => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1
                                        ..data.len() - binlog_checksum_length],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event);
                            }
                            mysql_binlog::event::TypeCode::FormatDescriptionEvent => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1..data.len()],
                                    Some(&table_map),
                                )?;
                                if let Some(FormatDescriptionEvent {
                                    checksum_algorithm: ca,
                                    ..
                                }) = event
                                {
                                    match ca {
                                        ChecksumAlgorithm::None => binlog_checksum_length = 0,
                                        ChecksumAlgorithm::CRC32 => binlog_checksum_length = 4,
                                        ChecksumAlgorithm::Other(u8) => binlog_checksum_length = 8,
                                    }
                                }
                            }
                            _ => {
                                let event = mysql_binlog::event::EventData::from_data(
                                    typ,
                                    &data[EVENT_HEADER_SIZE + 1
                                        ..data.len() - binlog_checksum_length],
                                    Some(&table_map),
                                )?;
                                println!("end:        {:?}", event)
                            }
                        },
                        None => {}
                        _ => {
                            unimplemented!("不应该出现的地方!");
                        }
                    };

                    let typ = mysql_binlog::event::TypeCode::from_byte(*data.get(5).unwrap());
                    println!("before:    {:?}", typ);
                }
                Err(e) => return Err(Box::from(e.to_string())),
            };
        }
    }
}

#[test]
fn test_conn_progress() {
    let mut runner = Runner::new("mysql://root:123456@127.0.0.1:3306").unwrap();
    runner.register_slave().unwrap();
    runner.write_register_slave_command().unwrap();
    runner.enable_semi_sync().unwrap();
    runner
        .write_dump_cmd(OffsetConfig {
            pos: Some(("mysql-bin.000132".to_string(), 194)),
            gtid: None,
        })
        .unwrap();
    runner.get_event();
}
