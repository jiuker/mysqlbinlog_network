use crate::none;
use crate::none_ref;
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use mysql::consts::Command;
use mysql::prelude::Queryable;
use mysql::{Conn, Opts};
use std::error::Error;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::panic::resume_unwind;
use std::thread::sleep_ms;
use std::{io, result};

pub struct OffsetConfig {
    pub pos: Option<(String, u32)>,
    pub gtid: Option<String>,
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
        Ok(Runner { opt, conn })
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
        let mut data = vec![0u8; 4];
        // slave
        data.write_u8(21);
        data.write_u32::<LittleEndian>(789);
        data.write_u8(h_name.len() as u8);
        data.write_all(h_name.as_bytes());
        data.write_u8(none!(self.opt.get_user()).len() as u8);
        data.write_all(none!(self.opt.get_user()).as_bytes());
        data.write_u8(none!(self.opt.get_pass()).len() as u8);
        data.write_all(none!(self.opt.get_pass()).as_bytes());
        data.write_u16::<LittleEndian>(3306);
        data.write_u32::<LittleEndian>(0);
        data.write_u32::<LittleEndian>(0);
        self.write_packet(data)?;
        let rsl = self.read_packet()?;
        dbg!(rsl);
        Ok(())
    }
    fn enable_semi_sync(&mut self) -> Result<()> {
        let _: Vec<(String, String)> =
            self.query("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';")?;
        self.query_drop("SET @rpl_semi_sync_slave = 1;")?;
        Ok(())
    }
    pub fn write_dump_cmd(&mut self, offset: OffsetConfig) -> Result<()> {
        if !offset.pos.is_none() {
            let mut data = vec![0u8; 4];
            data.write_u8(18);
            data.write_u32::<LittleEndian>(offset.pos.clone().unwrap().1);
            data.write_u16::<LittleEndian>(0);
            data.write_u32::<LittleEndian>(789);
            data.write_all(offset.pos.unwrap().clone().0.as_bytes());
            self.write_packet(data)?;
        }
        Ok(())
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
    loop {
        let rsl = runner.read_packet().unwrap();
        dbg!(rsl);
        sleep_ms(1000)
    }
}
