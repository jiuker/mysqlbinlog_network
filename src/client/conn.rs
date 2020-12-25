use std::io::{BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::ops::Index;
use std::error::Error;
use sha1::Sha1;
use crate::client::pos::Pos;
use crate::pkg::vec::{get_vec, set_to_vec};
use crate::pkg::end_dian::{u16lit, u32lit, put_u32lit_4, put_u16lit_2, put_u8lit_1};
use crate::pkg::err::Result;
use crate::none;


pub struct BaseConn{
    br:BufReader<TcpStream>,
    bw:BufWriter<TcpStream>,
    sequence:u8,
}

impl BaseConn{
    pub fn new(conn:TcpStream)->Self{
        BaseConn{
            sequence: 0,
            br:BufReader::with_capacity(65536,conn.try_clone().unwrap()),
            bw:BufWriter::with_capacity(65536,conn.try_clone().unwrap()),
        }
    }
    pub fn read_packet(&mut self) ->Result<Vec<u8>>{
        let mut header =[0;4];
        self.br.read_exact(&mut header)?;
        let length = (header[0] as u32 | ((header[1] as u32)<<8)|((header[2] as u32)<<16)) as i32;
        let sequence = header[3];
        if sequence!=self.sequence{
            return Err(Box::from("sequence 不正确!"));
        }
        self.sequence = self.sequence.wrapping_add(1);
        let mut buf =vec![0; length as usize];
        self.br.read_exact(&mut buf)?;
        Ok(buf)
    }
    pub fn write_pack(&mut self, data: &mut Vec<u8>) ->Result<()>{
        let length = data.len()-4;
        // header
        *none!(data.get_mut(0)) = length as u8;
        *none!(data.get_mut(1)) = (length >> 8 as u8) as u8;
        *none!(data.get_mut(2)) = (length >> 16 as u8) as u8;
        *none!(data.get_mut(3)) = self.sequence as u8;
        self.bw.write(data.as_slice())?;
        self.sequence = self.sequence.wrapping_add(1);
        self.bw.flush();
        Ok(())
    }
    pub fn reset_sequence(&mut self,){
        self.sequence = 0;
    }
}
pub struct Conn{
    base_conn:BaseConn,
    addr:String,
    user:String,
    password:String,
    db:String,
    charset:String,
    connection_id:u32,
    salt:Vec<u8>,
    capability:u32,
    status:u16,
    auth_plugin_name:String,
    server_id:u32,
    port:u16,
}
const MIN_PROTOCOL_VERSION:u8  = 10;

const CLIENT_LONG_PASSWORD:u32 = 1<<0;
const CLIENT_FOUND_ROWS:u32 = 1<<1;
const CLIENT_LONG_FLAG:u32 = 1<<2;
const CLIENT_CONNECT_WITH_DB:u32 = 1<<3;
const CLIENT_NO_SCHEMA:u32 = 1<<4;
const CLIENT_COMPRESS:u32 = 1<<5;
const CLIENT_ODBC:u32 = 1<<6;
const CLIENT_LOCAL_FILES:u32 = 1<<7;
const CLIENT_IGNORE_SPACE:u32 = 1<<8;
const CLIENT_PROTOCOL_41:u32 = 1<<9;
const CLIENT_INTERACTIVE:u32 = 1<<10;
const CLIENT_SSL:u32 = 1<<11;
const CLIENT_IGNORE_SIGPIPE:u32 = 1<<12;
const CLIENT_TRANSACTIONS:u32 = 1<<13;
const CLIENT_RESERVED:u32 = 1<<14;
const CLIENT_SECURE_CONNECTION:u32 = 1<<15;
const CLIENT_MULTI_STATEMENTS:u32 = 1<<16;
const CLIENT_MULTI_RESULTS:u32 = 1<<17;
const CLIENT_PS_MULTI_RESULTS:u32 = 1<<18;
const CLIENT_PLUGIN_AUTH:u32 = 1<<19;
const CLIENT_CONNECT_ATTRS:u32 = 1<<20;
const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA:u32 = 1<<21;


const AUTH_MYSQL_OLD_PASSWORD:&str    = "mysql_old_password";
const AUTH_NATIVE_PASSWORD:&str       = "mysql_native_password";
const AUTH_CACHING_SHA2_PASSWORD:&str = "caching_sha2_password";
const AUTH_SHA256_PASSWORD:&str       = "sha256_password";

const DEFAULT_COLLATION_ID:u8 = 33;


const OK_HEADER:u8 = 0x00;
const MORE_DATE_HEADER:u8 = 0x01;
const ERR_HEADER:u8 = 0xff;
const EOF_HEADER:u8 = 0xfe;
const LOCAL_IN_FILE_HEADER:u8 = 0xfb;

const CACHE_SHA2_FAST_AUTH:u8 = 0x03;
const CACHE_SHA2_FULL_AUTH:u8 = 0x04;
const SemiSyncIndicator:u8 = 0xef;

impl Conn{
    pub fn new(addr:String,user:String,password:String,db:String)->Result<Self>{
        let tcp_conn = TcpStream::connect(addr.clone())?;
        let mut conn = Conn{
            base_conn:BaseConn::new(tcp_conn),
            addr,
            user,
            password,
            db,
            charset:"utf8".to_string(),
            connection_id:0,
            salt: vec![],
            capability: 0,
            status: 0,
            auth_plugin_name: "".to_string(),
            server_id: 789,
            port: 3306
        };
        conn.hand_shake()?;
        Ok(conn)
    }
    fn hand_shake(&mut self) ->Result<()>{
        self.read_init_hand_shake()?;
        self.write_auth_handshake()?;
        self.read_auth_result()?;
        Ok(())
    }

    fn read_init_hand_shake(&mut self) ->Result<()>{
        let data = self.base_conn.read_packet()?;
        if *none!(data.get(0)) == ERR_HEADER{
            return Err(Box::from("read initial handshake error"));
        }
        if *none!(data.get(0)) < MIN_PROTOCOL_VERSION {
            return Err(Box::from(format!("invalid protocol version {}, must >= 10",*none!(data.get(0)))));
        }
        // skip mysql version
        let mut pos = (1+data.index(0x00)+1) as usize;
        self.connection_id = u32lit(get_vec(&data, pos, 4)?.as_slice());
        pos+=4;
        self.salt.append(&mut get_vec(&data, pos, 8)?);
        pos += 8 + 1;
        self.capability = u16lit(get_vec(&data, pos, 2)?.as_slice()) as u32;
        if self.capability&CLIENT_PROTOCOL_41 ==0{
            return Err(Box::from("the MySQL server can not support protocol 41 and above required by the client"));
        }
        // todo 判断是不是ssl
        if self.capability&CLIENT_SSL == 0{

        }
        pos += 2;
        if data.len()> pos as usize {
            pos+=1;
            self.status = u16lit(get_vec(&data, pos, 2)?.as_slice());
            pos += 2;
            self.capability = (((u16lit(get_vec(&data, pos, 2)?.as_slice()) as u32) << 16 as u32)as u32| self.capability) as u32;
            pos += 2;
            pos += 10 + 1;
            self.salt.append(&mut get_vec(&data, pos, 12)?);
            pos += 13;
            let _data:Vec<u8> = get_vec(&data,pos,0)?;
            let end = _data.index(0x00);
            if *end> pos as u8 &&*end<= data.len() as u8 {
                self.auth_plugin_name = String::from_utf8(get_vec(&data, pos, *end as usize)?)?;
            }else{
                self.auth_plugin_name = String::from_utf8(get_vec(&data, pos, 0)?)?;
            }
        }
        if self.auth_plugin_name.is_empty(){
            self.auth_plugin_name = AUTH_NATIVE_PASSWORD.to_string();
        }
        Ok(())
    }
    fn write_auth_handshake(&mut self) ->Result<()>{
        let mut capability = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_LONG_PASSWORD | CLIENT_TRANSACTIONS | CLIENT_PLUGIN_AUTH | self.capability&CLIENT_LONG_FLAG;
        // todo tls
        let (auth, add_null) = self.gen_auth_response(self.salt.clone())?;
        let mut auth_resp_leibuf = vec![];
        let auth_resp_lei = append_length_encoded_integer(auth_resp_leibuf, auth.len() as u64);
        if auth_resp_lei.len()>1{
            capability |= CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA;
        }
        let mut length = 4 + 4 + 1 + 23 + self.user.len() + 1 + auth_resp_lei.len() + auth.len() + 21 + 1;
        if add_null {
            length+=1;
        }
// db name
        if !self.db.is_empty() {
            capability |= CLIENT_CONNECT_WITH_DB;
            length += self.db.len() + 1;
        }

        let mut data = vec![0u8;length+4];
        put_u32lit_4(&mut data,4,capability);

        put_u8lit_1(&mut data,8,0x00);
        put_u8lit_1(&mut data,9,0x00);
        put_u8lit_1(&mut data,10,0x00);
        put_u8lit_1(&mut data,11,0x00);

        put_u8lit_1(&mut data,12,DEFAULT_COLLATION_ID);


        // todo tls

        let mut pos = 13;
        for _ in pos..pos+23 {
            put_u8lit_1(&mut data,pos,0x00);
            pos+=1;
        }

        if !self.user.is_empty(){
            pos+=set_to_vec(&mut data,pos,self.user.as_bytes())?;
        }
        put_u8lit_1(&mut data,pos,0x00);
        pos+=1;


        pos+=set_to_vec(&mut data,pos,auth_resp_lei.as_slice())?;

        pos+=set_to_vec(&mut data,pos,auth.as_slice())?;



        if add_null {
            put_u8lit_1(&mut data,pos,0x00);
            pos+=1;
        }

        if !self.db.is_empty(){

            pos+=set_to_vec(&mut data,pos,self.db.as_bytes())?;

            put_u8lit_1(&mut data,pos,0x00);
            pos+=1;
        }

        pos+=set_to_vec(&mut data,pos,self.auth_plugin_name.as_bytes())?;
        put_u8lit_1(&mut data,pos,0x00);
        self.base_conn.write_pack(&mut data);
        Ok(())
    }
    fn gen_auth_response(&mut self, auth_data:Vec<u8>) ->Result<(Vec<u8>, bool)>{
        return match self.auth_plugin_name.as_str() {
            AUTH_NATIVE_PASSWORD => {
                Ok((calc_password(get_vec(&auth_data, 0, 20)?, self.password.as_bytes().to_vec()), false, ))
            },
            AUTH_CACHING_SHA2_PASSWORD => {
                Ok((calc_caching_sha2password(auth_data, self.password.as_bytes().to_vec()), false, ))
            },
            _ => {
                if self.password.is_empty() {
                    return Ok((vec![], true))
                }
                Ok((vec![1], true))
            }
        };
    }
    fn read_auth_result(&mut self) ->Result<(Vec<u8>,String)>{
        let data = self.base_conn.read_packet()?;
        match *none!(data.get(0)){
            OK_HEADER=>{
                return Ok((vec![],"".to_string()))
            }
            MORE_DATE_HEADER=>{
                return Ok((get_vec(&data, 1, 0)?,"".to_string()))
            }
            EOF_HEADER=>{

            }
            _ => {
                self.handle_error_packet(data)?;
            }
        };
        Ok((vec![],"".to_string()))
    }
    fn handle_error_packet(&mut self, data:Vec<u8>) ->Result<()>{
        let mut pos =1;
        let code = u16lit(get_vec(&data, pos, 0)?.as_slice());
        pos += 2;
        let mut state = "".to_string();
        if self.capability&CLIENT_PROTOCOL_41 > 0 {
            //skip '#'
            pos+=1;
            state = String::from_utf8(get_vec(&data, pos, 5)?)?;
            pos += 5;
        };
        let message = String::from_utf8(get_vec(&data, pos, 0)?)?;
        Err(Box::from(format!("[{}][{}]:{}", code,state, message)))
    }
    fn exec(&mut self,cmd:String)->Result<()>{
        let mut length = cmd.bytes().len() + 1;
        let mut data = vec![0; length + 4];
        // Query Type
        *none!(data.get_mut(4)) = 3;
        set_to_vec(&mut data,5,cmd.as_bytes())?;
        self.base_conn.write_pack(&mut data)?;
        Ok(())
    }
    pub fn execute(&mut self, cmd:String, ignore:i32) ->Result<()>{
        self.base_conn.reset_sequence();
        self.exec(cmd)?;
        let mut count = 0;
        while count < ignore {
            let rsl = self.base_conn.read_packet()?;
            count+=1;
            dbg!(count);
        }
        Ok(())
    }
    pub fn start_sync(&mut self, pos:Pos) ->Result<()>{
        self.prepare_sync_pos(pos);
        Ok(())
    }
    fn prepare_sync_pos(&mut self, mut pos:Pos) ->Result<()>{
        if pos.pos<4 {
            pos.pos = 4;
        };
        self.prepare();
        self.write_binlog_dump_command(pos)?;
        Ok(())
    }
    fn prepare(&mut self)->Result<()>{
        self.register_slave();
        Ok(())
    }
    fn register_slave(&mut self) ->Result<()>{
        self.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'".to_string(),6)?;
        self.execute("SET @master_binlog_checksum='NONE';".to_string(),1)?;
        self.write_register_slave_command()?;
        let rsl = self.base_conn.read_packet()?;
        dbg!(rsl);
        self.execute("SHOW VARIABLES LIKE 'rpl_semi_sync_master_enabled';".to_string(),5)?;
        self.execute("SET @rpl_semi_sync_slave = 1;".to_string(),1)?;
        Ok(())
    }
    fn write_binlog_dump_command(&mut self, pos_args:Pos) ->Result<()>{
        self.base_conn.reset_sequence();
        let mut data = vec![0; 4+1+4+2+4+pos_args.name.len()];
        let mut pos = 4;
        put_u8lit_1(&mut data,pos,18);
        pos+=1;

        put_u32lit_4(&mut data,pos,pos_args.pos);
        pos += 4;

        put_u16lit_2(&mut data,pos,0);
        pos += 2;

        put_u32lit_4(&mut data,pos,self.server_id);
        pos += 4;

        set_to_vec(&mut data,pos,pos_args.name.as_bytes())?;

        self.base_conn.write_pack(&mut data)
    }
    fn write_register_slave_command(&mut self) ->Result<()>{
        self.base_conn.reset_sequence();
        let h_name =  sys_info::hostname()?;
        let mut data = vec![0; 4+1+4+1+h_name.bytes().len()+1+self.user.len()+1+self.password.len()+2+4+4];
        let mut pos = 4;
        // slave

        put_u8lit_1(&mut data,pos,21)?;
        pos+=1;

        put_u32lit_4(&mut data,pos,self.server_id)?;
        pos += 4;

        put_u8lit_1(&mut data,pos,h_name.len() as u8)?;
        pos+=1;

        pos += set_to_vec(&mut data,pos,h_name.as_bytes())?;

        put_u8lit_1(&mut data,pos,self.user.len() as u8)?;
        pos+=1;

        pos += set_to_vec(&mut data,pos,self.user.as_bytes())?;


        put_u8lit_1(&mut data,pos,self.password.len() as u8)?;
        pos+=1;

        pos += set_to_vec(&mut data,pos,self.password.as_bytes())?;


        put_u16lit_2(&mut data,pos,self.port)?;
        pos += 2;
        put_u32lit_4(&mut data,pos,0)?;

        pos += 4;
        put_u32lit_4(&mut data,pos,0)?;
        self.base_conn.write_pack(&mut data)
    }
    pub fn get_event(&mut self) ->Result<()>{
        loop{
            let rsl = match self.base_conn.read_packet(){
                Ok(data)=>{
                    match *none!(data.get(0)) {
                        OK_HEADER=>{
                            // success
                            // skip success
                            let mut data = get_vec(&data,1,0)?;
                            let mut need_ack = false;
                            if *none!(data.get(0))==SemiSyncIndicator{
                                need_ack = *none!(data.get(1)) == 0x01;
                                data = get_vec(&data,2,0)?;
                            }
                            data
                        }
                        _ =>{
                            // error
                            vec![]
                        }
                    }
                },
                Err(e)=>{
                    continue;
                }
            };
            dbg!(rsl.len());
        };
    }
}
fn calc_password(scramble:Vec<u8>, password:Vec<u8>) ->Vec<u8>{
    if password.is_empty(){
        return vec![];
    }
    let mut crypt = Sha1::new();
    crypt.update(password.as_slice());
    let stage1 = crypt.digest().bytes();
    let stage_c = Vec::from(stage1);

    crypt.reset();
    crypt.update(stage_c.as_slice());
    let hash = crypt.digest().bytes();
    let hash_c = Vec::from(hash);

    crypt.reset();
    crypt.update(scramble.as_slice());
    crypt.update(hash_c.as_slice());
    let mut scramble = crypt.digest().bytes();

    let mut i = 0;
    for item in scramble.iter_mut() {
        *item ^= stage1.get(i).unwrap();
        i+=1;
    }
    scramble.to_vec()
}
fn calc_caching_sha2password(scramble:Vec<u8>, password:Vec<u8>) ->Vec<u8>{
    if password.is_empty(){
        return vec![];
    }
    let mut crypt = hmac_sha256::Hash::new();
    crypt.update(password.as_slice());
    let stage1 = crypt.finalize();
    let stage2 = Vec::from(stage1);

    let mut crypt = hmac_sha256::Hash::new();
    crypt.update(stage2);
    let hash = crypt.finalize();

    let mut crypt = hmac_sha256::Hash::new();
    crypt.update(hash);
    crypt.update(scramble.as_slice());
    let mut scramble = crypt.finalize();

    let mut i = 0;
    for item in scramble.iter_mut() {
        *item ^= stage1.get(i).unwrap();
        i+=1;
    }
    scramble.to_vec()
}
fn append_length_encoded_integer(mut b:Vec<u8>, n:u64) ->Vec<u8>{
    return match n {
        n if n <= 250 => {
            b.push(n as u8);
            b
        }
        n if n <= 0xffff => {
            b.push(0xfc);
            b.push(n as u8);
            b.push((n >> 8) as u8);
            b
        }
        n if n <= 0xffffff => {
            b.push(0xfc);
            b.push(n as u8);
            b.push((n >> 8) as u8);
            b.push((n >> 16) as u8);
            b
        }
        _ => {
            b.push(0xfc);
            b.push(n as u8);
            b.push((n >> 8) as u8);
            b.push((n >> 16) as u8);
            b.push((n >> 24) as u8);
            b.push((n >> 32) as u8);
            b.push((n >> 40) as u8);
            b.push((n >> 48) as u8);
            b.push((n >> 56) as u8);
            b
        }
    };
}