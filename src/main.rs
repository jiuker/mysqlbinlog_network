use mysqlbinlog_network::client::conn::Conn;
use mysqlbinlog_network::client::pos::Pos;
use std::cell::RefCell;
use std::net::TcpStream;
use std::sync::Arc;
use std::thread::spawn;

fn main() {
    let mut connA = Conn::new(
        "127.0.0.1:3306".to_string(),
        "root".to_string(),
        "123456".to_string(),
        "dmall".to_string(),
    )
    .unwrap();
    connA
        .start_sync(&mut Pos {
            name: "mysql-bin.000126".to_string(),
            pos: 4,
        })
        .unwrap();
    connA.get_event().unwrap();
}
