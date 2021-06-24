use mysqlbinlog_network::client::conn::Conn;
use mysqlbinlog_network::client::pos::Pos;
use std::net::TcpStream;

fn main() {
    let mut conn = Conn::new(
        "127.0.0.1:3306".to_string(),
        "root".to_string(),
        "123456".to_string(),
        "dmall".to_string(),
    )
    .unwrap();
    conn.start_sync(&mut Pos {
        name: "mysql-bin.000126".to_string(),
        pos: 4,
    })
    .unwrap();
    conn.get_event().unwrap();
}
