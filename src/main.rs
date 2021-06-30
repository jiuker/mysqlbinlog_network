use mysqlbinlog_network::client::sync::{OffsetConfig, Runner};

fn main() {
    let mut runner = Runner::new("mysql://root:root@127.0.0.1:3307").unwrap();
    runner.register_slave().unwrap();
    runner.write_register_slave_command().unwrap();
    runner.enable_semi_sync().unwrap();
    runner
        .write_dump_cmd(OffsetConfig {
            pos: Some(("binlog.000002".to_string(), 34834)),
            gtid: None,
        })
        .unwrap();
    runner.get_event().unwrap();
}
