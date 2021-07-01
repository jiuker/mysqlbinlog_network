use mysqlbinlog_network::client::sync::{OffsetConfig, Runner};

fn main() {
    let mut runner = Runner::new("mysql://root:123456@127.0.0.1:3306", 1111).unwrap();
    runner
        .start_sync(OffsetConfig {
            // pos: Some(("binlog.000002".to_string(), 34834)),
            pos: None,
            gtid: Some("0575a804-6403-11ea-8d3d-e454e8d4a4fe:1-1467870".into()),
        })
        .unwrap();
    runner.get_event().unwrap();
}
