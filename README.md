# mysqlbinglog_network

This create offers:

*  Get MySql5.7+/8.0+ Binlog-Event From Mysql Instance

Features:

*   macOS, Windows and Linux support;
*   Mysql Gtid Support;
*   mysql 5.7+/mysql 8.0+ Event Support;


### Installation

Put the desired version of the crate into the `dependencies` section of your `Cargo.toml`:

```toml
[dependencies]
mysqlbinglog_network = "*"
```

### Example

```rust
use mysqlbinlog_network::client::sync::{OffsetConfig, Runner};

fn main() {
    // Use crate[mysql] opt url to connect source
    // ServerID is unique,
    let mut runner = Runner::new("mysql://root:123456@127.0.0.1:3306", 1111).unwrap();
    runner
        .start_sync(OffsetConfig {
            // pos: Some(("binlog.000002".to_string(), 34834)),
            pos: None,
            gtid: Some("0575a804-6403-11ea-8d3d-e454e8d4a4fe:1-1467870".into()),
        })
        .unwrap();
    loop {
        let e = runner.get_event().unwrap();
        dbg!(e);
    }
}

```

### API Documentation

Please refer to the [crate docs].

## Event

Event|support
--|--:
Unknown|- []
StartEventV3|- [x]
QueryEvent|- [x]
StopEvent|- [x]
RotateEvent|- [x]
IntvarEvent|- []
LoadEvent|- []
SlaveEvent|- []
CreateFileEvent|- []
AppendBlockEvent|- []
ExecLoadEvent|- []
DeleteFileEvent|- []
NewLoadEvent|- []
RandEvent|- []
UserVarEvent|- []
FormatDescriptionEvent|- [x]
XidEvent|- [x]
BeginLoadQueryEvent|- [x]
ExecuteLoadQueryEvent|- [x]
TableMapEvent|- [x]
PreGaWriteRowsEvent|- []
PreGaUpdateRowsEvent|- []
PreGaDeleteRowsEvent|- []
WriteRowsEventV1|- [x]
UpdateRowsEventV1|- [x]
DeleteRowsEventV1|- [x]
IncidentEvent|- []
HeartbeatLogEvent|- [x]
IgnorableLogEvent|- []
RowsQueryLogEvent|- [x]
WriteRowsEventV2|- [x]
UpdateRowsEventV2|- [x]
DeleteRowsEventV2|- [x]
GtidLogEvent|- [x]
AnonymousGtidLogEvent|- []
PreviousGtidsLogEvent|- []
OtherUnknown(u8)|- []

Not support Means Event Will Not Parse!
## License

Licensed under either of

* Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or https://opensource.org/licenses/MIT)

## Thanks

* crate [mysql-20.1.0](https://crates.io/crates/mysql)
* crate [mysql_binlog-0.3.1](https://crates.io/crates/mysql_binlog)

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
