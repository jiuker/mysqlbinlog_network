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
mysqlbinlog-network = {git="https://github.com/jiuker/mysqlbinlog_network"}
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
output:

[src/main.rs:14] e = Event {
        header: EventHeader {
        timestamp: 0,
        event_type: RotateEvent,
        server_id: 2,
        event_size: 43,
        log_pos: 0,
        flags: 32,
    },
    event: Some(
        RotateEvent {
            pos: 4,
            next_log_name: "mysql-bin.000132",
        },
    ),
}
[src/main.rs:14] e = Event {
        header: EventHeader {
        timestamp: 1625022967,
        event_type: FormatDescriptionEvent,
        server_id: 2,
        event_size: 119,
        log_pos: 123,
        flags: 0,
    },
    event: Some(
        FormatDescriptionEvent {
            binlog_version: 4,
            server_version: "5.7.29-log",
            create_timestamp: 0,
            common_header_len: 19,
            checksum_algorithm: CRC32,
        },
    ),
}
[src/main.rs:14] e = Event {
            header: EventHeader {
            timestamp: 1625022967,
            event_type: PreviousGtidsLogEvent,
            server_id: 2,
            event_size: 71,
            log_pos: 194,
            flags: 128,
        },
        event: None,
    }
[src/main.rs:14] e = Event {
        header: EventHeader {
            timestamp: 0,
            event_type: HeartbeatLogEvent,
            server_id: 2,
            event_size: 39,
            log_pos: 4178350,
            flags: 0,
        },
        event: None,
    }
[src/main.rs:14] e = Event {
        header: EventHeader {
        timestamp: 1625047157,
        event_type: GtidLogEvent,
        server_id: 2,
        event_size: 65,
        log_pos: 4178415,
        flags: 0,
    },
    event: Some(
            GtidLogEvent {
                flags: 0,
                uuid: 0575a804-6403-11ea-8d3d-e454e8d4a4fe,
                coordinate: 1467870,
                last_committed: Some(
                    4662,
                ),
                sequence_number: Some(
                    4663,
                ),
            },
        ),
    }
...

```

### API Documentation

Please refer to the [crate docs].

## Event

- [ ] Unknown
- [x] StartEventV3
- [x] QueryEvent
- [x] StopEvent
- [x] RotateEvent
- [ ] IntvarEvent
- [ ] LoadEvent
- [ ] SlaveEvent
- [ ] CreateFileEvent
- [ ] AppendBlockEvent
- [ ] ExecLoadEvent
- [ ] DeleteFileEvent
- [ ] NewLoadEvent
- [ ] RandEvent
- [ ] UserVarEvent
- [x] FormatDescriptionEvent
- [x] XidEvent
- [x] BeginLoadQueryEvent
- [x] ExecuteLoadQueryEvent
- [x] TableMapEvent
- [ ] PreGaWriteRowsEvent
- [ ] PreGaUpdateRowsEvent
- [ ] PreGaDeleteRowsEvent
- [x] WriteRowsEventV1
- [x] UpdateRowsEventV1
- [x] DeleteRowsEventV1
- [ ] IncidentEvent
- [x] HeartbeatLogEvent
- [ ] IgnorableLogEvent
- [x] RowsQueryLogEvent
- [x] WriteRowsEventV2
- [x] UpdateRowsEventV2
- [x] DeleteRowsEventV2
- [x] GtidLogEvent
- [ ] AnonymousGtidLogEvent
- [ ] PreviousGtidsLogEvent
- [x] OtherUnknown(u8)|

Not support Means Event Will Not Parse!But Event Header Will Can Tell It!
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
