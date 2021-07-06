//! Parser for the MySQL binary log format.
//!
//! # Limitations
//!
//! - Targets Percona and Oracle MySQL 5.6 and 5.7. Has not been tested with MariaDB, MySQL 8.0, or older versions of MySQL
//! - Like all 5.6/5.7 MySQL implementations, UNSIGNED BIGINT cannot safely represent numbers between `2^63` and `2^64` because `i64` is used internally for all integral data types
//!
//! # Example
//!
//! A simple command line event parser and printer
//!
//! ```no_run
//!  use mysqlbinlog_network::{Runner,OffsetConfig};
//!  let mut runner = Runner::new("mysql://root:123456@127.0.0.1:3306", 1111).unwrap();
//!     runner
//!         .start_sync(OffsetConfig {
//!             //! pos: Some(("binlog.000002".to_string(), 34834)),
//!             pos: None,
//!             gtid: Some("0575a804-6403-11ea-8d3d-e454e8d4a4fe:1-1469903".into()),
//!         })
//!         .unwrap();
//!     loop {
//!         let e = runner.get_event().unwrap();
//!         dbg!(e);
//!     }
//! ```
pub mod client;
pub mod mysql_binlog;
pub mod pkg;

pub use client::sync::OffsetConfig;
pub use client::sync::Runner;
pub use mysql::Value;
pub use mysql_binlog::EventIterator;
pub use pkg::event::Event;
pub use pkg::mysql_gtid::Gtid;
