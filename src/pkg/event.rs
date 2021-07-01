use mysql_binlog::event::EventData;

#[derive(Debug)]
pub struct Event {
    header: EventData,
    event: EventData,
}
