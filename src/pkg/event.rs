use mysql_binlog::event::EventData;

#[derive(Debug)]
pub struct Event {
    pub header: EventData,
    pub event: Option<EventData>,
}
