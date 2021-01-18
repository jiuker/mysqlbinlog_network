use crate::event::event::FormatDescriptionEvent;
use std::collections::HashMap;
use crate::event::rows_event::TableMapEvent;

pub struct BinlogParser {
    flavor:String,
    format:FormatDescriptionEvent,
    tables:HashMap<u64,TableMapEvent>
}