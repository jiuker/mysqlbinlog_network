#[macro_export(none)]
macro_rules! none {
    ($exp:expr) => {
        match $exp {
            None => {
                return Err(Box::from("None Err"));
            }
            Some(d) => d,
        }
    };
}

#[macro_export(none_ref)]
macro_rules! none_ref {
    ($exp:expr) => {
        match $exp {
            None => {
                return Err(Box::from("None Err"));
            }
            Some(ref d) => d,
        }
    };
}
