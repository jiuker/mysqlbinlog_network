#[macro_export(none)]
macro_rules! none {
    ($exp:expr) => {
        match $exp{
            None => {
                return Err(Box::from("None Err"));
            }
            Some(d)=>{
                d
            }
        }
    };
}