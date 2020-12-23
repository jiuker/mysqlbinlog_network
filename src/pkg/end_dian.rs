use crate::pkg::err::Result;
use crate::none;
macro_rules! base_u {
    ($fun_name:ident,$fun_name_big:ident,$result_type:ty,$max:expr) => (
        pub fn $fun_name(b: &[u8]) -> $result_type {
            let mut rsl:$result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[index] as $result_type) << 8*index | rsl;
                index = index + 1;
            }
            rsl
        }
        pub fn $fun_name_big(b: &[u8]) -> $result_type {
            let mut rsl:$result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[($max-index)] as $result_type) << 8*index | rsl;
                index = index + 1;
            }
            rsl
        }
    );
}
base_u!(u8lit,u8big,u8,0);
base_u!(u16lit,u16big,u16,1);
base_u!(u32lit,u32big,u32,3);
base_u!(u64lit,u64big,u64,7);

macro_rules! base_put_u {
    ($fun_name:ident,$fun_name_big:ident,$type:ty,$add_max_count:expr) => (
        pub fn $fun_name(b: &mut Vec<u8>,pos:usize,v:$type)->Result<()>{
            let mut index = 0;
            while index < $add_max_count {
                *none!(b.get_mut(index+pos)) = (v << 8*index) as u8;
                index = index + 1;
            };
            Ok(())
        }
        pub fn $fun_name_big(b: &mut Vec<u8>,pos:usize,v:$type)->Result<()>  {
            let mut index = 0;
            while index < $add_max_count {
                *none!(b.get_mut($add_max_count-1-index+pos)) = (v << 8*index) as u8;
                index = index + 1;
            };
            Ok(())
        }
    );
}

base_put_u!(put_u8lit,put_u8big,u8,1);
base_put_u!(put_u16lit,put_u16big,u16,2);
base_put_u!(put_u32lit,put_u32big,u32,4);
base_put_u!(put_u64lit,put_u64big,u64,8);