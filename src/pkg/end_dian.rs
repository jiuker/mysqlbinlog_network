use crate::none;
use crate::pkg::err::Result;
macro_rules! base_u {
    ($fun_name:ident,$fun_name_big:ident,$result_type:ty,$max:expr) => {
        pub fn $fun_name(b: &[u8]) -> $result_type {
            let mut rsl: $result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[index] as $result_type) << 8 * index | rsl;
                index = index + 1;
            }
            rsl
        }
        pub fn $fun_name_big(b: &[u8]) -> $result_type {
            let mut rsl: $result_type = 0;
            let mut index = 0;
            while index <= $max {
                rsl = (b[($max - index)] as $result_type) << 8 * index | rsl;
                index = index + 1;
            }
            rsl
        }
    };
}
base_u!(u8lit, u8big, u8, 0);
base_u!(u16lit, u16big, u16, 1);
base_u!(u24lit, u24big, u32, 2);
base_u!(u32lit, u32big, u32, 3);
base_u!(u64lit, u64big, u64, 7);

macro_rules! base_put_u {
    ($fun_name:ident,$fun_name_big:ident,$type:ty,$add_max_count:expr) => {
        pub fn $fun_name(b: &mut Vec<u8>, pos: usize, v: $type) -> Result<()> {
            let mut index = 0;
            while index < $add_max_count {
                *none!(b.get_mut(index + pos)) = (v >> 8 * index) as u8;
                index = index + 1;
            }
            Ok(())
        }
        pub fn $fun_name_big(b: &mut Vec<u8>, pos: usize, v: $type) -> Result<()> {
            let mut index = 0;
            while index < $add_max_count {
                *none!(b.get_mut($add_max_count - 1 - index + pos)) = (v >> 8 * index) as u8;
                index = index + 1;
            }
            Ok(())
        }
    };
}

base_put_u!(put_u8lit_1, put_u8big_1, u8, 1);
base_put_u!(put_u16lit_2, put_u16big_2, u16, 2);
base_put_u!(put_u32lit_4, put_u32big_4, u32, 4);
base_put_u!(put_u64lit_8, put_u64big_8, u64, 8);
