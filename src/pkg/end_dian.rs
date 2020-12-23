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