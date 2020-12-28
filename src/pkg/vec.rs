use crate::pkg::err::Result;
use crate::none;



pub fn set_to_vec(data:&mut Vec<u8>,pos:usize,p_data:&[u8])->Result<usize> {
    let mut i =0;
    for b in p_data.iter(){
        *none!(data.get_mut(pos+i)) = *b;
        i+=1;
    };
    Ok(p_data.len())
}