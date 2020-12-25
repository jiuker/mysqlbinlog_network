use crate::pkg::err::Result;
use crate::none;

// 获取类似vec取index..start
// offset 为 0 就是全部
pub fn get_vec(data:&Vec<u8>,start:usize,offset:usize)->Result<Vec<u8>> {
    let mut rsl = vec![];
    let mut find_offset = 0;
    if offset == 0{
        find_offset = data.len() - start -1
    }else{
        find_offset = offset
    }
    for index in start..start+find_offset{
        rsl.push(data.get(index).expect("超出index").clone().into());
    }
    Ok(rsl)
}
pub fn set_to_vec(data:&mut Vec<u8>,pos:usize,p_data:&[u8])->Result<usize> {
    let mut i =0;
    for b in p_data.iter(){
        *none!(data.get_mut(pos+i)) = *b;
        i+=1;
    };
    Ok(p_data.len())
}