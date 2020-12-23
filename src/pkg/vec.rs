use std::error::Error;

// 获取类似vec取index..start
pub fn get_vec(data:&Vec<u8>,start:usize,offset:usize)->Result<Vec<u8>,Box<dyn Error>> {
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