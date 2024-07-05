#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
/// -----------------------------------------------------------------------
/// |                           Entry #1                            | ... |
/// -----------------------------------------------------------------------
/// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
/// -----------------------------------------------------------------------
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let len = self.data.len() + self.offsets.len() * 2 + 2;
        let mut block = BytesMut::with_capacity(len);
        block.put(self.data.as_slice());
        self.offsets
            .iter()
            .for_each(|offset| block.put_u16(*offset));
        block.put_u16(self.offsets.len() as u16);
        block.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_of_elements = u16::from_be_bytes([data[data_len - 2], data[data_len - 1]]);

        let mut offsets = vec![];
        for i in 0..num_of_elements {
            offsets.push(u16::from_be_bytes([
                data[data_len - 2 * ((num_of_elements - i + 1) as usize)],
                data[data_len - 2 * ((num_of_elements - i + 1) as usize) + 1],
            ]));
        }

        let mut datas = vec![];
        datas.extend_from_slice(&data[0..data_len - 2 - 2 * num_of_elements as usize]);
        Block {
            data: datas,
            offsets,
        }
    }
}
