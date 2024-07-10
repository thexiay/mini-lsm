#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// In same time, return represent add success or not.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let current_size = self.offsets.len() * 2 + self.data.len() + 2;
        let to_be_full = current_size + key.len() + value.len() + 6 >= self.block_size;
        // only first or not full can be added
        if to_be_full && !self.is_empty() {
            return false;
        }

        assert!(key.len() <= u16::MAX as usize);
        assert!(value.len() <= u16::MAX as usize);
        assert!(self.data.len() <= u16::MAX as usize);
        self.offsets.push(self.data.len() as u16);
        let overlap_len = if self.is_empty() {
            self.first_key = key.to_key_vec();
            0
        } else {
            self.first_key.prefix_length(key.raw_ref()) as u16
        };
        self.data.put_u16(overlap_len);
        self.data.put_u16(key.len() as u16 - overlap_len);
        self.data.put(&key.raw_ref()[overlap_len as usize..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
