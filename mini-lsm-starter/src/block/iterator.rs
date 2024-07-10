#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp::Ordering, sync::Arc};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to_idx(0);
        block_iter.first_key = block_iter.key.clone();
        block_iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        // notice that seek key and constuct key may use first_key, so we must fill first key firstly
        let mut block_iter = Self::create_and_seek_to_first(block);
        block_iter.seek_to_key(key);
        block_iter
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        let key_offset = *self.block.offsets.get(idx).unwrap() as usize;
        let key_overlap_len = (&self.block.data[key_offset..key_offset + 2]).get_u16();
        let rest_key_len = (&self.block.data[key_offset + 2..key_offset + 4]).get_u16();
        let mut key = vec![];
        key.extend_from_slice(&self.first_key.raw_ref()[0..key_overlap_len as usize]);
        key.extend_from_slice(
            &self.block.data[key_offset + 4..key_offset + 4 + rest_key_len as usize],
        );
        self.key = KeyVec::from_vec(key);

        let value_offset = key_offset + 4 + rest_key_len as usize;
        let value_len = (&self.block.data[value_offset..value_offset + 2]).get_u16();
        self.value_range = (value_offset + 2, value_offset + 2 + value_len as usize);
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut r = self.block.offsets.len();
        while l < r {
            let mid = l + (r - l) / 2;
            if mid >= self.block.offsets.len() {
                // only happen when `l = r = block.offsets.len()`
                break;
            }
            // get mid key
            self.seek_to_idx(mid);
            match self.key().cmp(&key) {
                Ordering::Less => l = mid + 1,
                Ordering::Greater => r = mid,
                Ordering::Equal => r = mid,
            }
        }
        self.seek_to_idx(l);
    }
}
