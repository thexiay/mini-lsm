#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;
use log::info;

use super::SsTable;
use crate::{
    block::BlockIterator,
    iterators::StorageIterator,
    key::{Key, KeySlice},
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: Option<BlockIterator>,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = SsTableIterator {
            table,
            blk_iter: None,
            blk_idx: 0,
        };
        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block(0)?;
        self.blk_idx = 0;
        self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn seek_to_key_inner(
        table: &Arc<SsTable>,
        key: KeySlice,
    ) -> Result<(usize, Option<BlockIterator>)> {
        let block_idx = table.find_block_idx(key);
        if block_idx >= table.num_of_blocks() {
            Ok((block_idx, None))
        } else {
            // find in block l
            let block = table.read_block(block_idx)?;
            let blk_iter = BlockIterator::create_and_seek_to_key(block, key);
            Ok((block_idx, Some(blk_iter)))
        }
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter
            .as_ref()
            .map_or(Key::default(), |iter| iter.key())
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.as_ref().map_or(&[], |iter| iter.value())
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        if let Some(iter) = &self.blk_iter {
            iter.is_valid()
        } else {
            false
        }
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.blk_iter {
            iter.next();
            info!("move to next sst record, is valid {}", iter.is_valid());
            if !iter.is_valid() {
                self.blk_idx += 1;
                if self.blk_idx < self.table.num_of_blocks() {
                    let block = self.table.read_block(self.blk_idx)?;
                    self.blk_iter = Some(BlockIterator::create_and_seek_to_first(block));
                } else {
                    self.blk_iter = None;
                }
            }
        }
        Ok(())
    }
}
