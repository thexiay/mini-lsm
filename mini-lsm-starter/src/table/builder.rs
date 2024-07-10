#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{mem, path::Path};

use anyhow::Result;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeySlice},
    lsm_storage::BlockCache,
};

const DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.01;
const DEFAULT_BLOCK_SIZE: usize = 128;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Key<Vec<u8>>,
    last_key: Key<Vec<u8>>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl Default for SsTableBuilder {
    fn default() -> Self {
        SsTableBuilder::new(DEFAULT_BLOCK_SIZE)
    }
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Key::new(),
            last_key: Key::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            self.last_key.set_from_slice(key);
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // flush all data
        self.finish_block();

        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        Bloom::encode_bloom_meta(&self.key_hashes, 0.01, &mut self.data);
        let file = FileObject::create(path.as_ref(), self.data)?;
        SsTable::open(id, block_cache, file)
    }

    fn finish_block(&mut self) {
        let finished_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = finished_builder.build();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: mem::take(&mut self.first_key).into_key_bytes(),
            last_key: mem::take(&mut self.last_key).into_key_bytes(),
        });

        self.data.extend_from_slice(&block.encode());
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
