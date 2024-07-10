#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::RangeSsTableIterator;
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_metas: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        assert!(buf.len() <= u32::MAX as usize);
        assert!(block_metas.len() <= u16::MAX as usize);
        let offset = buf.len() as u32;
        buf.put_u16(block_metas.len() as u16);
        for block_meta in block_metas {
            assert!(buf.len() <= u32::MAX as usize);
            buf.put_u64(block_meta.offset as u64);
            buf.put_u16(block_meta.first_key.len() as u16);
            buf.put(block_meta.first_key.raw_ref());
            buf.put_u16(block_meta.last_key.len() as u16);
            buf.put(block_meta.last_key.raw_ref())
        }
        buf.put_u32(offset);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let block_meta_nums = buf.get_u16();
        let mut block_metas = Vec::with_capacity(block_meta_nums as usize);
        for i in 0..block_meta_nums {
            let offset = buf.get_u64();
            let first_key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(first_key_len as usize);
            let last_key_len = buf.get_u16();
            let last_key = buf.copy_to_bytes(last_key_len as usize);
            let block_meta = BlockMeta {
                offset: offset as usize,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            };
            block_metas.push(block_meta);
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
/// SSTable file:
/// -----------------------------------------------------------------------------------------------------
/// |         Block Section         |          Metadata Section                                         |
/// -----------------------------------------------------------------------------------------------------
/// | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
/// |                               | varlen   |        u32        |    varlen    |      u32            |
/// -----------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let data = file.read(0, file.1)?;

        let bloom_offset = (&data.as_slice()[data.len() - 4..]).get_u32() as usize;
        let bloom = Bloom::decode_bloom_meta(&data[bloom_offset..data.len() - 4]);

        let block_meta_offset = (&data.as_slice()[bloom_offset - 4..]).get_u32() as usize;
        let block_meta = BlockMeta::decode_block_meta(&data[block_meta_offset..]);

        let first_key = block_meta
            .first()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |meta| {
                meta.first_key.clone()
            });
        let last_key = block_meta
            .last()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |meta| {
                meta.last_key.clone()
            });
        Ok(SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0, // todo
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self
            .block_meta
            .get(block_idx)
            .ok_or_else(|| anyhow!("error idx read block"))?
            .offset;
        let next_offset = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |meta| meta.offset);
        let block = FileObject::read(&self.file, offset as u64, (next_offset - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    /// return range [0, block_meta.len()]
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // find the the first block whose first key that is greater than or equal to key
        let over_block_idx = self
            .block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() < key);
        // check previouse block range, maybe the key is in the previous block
        // like [1, 3], [3, 7], key = 2, it should be in the 0 block but over block is 1
        if over_block_idx > 0 {
            let prev_block_meta = self.block_meta.get(over_block_idx - 1).unwrap();
            if prev_block_meta.last_key.as_key_slice() >= key {
                over_block_idx - 1
            } else {
                over_block_idx
            }
        } else {
            over_block_idx
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
