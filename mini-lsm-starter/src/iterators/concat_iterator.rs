#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use log::debug;

use super::StorageIterator;
use crate::{
    key::{KeyBytes, KeySlice},
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    current_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let current = sstables
            .first()
            .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()))
            .transpose()?;
        let current_sst_idx = 0;
        Ok(Self {
            current,
            current_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        // todo: 二分查找来优化
        let key = {
            sstables.first().map_or(KeySlice::default(), |sst| {
                let first_key = sst.first_key();
                if key < first_key.as_key_slice() {
                    first_key.as_key_slice()
                } else {
                    key
                }
            })
        };
        let item = sstables.iter().enumerate().find(|(_, sst)| 
            sst.first_key().as_key_slice() <= key && key <= sst.last_key().as_key_slice()
        );
        let current = item
            .map(|(_, sst)| SsTableIterator::create_and_seek_to_key(sst.clone(), key))
            .transpose()?;
        let current_sst_idx = item.map(|(idx, _)| idx).unwrap_or(0);
        Ok(Self {
            current,
            current_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map_or(KeySlice::default(), |iter| iter.key())
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().map_or(&[], |iter| iter.value())
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().map_or(false, |iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let iter = match self.current.as_mut() {
            Some(iter) => iter,
            None => return Ok(()),
        };

        iter.next()?;
        if !iter.is_valid() {
            self.current_sst_idx += 1;
            if self.current_sst_idx < self.sstables.len() {
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.current_sst_idx].clone(),
                )?);
            } else {
                self.current = None;
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.sstables.len()
    }
}
