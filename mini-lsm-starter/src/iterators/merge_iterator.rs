#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;
use log::debug;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // all to be merged iterators
    iters: BinaryHeap<HeapWrapper<I>>,
    // current iter which is current keyvalue
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.iter().all(|iter| !iter.is_valid()) {
            let mut iters = iters;
            let current = iters.pop().map(|iter| HeapWrapper(0, iter));
            return MergeIterator {
                iters: BinaryHeap::new(),
                current,
            };
        }

        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                let wrapper = HeapWrapper(i, iter);
                heap.push(wrapper);
            }
        }
        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map_or(KeySlice::default(), |current| current.1.key())
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map_or(&[], |current| current.1.value())
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or(false, |current| current.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if self.current.is_none() {
            return Ok(());
        }

        // evict value which key eq to current key
        let current = self.current.as_mut().unwrap();
        while let Some(mut next_iter) = self.iters.peek_mut() {
            if next_iter.1.key() == current.1.key() {
                debug!("evict from iter {} key {:?}", next_iter.0, current.1.key());
                if let e @ Err(_) = next_iter.1.next() {
                    PeekMut::pop(next_iter);
                    return e;
                }

                if !next_iter.1.is_valid() {
                    debug!("evict iter {} ", next_iter.0);
                    PeekMut::pop(next_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        // election next in iters and current, set next element iter as current
        if current.1.is_valid() {
            // should evict useless iterator
            if let Some(mut next_iter) = self.iters.peek_mut() {
                if &*next_iter > current {
                    debug!(
                        "swap iter {} key {:?} with iter {} key {:?}",
                        next_iter.0,
                        next_iter.1.key(),
                        current.0,
                        current.1.key()
                    );
                    std::mem::swap(current, &mut (*next_iter));
                }
            }
        } else {
            debug!("evict iter {} ", current.0);
            self.current = self.iters.pop();
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + self.current.as_ref().map_or(0, |_| 1)
    }
}
