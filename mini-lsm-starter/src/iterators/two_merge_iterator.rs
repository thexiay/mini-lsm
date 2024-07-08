#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    choose_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut iter = TwoMergeIterator {
            a,
            b,
            choose_a: true,
        };
        iter.choose();
        Ok(iter)
    }

    fn choose(&mut self) {
        self.choose_a = if !self.a.is_valid() {
            false
        } else if !self.b.is_valid() {
            true
        } else {
            self.a.key() <= self.b.key()
        };
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.choose_a {
            true => self.a.key(),
            false => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.choose_a {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.choose_a {
            true => self.a.is_valid(),
            false => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        // evict other iter value is equal to current, then go to next
        match self.choose_a {
            true => {
                if self.b.is_valid() && self.a.key() == self.b.key() {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            false => {
                if self.a.is_valid() && self.a.key() == self.b.key() {
                    self.a.next()?;
                }
                self.b.next()?;
            }
        }
        // choose smaller, prefer a
        self.choose();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
