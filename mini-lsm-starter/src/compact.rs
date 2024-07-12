#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use log::info;
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::mem_table::TOMBSTONE;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = { self.state.read().as_ref().clone() };
        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(_) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut old_sst_ids = vec![];
                let l0_ssts = l0_sstables
                    .iter()
                    .filter_map(|sst_idx| snapshot.sstables.get(sst_idx))
                    .map(|sst| sst.clone())
                    .collect::<Vec<Arc<SsTable>>>();
                let l1_ssts = l1_sstables
                    .iter()
                    .filter_map(|sst_idx| snapshot.sstables.get(sst_idx))
                    .map(|sst| sst.clone())
                    .collect::<Vec<Arc<SsTable>>>();
                l0_ssts
                    .iter()
                    .for_each(|sst| old_sst_ids.push(sst.sst_id()));
                l1_ssts
                    .iter()
                    .for_each(|sst| old_sst_ids.push(sst.sst_id()));

                let l0_ssts_iters = l0_ssts
                    .iter()
                    .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()).map(Box::new))
                    .collect::<Result<Vec<_>>>()?;
                let l1_ssts_iters = l1_ssts
                    .iter()
                    .map(|sst| SsTableIterator::create_and_seek_to_first(sst.clone()).map(Box::new))
                    .collect::<Result<Vec<_>>>()?;
                let mut iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_ssts_iters),
                    MergeIterator::create(l1_ssts_iters),
                )?;

                // rewrite id
                let mut sst_builder = SsTableBuilder::default();
                while iter.is_valid() {
                    match iter.value() {
                        TOMBSTONE => (),
                        _ => sst_builder.add(iter.key(), iter.value()),
                    }
                    iter.next()?;
                }
                let sst_id = self.next_sst_id();
                let path = self.path_of_sst(sst_id);
                let sst = Arc::new(sst_builder.build(sst_id, None, path)?);

                // update state
                {
                    let _guard = self.state_lock.lock();
                    let mut state = self.state.write();
                    let mut snapshot = state.as_ref().clone();
                    snapshot
                        .l0_sstables
                        .retain(|sst_id| !old_sst_ids.contains(sst_id));

                    snapshot
                        .sstables
                        .retain(|sst_id, _| !old_sst_ids.contains(sst_id));
                    snapshot.sstables.insert(sst.sst_id(), sst.clone());

                    let l1_sstables = &mut snapshot.levels.get_mut(0).unwrap().1;
                    l1_sstables.retain(|sst_id| !old_sst_ids.contains(sst_id));
                    l1_sstables.push(sst.sst_id());

                    *state = Arc::new(snapshot);
                    info!("rewrite sst {:?} into sst {}", old_sst_ids, sst_id);
                }

                let mut old_ssts = vec![];
                old_ssts.extend_from_slice(&l0_ssts);
                old_ssts.extend_from_slice(&l1_ssts);
                Ok(old_ssts)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        const LEVEL_1: usize = 1;
        let (l0_sstables, l1_sstables) = {
            let guard = self.state.read();
            let snapshot = guard.as_ref().clone();
            let l1_ssts = snapshot
                .levels
                .get(LEVEL_1 - 1)
                .map_or(Vec::new(), |(_, level_ssts)| level_ssts.clone());
            (snapshot.l0_sstables.clone(), l1_ssts)
        };

        self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        })?;
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
