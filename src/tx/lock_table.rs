// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{
    collections::HashMap,
    sync::{Condvar, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};
use thiserror::Error;

use crate::file::block_id::BlockId;

const MAX_TIME: u64 = 10000; // 10 sec

pub struct LockTable {
    locks: Mutex<HashMap<BlockId, i32>>,
    waiting: Condvar,
}

#[derive(Debug, Error)]
pub enum LockTableError {
    #[error("aborted to acquire the lock of {0}")]
    LockAborted(BlockId),
}

pub type Result<T> = core::result::Result<T, LockTableError>;

impl LockTable {
    pub fn new() -> Self {
        Self {
            locks: Mutex::new(HashMap::new()),
            waiting: Condvar::new(),
        }
    }

    pub fn slock(&self, blk: &BlockId) -> Result<()> {
        let mut locks = self.locks.lock().unwrap();

        let begintime = SystemTime::now();
        while self.has_xlock(&locks, blk) && !self.waiting_too_long(begintime) {
            let result = self
                .waiting
                .wait_timeout(locks, Duration::from_millis(MAX_TIME))
                .unwrap();
            if result.1.timed_out() {
                return Err(LockTableError::LockAborted(blk.clone()));
            } else {
                locks = result.0;
            }
        }

        if self.has_xlock(&locks, blk) {
            Err(LockTableError::LockAborted(blk.clone()))
        } else {
            let val = self.get_lock_val(&locks, blk);
            locks.insert(blk.clone(), val + 1);
            Ok(())
        }
    }

    pub fn xlock(&self, blk: &BlockId) -> Result<()> {
        let mut locks = self.locks.lock().unwrap();

        let begintime = SystemTime::now();
        while self.has_other_slocks(&locks, blk) && !self.waiting_too_long(begintime) {
            let result = self
                .waiting
                .wait_timeout(locks, Duration::from_millis(MAX_TIME))
                .unwrap();
            if result.1.timed_out() {
                return Err(LockTableError::LockAborted(blk.clone()));
            } else {
                locks = result.0;
            }
        }

        if self.has_other_slocks(&locks, blk) {
            Err(LockTableError::LockAborted(blk.clone()))
        } else {
            locks.insert(blk.clone(), -1);
            Ok(())
        }
    }

    pub fn unlock(&self, blk: &BlockId) {
        let mut locks = self.locks.lock().unwrap();
        let val = self.get_lock_val(&locks, blk);
        if val > 1 {
            locks.insert(blk.clone(), val - 1);
        } else {
            locks.remove(blk);
            self.waiting.notify_all();
        }
    }

    fn waiting_too_long(&self, begintime: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(begintime)
            .unwrap()
            .as_millis()
            > MAX_TIME.into()
    }

    fn has_xlock(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        self.get_lock_val(locks, blk) < 0
    }

    fn has_other_slocks(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> bool {
        self.get_lock_val(locks, blk) > 1
    }

    fn get_lock_val(&self, locks: &MutexGuard<HashMap<BlockId, i32>>, blk: &BlockId) -> i32 {
        match locks.get(blk) {
            Some(val) => *val,
            None => 0,
        }
    }
}
