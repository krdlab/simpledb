// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::collections::HashMap;

use crate::BlockId;

use super::lock_table::{LockTable, Result};
use once_cell::sync::OnceCell;

#[derive(Debug, PartialEq, Eq)]
enum LockType {
    S,
    X,
}

pub struct ConcurrencyMgr {
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyMgr {
    fn lock_table() -> &'static LockTable {
        static LOCK_TABLE: OnceCell<LockTable> = OnceCell::new();
        LOCK_TABLE.get_or_init(|| LockTable::new())
    }

    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    pub fn slock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.locks.contains_key(blk) {
            ConcurrencyMgr::lock_table().slock(blk)?;
            self.locks.insert(blk.clone(), LockType::S);
        }
        Ok(())
    }

    pub fn xlock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.has_xlock(blk) {
            self.slock(blk)?;
            ConcurrencyMgr::lock_table().xlock(blk)?;
            self.locks.insert(blk.clone(), LockType::X);
        }
        Ok(())
    }

    fn has_xlock(&self, blk: &BlockId) -> bool {
        match self.locks.get(blk) {
            Some(locktype) => *locktype == LockType::X,
            None => false,
        }
    }

    pub fn release(&mut self) {
        for blk in self.locks.keys() {
            ConcurrencyMgr::lock_table().unlock(blk);
        }
        self.locks.clear();
    }
}
