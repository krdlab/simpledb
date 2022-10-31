// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::lock_table::{LockTable, Result};
use crate::BlockId;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
enum LockType {
    S,
    X,
}

pub struct ConcurrencyMgr<'lt> {
    lock_table: &'lt LockTable,
    locks: HashMap<BlockId, LockType>,
}

impl<'lt> ConcurrencyMgr<'lt> {
    pub fn new(lock_table: &'lt LockTable) -> Self {
        Self {
            lock_table,
            locks: HashMap::new(),
        }
    }

    pub fn slock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.locks.contains_key(blk) {
            self.lock_table.slock(blk)?;
            self.locks.insert(blk.clone(), LockType::S);
        }
        Ok(())
    }

    pub fn xlock(&mut self, blk: &BlockId) -> Result<()> {
        if !self.has_xlock(blk) {
            self.slock(blk)?;
            self.lock_table.xlock(blk)?;
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
            self.lock_table.unlock(blk);
        }
        self.locks.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::{server::simple_db::SimpleDB, BlockId};
    use std::{path::Path, sync::Arc, thread, time::Duration};
    use tempfile::tempdir;

    struct Context<'lm, 'bm> {
        db: SimpleDB<'lm, 'bm>,
    }
    impl Context<'_, '_> {
        pub fn new(path: &Path) -> Self {
            let db = SimpleDB::new_for_test(path, "test_concurrency_mgr.log");
            Context { db }
        }
    }

    const FILE_NAME: &str = "test_concurrency_mgr_file";

    #[test]
    fn test_concurrency_mgr() {
        let dir = tempdir().unwrap();
        let ctx = Arc::new(Context::new(dir.path()));
        {
            let ctx1 = ctx.clone();
            let th1 = thread::spawn(move || {
                let mut tx = ctx1.db.new_tx();

                let block1 = BlockId::new(FILE_NAME, 1);
                let block2 = BlockId::new(FILE_NAME, 2);
                tx.pin(&block1).unwrap();
                tx.pin(&block2).unwrap();

                println!("tx1: request slock 1");
                tx.get_i32(&block1, 0).unwrap();
                println!("tx1: receive slock 1");

                thread::sleep(Duration::from_millis(1000));

                println!("tx1: request slock 2");
                tx.get_i32(&block2, 0).unwrap();
                println!("tx1: receive slock 2");

                tx.commit().unwrap();
                println!("tx1: commit");
            });

            let ctx2 = ctx.clone();
            let th2 = thread::spawn(move || {
                let mut tx = ctx2.db.new_tx();

                let block1 = BlockId::new(FILE_NAME, 1);
                let block2 = BlockId::new(FILE_NAME, 2);
                tx.pin(&block1).unwrap();
                tx.pin(&block2).unwrap();

                println!("tx2: request xlock 2");
                tx.set_i32(&block2, 0, 0, false).unwrap();
                println!("tx2: receive xlock 2");

                thread::sleep(Duration::from_millis(1000));

                println!("tx2: request slock 1");
                tx.get_i32(&block1, 0).unwrap();
                println!("tx2: receive slock 1");

                tx.commit().unwrap();
                println!("tx2: commit");
            });

            let ctx3 = ctx.clone();
            let th3 = thread::spawn(move || {
                let mut tx = ctx3.db.new_tx();

                let block1 = BlockId::new(FILE_NAME, 1);
                let block2 = BlockId::new(FILE_NAME, 2);
                tx.pin(&block1).unwrap();
                tx.pin(&block2).unwrap();

                thread::sleep(Duration::from_millis(500));
                println!("tx3: request xlock 1");
                tx.set_i32(&block1, 0, 0, false).unwrap();
                println!("tx3: receive xlock 1");

                thread::sleep(Duration::from_millis(1000));
                println!("tx3: request slock 2");
                tx.get_i32(&block2, 0).unwrap();
                println!("tx3: receive slock 2");

                tx.commit().unwrap();
                println!("tx3: commit");
            });

            th1.join().unwrap();
            th2.join().unwrap();
            th3.join().unwrap();
        }
        dir.close().unwrap();
    }
}
