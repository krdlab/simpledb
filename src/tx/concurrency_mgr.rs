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

#[cfg(test)]
mod tests {
    use crate::{
        buffer_mgr::BufferMgr, file_mgr::FileMgr, log_mgr::LogMgr, server::simple_db::SimpleDB,
        tx::transaction::Transaction, BlockId,
    };
    use std::{path::Path, sync::Arc, thread, time::Duration};
    use tempfile::tempdir;

    struct Context<'lm, 'bm> {
        db: SimpleDB<'lm, 'bm>,
        fm: Arc<FileMgr>,
        lm: Arc<LogMgr<'lm>>,
        bm: Arc<BufferMgr<'bm, 'lm>>,
    }
    impl Context<'_, '_> {
        pub fn new(path: &Path) -> Self {
            let db = SimpleDB::new(path, 400, 8);
            let fm = db.file_mgr();
            let lm = db.log_mgr();
            let bm = db.buffer_mgr();
            Context { db, fm, lm, bm }
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
                let mut tx = Transaction::new(ctx1.fm.clone(), ctx1.lm.clone(), ctx1.bm.clone());

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
                let mut tx = Transaction::new(ctx2.fm.clone(), ctx2.lm.clone(), ctx2.bm.clone());

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
                let mut tx = Transaction::new(ctx3.fm.clone(), ctx3.lm.clone(), ctx3.bm.clone());

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
