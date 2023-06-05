// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    buffer_list::BufferList,
    concurrency_mgr::ConcurrencyMgr,
    lock_table::{LockTable, LockTableError},
    recovery_mgr::{RecoveryError, RecoveryMgr},
};
use crate::{
    buffer_mgr::{BufferError, BufferMgr},
    file::{
        block_id::BlockId,
        file_mgr::{FileMgr, FileMgrError},
        page::PageError,
    },
    log_mgr::LogMgr,
};
use std::sync::Arc;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicI32, Ordering},
};
use thiserror::Error;

const END_OF_FILE: i32 = -1;

#[derive(Debug, Error)]
pub enum TransactionError {
    #[error("{0:?}")]
    BufferError(#[from] BufferError),

    #[error("{0:?}")]
    LockTableError(#[from] LockTableError),

    #[error("{0:?}")]
    PageError(#[from] PageError),

    #[error("{0:?}")]
    RecoveryError(#[from] RecoveryError),

    #[error("{0:?}")]
    FileMgrError(#[from] FileMgrError),
}

pub type Result<T> = core::result::Result<T, TransactionError>;

pub(crate) struct TxInner<'lm, 'bm> {
    cm: RefCell<ConcurrencyMgr>,
    bl: BufferList<'bm, 'lm>,
    txnum: i32,
}
impl TxInner<'_, '_> {
    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        self.bl.pin(blk)?;
        Ok(())
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        self.bl.unpin(blk);
    }

    pub fn set_i32_for_recovery(&mut self, blk: &BlockId, offset: usize, val: i32) -> Result<()> {
        self.cm.borrow_mut().xlock(blk)?;
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents_as_mut();
        p.set_i32(offset, val)?;
        buff.set_modified(self.txnum, lsn);
        Ok(())
    }

    pub fn set_string_for_recovery(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: &str,
    ) -> Result<()> {
        self.cm.borrow_mut().xlock(blk)?;
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents_as_mut();
        p.set_string(offset, val)?;
        buff.set_modified(self.txnum, lsn);
        Ok(())
    }
}

pub struct TxNumber {
    next: AtomicI32,
}
impl TxNumber {
    pub fn new() -> Self {
        Self {
            next: AtomicI32::new(1),
        }
    }

    pub fn next(&self) -> i32 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct Transaction<'lm, 'bm> {
    inner: TxInner<'lm, 'bm>,
    fm: Arc<FileMgr>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
    rm: RecoveryMgr<'lm, 'bm>,
}

impl<'lm, 'bm> Transaction<'lm, 'bm> {
    pub fn new(
        txnum: i32,
        fm: Arc<FileMgr>,
        lm: Arc<LogMgr<'lm>>,
        bm: Arc<BufferMgr<'bm, 'lm>>,
        lock_table: Arc<LockTable>,
    ) -> Self {
        let inner = TxInner {
            cm: RefCell::new(ConcurrencyMgr::new(lock_table)),
            bl: BufferList::new(bm.clone()),
            txnum,
        };
        Self {
            inner,
            fm,
            bm: bm.clone(),
            rm: RecoveryMgr::new(txnum, lm, bm.clone()),
        }
    }

    pub fn txnum(&self) -> i32 {
        self.inner.txnum
    }

    pub fn pin(&mut self, blk: &BlockId) -> Result<()> {
        self.inner.bl.pin(blk)?;
        Ok(())
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        self.inner.bl.unpin(blk);
    }

    pub fn commit(&mut self) -> Result<()> {
        self.rm.commit()?;
        self.inner.cm.borrow_mut().release();
        self.inner.bl.unpin_all();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.rm.rollback(&mut self.inner)?;
        self.inner.cm.borrow_mut().release();
        self.inner.bl.unpin_all();
        Ok(())
    }

    pub fn recover(&mut self) -> Result<()> {
        self.bm.flush_all(self.inner.txnum)?;
        self.rm.recover(&mut self.inner)?;
        Ok(())
    }

    pub fn get_i32(&self, blk: &BlockId, offset: usize) -> Result<i32> {
        self.inner.cm.borrow_mut().slock(blk)?;
        let buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let val = buff.contents_as_ref().get_i32(offset)?;
        Ok(val)
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> Result<String> {
        self.inner.cm.borrow_mut().slock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let val = buff.contents_as_mut().get_string(offset)?;
        Ok(val)
    }

    pub fn set_i32(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: i32,
        ok_to_log: bool,
    ) -> Result<()> {
        self.inner.cm.borrow_mut().xlock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_i32(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents_as_mut();
        p.set_i32(offset, val)?;
        buff.set_modified(self.inner.txnum, lsn);
        Ok(())
    }

    pub fn set_string(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: &str,
        ok_to_log: bool,
    ) -> Result<()> {
        self.inner.cm.borrow_mut().xlock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_string(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents_as_mut();
        p.set_string(offset, val)?;
        buff.set_modified(self.inner.txnum, lsn);
        Ok(())
    }

    pub fn available_buffs(&self) -> usize {
        self.bm.available()
    }

    pub fn size(&self, filename: &str) -> Result<u64> {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.borrow_mut().slock(&dummyblk)?;
        let len = self.fm.length(filename)?;
        Ok(len)
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.borrow_mut().xlock(&dummyblk)?;
        let block = self.fm.append(filename)?;
        Ok(block)
    }

    pub fn block_size(&self) -> usize {
        self.fm.blocksize()
    }
}

#[cfg(test)]
mod tests {
    use crate::{file::block_id::BlockId, server::simple_db::SimpleDB};
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        let db = SimpleDB::new_for_test(dir.path(), "test_transaction.log");
        {
            let block = BlockId::new("test_transaction_file", 1);

            let tx1 = db.new_tx();
            tx1.borrow_mut().pin(&block).unwrap();
            tx1.borrow_mut().set_i32(&block, 80, 1, false).unwrap();
            tx1.borrow_mut()
                .set_string(&block, 40, "one", false)
                .unwrap();
            tx1.borrow_mut().commit().unwrap();

            let tx2 = db.new_tx();
            tx2.borrow_mut().pin(&block).unwrap();
            let ival = tx2.borrow().get_i32(&block, 80).unwrap();
            let sval = tx2.borrow().get_string(&block, 40).unwrap();
            assert_eq!(ival, 1);
            assert_eq!(sval, "one");
            let newival = ival + 1;
            let newsval = sval + "!";
            tx2.borrow_mut().set_i32(&block, 80, newival, true).unwrap();
            tx2.borrow_mut()
                .set_string(&block, 40, &newsval, true)
                .unwrap();
            tx2.borrow_mut().commit().unwrap();

            let tx3 = db.new_tx();
            tx3.borrow_mut().pin(&block).unwrap();
            assert_eq!(tx3.borrow().get_i32(&block, 80).unwrap(), newival);
            assert_eq!(tx3.borrow().get_string(&block, 40).unwrap(), newsval);
            tx3.borrow_mut().set_i32(&block, 80, 9999, true).unwrap();
            assert_eq!(tx3.borrow().get_i32(&block, 80).unwrap(), 9999);
            tx3.borrow_mut().rollback().unwrap();

            let tx4 = db.new_tx();
            tx4.borrow_mut().pin(&block).unwrap();
            assert_eq!(tx4.borrow().get_i32(&block, 80).unwrap(), newival);
            tx4.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
