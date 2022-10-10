// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    buffer_list::BufferList,
    concurrency_mgr::ConcurrencyMgr,
    lock_table::LockTableError,
    recovery_mgr::{RecoveryError, RecoveryMgr},
};
use crate::{
    buffer_mgr::{BufferError, BufferMgr},
    file_mgr::{FileMgr, FileMgrError},
    log_mgr::LogMgr,
    page::PageError,
    BlockId,
};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
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
    cm: ConcurrencyMgr,
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
        self.cm.xlock(blk)?;
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents();
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
        self.cm.xlock(blk)?;
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents();
        p.set_string(offset, val)?;
        buff.set_modified(self.txnum, lsn);
        Ok(())
    }
}

pub struct Transaction<'lm, 'bm> {
    inner: TxInner<'lm, 'bm>,
    fm: Arc<FileMgr>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
    rm: RecoveryMgr<'lm, 'bm>,
}

impl<'lm, 'bm> Transaction<'lm, 'bm> {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<LogMgr<'lm>>, bm: Arc<BufferMgr<'bm, 'lm>>) -> Self {
        let txnum = Transaction::next_txnum();
        let inner = TxInner {
            cm: ConcurrencyMgr::new(),
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

    fn next_txnum() -> i32 {
        static NEXT_TXNUM: AtomicI32 = AtomicI32::new(0);
        NEXT_TXNUM.fetch_add(1, Ordering::SeqCst)
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
        println!("transaction {} committed", self.inner.txnum);
        self.inner.cm.release();
        self.inner.bl.unpin_all();
        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.rm.rollback(&mut self.inner)?;
        println!("transaction {} rolled back", self.inner.txnum);
        self.inner.cm.release();
        self.inner.bl.unpin_all();
        Ok(())
    }

    pub fn recover(&mut self) -> Result<()> {
        self.bm.flush_all(self.inner.txnum)?;
        self.rm.recover(&mut self.inner)?;
        Ok(())
    }

    pub fn get_i32(&mut self, blk: &BlockId, offset: usize) -> Result<i32> {
        self.inner.cm.slock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let val = buff.contents().get_i32(offset)?;
        Ok(val)
    }

    pub fn get_string(&mut self, blk: &BlockId, offset: usize) -> Result<String> {
        self.inner.cm.slock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let val = buff.contents().get_string(offset)?;
        Ok(val)
    }

    pub fn set_i32(
        &mut self,
        blk: &BlockId,
        offset: usize,
        val: i32,
        ok_to_log: bool,
    ) -> Result<()> {
        self.inner.cm.xlock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_i32(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents();
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
        self.inner.cm.xlock(blk)?;
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_string(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents();
        p.set_string(offset, val)?;
        buff.set_modified(self.inner.txnum, lsn);
        Ok(())
    }

    pub fn available_buffs(&self) -> usize {
        self.bm.available()
    }

    pub fn size(&mut self, filename: &str) -> Result<u64> {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.slock(&dummyblk)?;
        let len = self.fm.length(filename)?;
        Ok(len)
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.xlock(&dummyblk)?;
        let block = self.fm.append(filename)?;
        Ok(block)
    }

    pub fn block_size(&self) -> usize {
        self.fm.blocksize()
    }
}
