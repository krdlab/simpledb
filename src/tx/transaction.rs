// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::sync::atomic::{AtomicI32, Ordering};

use super::{buffer_list::BufferList, concurrency_mgr::ConcurrencyMgr, recovery_mgr::RecoveryMgr};
use crate::{buffer_mgr::BufferMgr, file_mgr::FileMgr, log_mgr::LogMgr, BlockId};
use std::sync::Arc;

const END_OF_FILE: i32 = -1;

pub(crate) struct TxInner<'lm, 'bm> {
    cm: ConcurrencyMgr,
    bl: BufferList<'bm, 'lm>,
    txnum: i32,
}
impl TxInner<'_, '_> {
    pub fn pin(&mut self, blk: &BlockId) {
        self.bl.pin(blk);
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        self.bl.unpin(blk);
    }

    pub fn set_i32_for_recovery(&mut self, blk: &BlockId, offset: usize, val: i32) {
        self.cm.xlock(blk);
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents();
        p.set_i32(offset, val);
        buff.set_modified(self.txnum, lsn);
    }

    pub fn set_string_for_recovery(&mut self, blk: &BlockId, offset: usize, val: &str) {
        self.cm.xlock(blk);
        let mut buff = self.bl.get_buffer(blk).unwrap().lock().unwrap();
        let lsn = -1;
        let p = buff.contents();
        p.set_string(offset, val);
        buff.set_modified(self.txnum, lsn);
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

    pub fn pin(&mut self, blk: &BlockId) {
        self.inner.bl.pin(blk);
    }

    pub fn unpin(&mut self, blk: &BlockId) {
        self.inner.bl.unpin(blk);
    }

    pub fn commit(&mut self) {
        self.rm.commit();
        println!("transaction {} committed", self.inner.txnum);
        self.inner.cm.release();
        self.inner.bl.unpin_all();
    }

    pub fn rollback(&mut self) {
        self.rm.rollback(&mut self.inner);
        println!("transaction {} rolled back", self.inner.txnum);
        self.inner.cm.release();
        self.inner.bl.unpin_all();
    }

    pub fn recover(&mut self) {
        self.bm.flush_all(self.inner.txnum);
        self.rm.recover(&mut self.inner);
    }

    pub fn get_i32(&mut self, blk: &BlockId, offset: usize) -> i32 {
        self.inner.cm.slock(blk);
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        buff.contents().get_i32(offset).unwrap()
    }

    pub fn get_string(&mut self, blk: &BlockId, offset: usize) -> String {
        self.inner.cm.slock(blk);
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        buff.contents().get_string(offset).unwrap()
    }

    pub fn set_i32(&mut self, blk: &BlockId, offset: usize, val: i32, ok_to_log: bool) {
        self.inner.cm.xlock(blk);
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_i32(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents();
        p.set_i32(offset, val);
        buff.set_modified(self.inner.txnum, lsn);
    }

    pub fn set_string(&mut self, blk: &BlockId, offset: usize, val: &str, ok_to_log: bool) {
        self.inner.cm.xlock(blk);
        let mut buff = self.inner.bl.get_buffer(blk).unwrap().lock().unwrap();
        let mut lsn = -1;
        if ok_to_log {
            lsn = self.rm.set_string(&mut *buff, offset, val).unwrap();
        }
        let p = buff.contents();
        p.set_string(offset, val);
        buff.set_modified(self.inner.txnum, lsn);
    }

    pub fn available_buffs(&self) -> i32 {
        self.available_buffs()
    }

    pub fn size(&mut self, filename: &str) -> u64 {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.slock(&dummyblk);
        self.fm.length(filename).unwrap()
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let dummyblk = BlockId::new(filename, END_OF_FILE.into());
        self.inner.cm.xlock(&dummyblk);
        self.fm.append(filename).unwrap()
    }

    pub fn block_size(&self) -> i32 {
        self.fm.blocksize().try_into().unwrap()
    }
}
