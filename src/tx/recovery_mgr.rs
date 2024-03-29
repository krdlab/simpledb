// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::transaction::TxInner;
use crate::buffer_mgr::{Buffer, BufferError, BufferMgr};
use crate::file::block_id::BlockId;
use crate::file::page::{self, Page, PageError};
use crate::log_mgr::{self, LogMgrError, LSN};
use crate::{constants::I32_BYTE_SIZE, log_mgr::LogMgr};
use std::sync::Arc;
use std::{convert::TryInto, fmt::Display};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("{0:?}")]
    IO(#[from] std::io::Error),

    #[error("{0:?}")]
    PageError(#[from] PageError),

    #[error("{0:?}")]
    LogMgrError(#[from] LogMgrError),

    #[error("{0:?}")]
    BufferMgrError(#[from] BufferError),

    #[error("failed to undo: {0:?}")]
    UndoError(String),

    #[error("unknown op: {0:?}")]
    UnknownOp(i32),
}

pub type Result<T> = core::result::Result<T, RecoveryError>;

#[derive(Debug, PartialEq, Eq)]
enum Op {
    Checkpoint = 0,
    Start,
    Commit,
    Rollback,
    SetInt,
    SetString,
}

impl Op {
    fn from_i32(i: i32) -> Option<Op> {
        match i {
            0 => Some(Op::Checkpoint),
            1 => Some(Op::Start),
            2 => Some(Op::Commit),
            3 => Some(Op::Rollback),
            4 => Some(Op::SetInt),
            5 => Some(Op::SetString),
            _ => None,
        }
    }
    fn to_i32(&self) -> i32 {
        match self {
            Op::Checkpoint => 0,
            Op::Start => 1,
            Op::Commit => 2,
            Op::Rollback => 3,
            Op::SetInt => 4,
            Op::SetString => 5,
        }
    }
}

trait LogRecord: Display {
    fn op(&self) -> Op;
    fn tx_number(&self) -> i32;
    fn undo<'t>(&self, tx: &'t mut TxInner) -> Result<()>;
}

fn create_log_record(mut bytes: Vec<u8>) -> Result<Box<dyn LogRecord>> {
    let mut p = Page::for_log(&mut bytes);
    let op = p.get_i32(0).unwrap();
    match Op::from_i32(op) {
        Some(Op::Checkpoint) => Ok(Box::new(CheckpointRecord {})),
        Some(Op::Start) => Ok(Box::new(StartRecord::new(&p)?)),
        Some(Op::Commit) => Ok(Box::new(CommitRecord::new(&p)?)),
        Some(Op::Rollback) => Ok(Box::new(RollbackRecord::new(&p)?)),
        Some(Op::SetInt) => Ok(Box::new(SetIntRecord::new(&mut p)?)),
        Some(Op::SetString) => Ok(Box::new(SetStringRecord::new(&mut p)?)),
        _ => Err(RecoveryError::UnknownOp(op)),
    }
}

struct CheckpointRecord {}
impl CheckpointRecord {
    pub fn write_to_log(lm: Arc<LogMgr>) -> log_mgr::Result<i64> {
        let i32_bytes: usize = I32_BYTE_SIZE.try_into().unwrap();
        let mut rec = vec![0u8; i32_bytes];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::Checkpoint.to_i32())?;
        }
        lm.apppend(&rec)
    }
}
impl LogRecord for CheckpointRecord {
    fn op(&self) -> Op {
        Op::Checkpoint
    }

    fn tx_number(&self) -> i32 {
        -1 // dummy value
    }

    fn undo<'t>(&self, _tx: &'t mut TxInner) -> Result<()> {
        Ok(())
    }
}
impl Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

struct StartRecord {
    txnum: i32,
}
impl StartRecord {
    pub fn new(p: &Page) -> page::Result<Self> {
        let tpos = I32_BYTE_SIZE.try_into().unwrap();
        let txnum = p.get_i32(tpos)?;
        Ok(Self { txnum })
    }

    pub fn write_to_log(lm: Arc<LogMgr>, txnum: i32) -> log_mgr::Result<i64> {
        let i32_bytes: usize = I32_BYTE_SIZE.try_into().unwrap();
        let mut rec = vec![0u8; i32_bytes * 2];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::Start.to_i32())?;
            p.set_i32(i32_bytes, txnum)?;
        }
        lm.apppend(&rec)
    }
}
impl LogRecord for StartRecord {
    fn op(&self) -> Op {
        Op::Start
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo<'t>(&self, _tx: &'t mut TxInner) -> Result<()> {
        Ok(())
    }
}
impl Display for StartRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<START {}>", self.txnum)
    }
}

struct CommitRecord {
    txnum: i32,
}
impl CommitRecord {
    pub fn new(p: &Page) -> page::Result<Self> {
        let tpos = I32_BYTE_SIZE.try_into().unwrap();
        let txnum = p.get_i32(tpos)?;
        Ok(Self { txnum })
    }

    pub fn write_to_log(lm: Arc<LogMgr>, txnum: i32) -> log_mgr::Result<i64> {
        let i32_bytes: usize = I32_BYTE_SIZE.try_into().unwrap();
        let mut rec = vec![0u8; 2 * i32_bytes];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::Commit.to_i32())?;
            p.set_i32(i32_bytes, txnum)?;
        }
        lm.apppend(&rec)
    }
}
impl LogRecord for CommitRecord {
    fn op(&self) -> Op {
        Op::Commit
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo<'t>(&self, _tx: &'t mut TxInner) -> Result<()> {
        Ok(())
    }
}
impl Display for CommitRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<COMMIT {}>", self.txnum)
    }
}

struct RollbackRecord {
    txnum: i32,
}
impl RollbackRecord {
    pub fn new(p: &Page) -> page::Result<Self> {
        let tpos = I32_BYTE_SIZE.try_into().unwrap();
        let txnum = p.get_i32(tpos)?;
        Ok(Self { txnum })
    }

    pub fn write_to_log(lm: Arc<LogMgr>, txnum: i32) -> log_mgr::Result<i64> {
        let i32_bytes = I32_BYTE_SIZE.try_into().unwrap();
        let mut rec = vec![0u8; 2 * i32_bytes];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::Rollback.to_i32())?;
            p.set_i32(i32_bytes, txnum)?;
        }
        lm.apppend(&rec)
    }
}
impl LogRecord for RollbackRecord {
    fn op(&self) -> Op {
        Op::Rollback
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo<'t>(&self, _tx: &'t mut TxInner) -> Result<()> {
        Ok(())
    }
}
impl Display for RollbackRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.txnum)
    }
}

struct SetIntRecord {
    txnum: i32,
    offset: usize,
    block: BlockId,
    value: i32,
}
impl SetIntRecord {
    pub fn new(p: &mut Page) -> page::Result<Self> {
        let i32_bytes = I32_BYTE_SIZE.try_into().unwrap();

        let tpos = i32_bytes;
        let txnum = p.get_i32(tpos)?;
        let fpos = tpos + i32_bytes;
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_i32(bpos)?.try_into().unwrap();
        let block = BlockId::new(&filename, blknum);
        let opos = bpos + i32_bytes;
        let offset = p.get_i32(opos)?.try_into().unwrap();
        let vpos = opos + i32_bytes;
        let value = p.get_i32(vpos)?;

        Ok(Self {
            txnum,
            offset,
            block,
            value,
        })
    }

    pub fn write_to_log(
        lm: Arc<LogMgr>,
        txnum: i32,
        blk: &BlockId,
        offset: usize,
        value: i32,
    ) -> log_mgr::Result<i64> {
        let i32_bytes = I32_BYTE_SIZE.try_into().unwrap();
        let tpos = i32_bytes;
        let fpos = tpos + i32_bytes;
        let bpos = fpos + Page::max_length(blk.filename().len());
        let opos = bpos + i32_bytes;
        let vpos = opos + i32_bytes;

        let mut rec = vec![0u8; vpos + i32_bytes];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::SetInt.to_i32())?;
            p.set_i32(tpos, txnum)?;
            p.set_string(fpos, blk.filename())?;
            p.set_i32(bpos, blk.number().try_into().unwrap())?;
            p.set_i32(opos, offset.try_into().unwrap())?;
            p.set_i32(vpos, value)?;
        }
        lm.apppend(&rec)
    }
}
impl LogRecord for SetIntRecord {
    fn op(&self) -> Op {
        Op::SetInt
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo<'t>(&self, tx: &'t mut TxInner) -> Result<()> {
        if let Err(e) = tx.pin(&self.block) {
            return Err(RecoveryError::UndoError(e.to_string()));
        }
        if let Err(e) = tx.set_i32_for_recovery(&self.block, self.offset, self.value) {
            return Err(RecoveryError::UndoError(e.to_string()));
        }
        tx.unpin(&self.block);
        Ok(())
    }
}
impl Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.txnum, self.block, self.offset, self.value
        )
    }
}

struct SetStringRecord {
    txnum: i32,
    offset: usize,
    block: BlockId,
    value: String,
}

impl SetStringRecord {
    // ! FIXME
    pub fn new<'p>(p: &'p mut Page) -> page::Result<Self> {
        let i32_bytes: usize = I32_BYTE_SIZE.try_into().unwrap();

        let tpos = i32_bytes;
        let txnum = p.get_i32(tpos)?;

        let fpos = tpos + i32_bytes;
        let filename = p.get_string(fpos)?;

        let bpos = fpos + Page::max_length(filename.len());
        let blknum = p.get_i32(bpos)?;
        let block = BlockId::new(&filename, blknum.try_into().unwrap());

        let opos = bpos + i32_bytes;
        let offset: usize = p.get_i32(opos)?.try_into().unwrap();

        let vpos = opos + i32_bytes;
        let value = p.get_string(vpos)?;

        Ok(Self {
            txnum,
            offset,
            block,
            value,
        })
    }

    pub fn write_to_log(
        lm: Arc<LogMgr>,
        txnum: i32,
        blk: &BlockId,
        offset: usize,
        val: String,
    ) -> log_mgr::Result<i64> {
        let i32_bytes: usize = I32_BYTE_SIZE.try_into().unwrap();
        let tpos = i32_bytes;
        let fpos = tpos + i32_bytes;
        let bpos = fpos + Page::max_length(blk.filename().len());
        let opos = bpos + i32_bytes;
        let vpos = opos + i32_bytes;
        let reclen = vpos + Page::max_length(val.len());

        let mut rec = vec![0u8; reclen];
        {
            let mut p = Page::for_log(&mut rec);
            p.set_i32(0, Op::SetString.to_i32())?;
            p.set_i32(tpos, txnum)?;
            p.set_string(fpos, blk.filename())?;
            p.set_i32(bpos, blk.number().try_into().unwrap())?;
            p.set_i32(opos, offset.try_into().unwrap())?;
            p.set_string(vpos, val.as_str())?;
        }
        lm.apppend(&rec)
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> Op {
        Op::SetString
    }

    fn tx_number(&self) -> i32 {
        self.txnum
    }

    fn undo<'t>(&self, tx: &'t mut TxInner) -> Result<()> {
        if let Err(e) = tx.pin(&self.block) {
            return Err(RecoveryError::UndoError(e.to_string()));
        }
        if let Err(e) = tx.set_string_for_recovery(&self.block, self.offset, &self.value) {
            return Err(RecoveryError::UndoError(e.to_string()));
        }
        tx.unpin(&self.block);
        Ok(())
    }
}

impl Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.txnum, self.block, self.offset, self.value
        )
    }
}

pub struct RecoveryMgr<'lm, 'bm> {
    lm: Arc<LogMgr<'lm>>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
    txnum: i32,
}

impl<'lm, 'bm> RecoveryMgr<'lm, 'bm> {
    pub fn new(txnum: i32, lm: Arc<LogMgr<'lm>>, bm: Arc<BufferMgr<'bm, 'lm>>) -> Self {
        StartRecord::write_to_log(lm.clone(), txnum).unwrap();
        Self { lm, bm, txnum }
    }

    pub fn commit(&mut self) -> Result<()> {
        self.bm.flush_all(self.txnum)?;
        let lsn = CommitRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm.flush(lsn)?;
        Ok(())
    }

    pub(crate) fn rollback<'tx, 'lt>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        self.do_rollback(tx)?;
        self.bm.flush_all(self.txnum)?;
        let lsn = RollbackRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm.flush(lsn)?;
        Ok(())
    }

    pub(crate) fn recover<'tx, 'lt>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        self.do_recover(tx)?;
        self.bm.flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(self.lm.clone())?;
        self.lm.flush(lsn)?;
        Ok(())
    }

    pub fn set_i32(&mut self, buff: &mut Buffer, offset: usize, _newval: i32) -> Result<LSN> {
        let oldval = buff.contents_as_mut().get_i32(offset)?;
        let blk = buff.block().as_ref().unwrap();
        let lsn = SetIntRecord::write_to_log(self.lm.clone(), self.txnum, blk, offset, oldval)?;
        Ok(lsn)
    }

    pub fn set_string(&self, buff: &mut Buffer, offset: usize, _newval: &str) -> Result<LSN> {
        let oldval = buff.contents_as_mut().get_string(offset)?;
        let blk = buff.block().as_ref().unwrap();
        let lsn = SetStringRecord::write_to_log(self.lm.clone(), self.txnum, blk, offset, oldval)?;
        Ok(lsn)
    }

    fn do_rollback<'tx, 'lt>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        let mut iter = self.lm.reverse_iter()?;
        while iter.has_next() {
            let bytes = iter.next().unwrap();
            let rec = create_log_record(bytes)?;
            if rec.tx_number() == self.txnum {
                if rec.op() == Op::Start {
                    break;
                }
                rec.undo(tx)?;
            }
        }
        Ok(())
    }

    fn do_recover<'tx, 'lt>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        let mut finished_txs: Vec<i32> = Vec::new();
        let mut iter = self.lm.reverse_iter()?;
        while iter.has_next() {
            let bytes = iter.next().unwrap();
            let rec = create_log_record(bytes)?;
            if rec.op() == Op::Checkpoint {
                break;
            }
            if rec.op() == Op::Commit || rec.op() == Op::Rollback {
                finished_txs.push(rec.tx_number());
            } else if !finished_txs.contains(&rec.tx_number()) {
                rec.undo(tx)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{file::file_mgr::FileMgr, server::simple_db::SimpleDB};
    use std::path::Path;
    use tempfile::tempdir;

    struct Context<'lm, 'bm> {
        db: SimpleDB<'lm, 'bm>,
        fm: Arc<FileMgr>,
        bm: Arc<BufferMgr<'bm, 'lm>>,
        block0: BlockId,
        block1: BlockId,
    }
    impl Context<'_, '_> {
        pub fn new(dir: &Path) -> Self {
            let db = SimpleDB::new_for_test(dir, "test_recovery_mgr.log");
            let fm = db.file_mgr();
            let bm = db.buffer_mgr();
            Context {
                db,
                fm: fm.clone(),
                bm,
                block0: BlockId::new("test_recovery_mgr_file", 0),
                block1: BlockId::new("test_recovery_mgr_file", 1),
            }
        }
    }

    #[test]
    fn test_recovery_mgr() {
        let dir = tempdir().unwrap();
        {
            let mut ctx = Context::new(dir.path());
            test_initialize(&mut ctx);
            test_modify(&ctx);
        }
        {
            let mut ctx = Context::new(dir.path());
            test_recover(&mut ctx);
        }
        dir.close().unwrap();
    }

    fn test_initialize(ctx: &mut Context) {
        let tx1 = ctx.db.new_tx();
        let tx2 = ctx.db.new_tx();
        tx1.borrow_mut().pin(&ctx.block0).unwrap();
        tx2.borrow_mut().pin(&ctx.block1).unwrap();

        let mut pos = 0;
        for _i in 0..6 {
            tx1.borrow_mut()
                .set_i32(&ctx.block0, pos, pos as i32, false)
                .unwrap();
            tx2.borrow_mut()
                .set_i32(&ctx.block1, pos, pos as i32, false)
                .unwrap();
            pos += I32_BYTE_SIZE as usize;
        }

        tx1.borrow_mut()
            .set_string(&ctx.block0, 30, "abc", false)
            .unwrap();
        tx2.borrow_mut()
            .set_string(&ctx.block1, 30, "def", false)
            .unwrap();
        tx1.borrow_mut().commit().unwrap();
        tx2.borrow_mut().commit().unwrap();

        assert_fm_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );
    }

    fn test_modify(ctx: &Context) {
        let tx3 = ctx.db.new_tx();
        let tx4 = ctx.db.new_tx();

        tx3.borrow_mut().pin(&ctx.block0).unwrap();
        tx4.borrow_mut().pin(&ctx.block1).unwrap();

        let mut pos = 0;
        for _i in 0..6 {
            tx3.borrow_mut()
                .set_i32(&ctx.block0, pos, pos as i32 + 100, true)
                .unwrap();
            tx4.borrow_mut()
                .set_i32(&ctx.block1, pos, pos as i32 + 100, true)
                .unwrap();
            pos += I32_BYTE_SIZE as usize;
        }
        tx3.borrow_mut()
            .set_string(&ctx.block0, 30, "uvw", true)
            .unwrap();
        tx4.borrow_mut()
            .set_string(&ctx.block1, 30, "xyz", true)
            .unwrap();
        ctx.bm.flush_all(tx3.borrow().txnum()).unwrap();
        ctx.bm.flush_all(tx4.borrow().txnum()).unwrap();
        assert_fm_values(
            &ctx,
            [
                [100, 104, 108, 112, 116, 120],
                [100, 104, 108, 112, 116, 120],
            ],
            ["uvw", "xyz"],
        );

        tx3.borrow_mut().rollback().unwrap();
        assert_fm_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [100, 104, 108, 112, 116, 120]],
            ["abc", "xyz"],
        );
    }

    fn test_recover(ctx: &mut Context) {
        let tx = ctx.db.new_tx();
        print_fm_values(ctx, &ctx.block1);
        tx.borrow_mut().recover().unwrap();
        print_fm_values(ctx, &ctx.block1);
        assert_fm_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );
    }

    fn print_fm_values(ctx: &Context, block: &BlockId) {
        let mut p = Page::for_data(ctx.fm.blocksize());
        ctx.fm.read(block, &mut p).unwrap();

        println!("print: block data ({:?})", block);
        let mut pos = 0;
        for _i in 0..6 {
            let v = p.get_i32(pos);
            println!("p.get_i32({pos}) = {:?}", v);
            pos += I32_BYTE_SIZE as usize;
        }
        println!("p.get_string(30) = {:?}", p.get_string(30));
        println!("print: block data");
    }

    fn assert_fm_values(ctx: &Context, expected_i32s: [[i32; 6]; 2], expected_strs: [&str; 2]) {
        let mut p0 = Page::for_data(ctx.fm.blocksize());
        let mut p1 = Page::for_data(ctx.fm.blocksize());
        ctx.fm.read(&ctx.block0, &mut p0).unwrap();
        ctx.fm.read(&ctx.block1, &mut p1).unwrap();

        let mut pos = 0;
        for i in 0..6 {
            let v0 = p0.get_i32(pos).unwrap();
            assert_eq!(v0, expected_i32s[0][i]);

            let v1 = p1.get_i32(pos).unwrap();
            assert_eq!(v1, expected_i32s[1][i]);

            pos += I32_BYTE_SIZE as usize;
        }
        assert_eq!(p0.get_string(30).unwrap(), expected_strs[0]);
        assert_eq!(p1.get_string(30).unwrap(), expected_strs[1]);
    }
}
