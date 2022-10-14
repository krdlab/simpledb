// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::transaction::TxInner;
use crate::buffer_mgr::{Buffer, BufferError, BufferMgr};
use crate::log_mgr::{LogMgrError, LSN};
use crate::page::{Page, PageError};
use crate::{constants::I32_BYTE_SIZE, log_mgr::LogMgr, BlockId};
use crate::{log_mgr, page};
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

trait LogRecord {
    fn op(&self) -> Op;
    fn tx_number(&self) -> i32;
    fn undo<'t>(&self, tx: &'t mut TxInner);
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

    fn undo<'t>(&self, _tx: &'t mut TxInner) {}
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

    fn undo<'t>(&self, _tx: &'t mut TxInner) {}
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

    fn undo<'t>(&self, _tx: &'t mut TxInner) {}
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

    fn undo<'t>(&self, _tx: &'t mut TxInner) {}
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

    fn undo<'t>(&self, tx: &'t mut TxInner) {
        tx.pin(&self.block);
        tx.set_i32_for_recovery(&self.block, self.offset, self.value);
        tx.unpin(&self.block);
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

    fn undo<'t>(&self, tx: &'t mut TxInner) {
        tx.pin(&self.block);
        tx.set_string_for_recovery(&self.block, self.offset, &self.value);
        tx.unpin(&self.block);
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

    pub(crate) fn rollback<'tx>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        self.do_rollback(tx);
        self.bm.flush_all(self.txnum)?;
        let lsn = RollbackRecord::write_to_log(self.lm.clone(), self.txnum)?;
        self.lm.flush(lsn)?;
        Ok(())
    }

    pub(crate) fn recover<'tx>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        self.do_recover(tx);
        self.bm.flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(self.lm.clone())?;
        self.lm.flush(lsn)?;
        Ok(())
    }

    pub fn set_i32(&mut self, buff: &mut Buffer, offset: usize, _newval: i32) -> Result<LSN> {
        let oldval = buff.contents().get_i32(offset)?;
        let blk = buff.block().as_ref().unwrap();
        let lsn = SetIntRecord::write_to_log(self.lm.clone(), self.txnum, blk, offset, oldval)?;
        Ok(lsn)
    }

    pub fn set_string(&self, buff: &mut Buffer, offset: usize, _newval: &str) -> Result<LSN> {
        let oldval = buff.contents().get_string(offset)?;
        let blk = buff.block().as_ref().unwrap();
        let lsn = SetStringRecord::write_to_log(self.lm.clone(), self.txnum, blk, offset, oldval)?;
        Ok(lsn)
    }

    fn do_rollback<'tx>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
        let mut iter = self.lm.reverse_iter()?;
        while iter.has_next() {
            let bytes = iter.next().unwrap();
            let rec = create_log_record(bytes)?;
            if rec.tx_number() == self.txnum {
                if rec.op() == Op::Start {
                    break;
                }
                rec.undo(tx);
            }
        }
        Ok(())
    }

    fn do_recover<'tx>(&self, tx: &'tx mut TxInner<'lm, 'bm>) -> Result<()> {
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
                rec.undo(tx);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::{file_mgr::FileMgr, server::simple_db::SimpleDB};
    use tempfile::{tempdir, TempDir};

    struct Context<'lm, 'bm> {
        db: SimpleDB<'lm, 'bm>,
        fm: Arc<FileMgr>,
        bm: Arc<BufferMgr<'bm, 'lm>>,
        block0: BlockId,
        block1: BlockId,
    }
    impl Context<'_, '_> {
        pub fn new(dir: &Path) -> Self {
            let db = SimpleDB::new(dir, 400, 8);
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
    fn test_recovery_mgr() -> Result<()> {
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
        Ok(())
    }

    fn test_initialize(ctx: &mut Context) {
        let mut tx1 = ctx.db.new_tx();
        let mut tx2 = ctx.db.new_tx();
        tx1.pin(&ctx.block0).unwrap();
        tx2.pin(&ctx.block1).unwrap();

        let mut pos = 0;
        for _i in 0..6 {
            tx1.set_i32(&ctx.block0, pos, pos as i32, false).unwrap();
            tx2.set_i32(&ctx.block1, pos, pos as i32, false).unwrap();
            pos += I32_BYTE_SIZE as usize;
        }

        tx1.set_string(&ctx.block0, 30, "abc", false).unwrap();
        tx2.set_string(&ctx.block1, 30, "def", false).unwrap();
        tx1.commit().unwrap();
        tx2.commit().unwrap();

        assert_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );
    }

    fn test_modify(ctx: &Context) {
        let mut tx3 = ctx.db.new_tx();
        let mut tx4 = ctx.db.new_tx();

        tx3.pin(&ctx.block0).unwrap();
        tx4.pin(&ctx.block1).unwrap();

        let mut pos = 0;
        for _i in 0..6 {
            tx3.set_i32(&ctx.block0, pos, pos as i32 + 100, true)
                .unwrap();
            tx4.set_i32(&ctx.block1, pos, pos as i32 + 100, true)
                .unwrap();
            pos += I32_BYTE_SIZE as usize;
        }
        tx3.set_string(&ctx.block0, 30, "uvw", true).unwrap();
        tx4.set_string(&ctx.block1, 30, "xyz", true).unwrap();
        ctx.bm.flush_all(3).unwrap();
        ctx.bm.flush_all(4).unwrap();
        assert_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );

        tx3.rollback().unwrap();
        assert_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );
    }

    fn test_recover(ctx: &mut Context) {
        let mut tx = ctx.db.new_tx();
        tx.recover().unwrap();
        assert_values(
            &ctx,
            [[0, 4, 8, 12, 16, 20], [0, 4, 8, 12, 16, 20]],
            ["abc", "def"],
        );
    }

    fn assert_values(ctx: &Context, expected_i32s: [[i32; 6]; 2], expected_strs: [&str; 2]) {
        let mut p0 = Page::for_data(ctx.fm.blocksize());
        let mut p1 = Page::for_data(ctx.fm.blocksize());
        ctx.fm.read(&ctx.block0, &mut p0).unwrap();
        ctx.fm.read(&ctx.block1, &mut p1).unwrap();

        let mut pos = 0;
        for i in 0..6 {
            assert_eq!(p0.get_i32(pos).unwrap(), expected_i32s[0][i]);
            assert_eq!(p1.get_i32(pos).unwrap(), expected_i32s[1][i]);
            pos += I32_BYTE_SIZE as usize;
        }
        assert_eq!(p0.get_string(30).unwrap(), expected_strs[0]);
        assert_eq!(p1.get_string(30).unwrap(), expected_strs[1]);
    }
}
