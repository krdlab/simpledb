// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::page::{Page, PageError};
use crate::{constants::I32_BYTE_SIZE, log_mgr::LogMgr, tx::transaction::Transaction, BlockId};
use crate::{log_mgr, page};
use std::{convert::TryInto, fmt::Display};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecoveryError {
    #[error("{0:?}")]
    Byte(#[from] PageError),

    #[error("unknown op: {0:?}")]
    UnknownOp(i32),
}

pub type Result<T> = core::result::Result<T, RecoveryError>;

#[derive(Debug)]
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
    fn undo<'t>(&self, tx: &'t mut Transaction);
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
    pub fn write_to_log(lm: &mut LogMgr) -> log_mgr::Result<i64> {
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

    fn undo<'t>(&self, _tx: &'t mut Transaction) {}
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

    pub fn write_to_log(lm: &mut LogMgr, txnum: i32) -> log_mgr::Result<i64> {
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

    fn undo<'t>(&self, _tx: &'t mut Transaction) {}
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

    pub fn write_to_log(lm: &mut LogMgr, txnum: i32) -> log_mgr::Result<i64> {
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

    fn undo<'t>(&self, _tx: &'t mut Transaction) {}
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

    pub fn write_to_log(lm: &mut LogMgr, txnum: i32) -> log_mgr::Result<i64> {
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

    fn undo<'t>(&self, _tx: &'t mut Transaction) {}
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
        lm: &mut LogMgr,
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

    fn undo<'t>(&self, tx: &'t mut Transaction) {
        tx.pin(&self.block);
        tx.set_i32(&self.block, self.offset, self.value, false);
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
        lm: &mut LogMgr,
        txnum: i32,
        blk: &BlockId,
        offset: i32,
        val: &str,
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
            p.set_i32(opos, offset)?;
            p.set_string(vpos, val)?;
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

    fn undo<'t>(&self, tx: &'t mut Transaction) {
        tx.pin(&self.block);
        tx.set_string(&self.block, self.offset, &self.value, false);
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
