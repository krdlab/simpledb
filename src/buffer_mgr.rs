// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    file_mgr::{FileMgr, FileMgrError},
    log_mgr::{LogMgr, LogMgrError, LSN},
    page::Page,
    BlockId,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BufferError {
    #[error("{0:?}")]
    LogMgr(#[from] LogMgrError),

    #[error("{0:?}")]
    FileMgr(#[from] FileMgrError),
}

pub type Result<T> = core::result::Result<T, BufferError>;

pub struct Buffer<'fm, 'lm> {
    fm: &'fm FileMgr,
    lm: &'lm mut LogMgr<'lm>,
    contents: Page<'fm>,
    blk: Option<BlockId>,
    pins: i32,
    txnum: i32,
    lsn: LSN,
}

impl<'fm, 'lm> Buffer<'fm, 'lm> {
    pub fn new(fm: &'fm FileMgr, lm: &'lm mut LogMgr<'lm>) -> Self {
        Self {
            fm,
            lm,
            contents: Page::for_data(fm.blocksize()),
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents(&self) -> &Page<'fm> {
        &self.contents
    }

    pub fn block(&self) -> &Option<BlockId> {
        &self.blk
    }

    pub fn set_modified(&mut self, txnum: i32, lsn: LSN) {
        self.txnum = txnum;
        if self.lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.txnum
    }

    pub(in crate) fn assign_to_block(&mut self, b: BlockId) -> Result<()> {
        self.flush()?;
        self.blk = Some(b);
        self.fm
            .read(self.blk.as_ref().unwrap(), &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    pub(in crate) fn flush(&mut self) -> Result<()> {
        if self.txnum >= 0 {
            self.lm.flush(self.lsn)?;
            self.fm
                .write(self.blk.as_ref().unwrap(), &mut self.contents)?;
            self.txnum = -1;
        }
        Ok(())
    }

    pub(in crate) fn pin(&mut self) {
        self.pins += 1;
    }

    pub(in crate) fn unpin(&mut self) {
        self.pins -= 1;
    }
}
