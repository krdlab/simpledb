// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    constants::I32_BYTE_SIZE,
    file::{
        block_id::BlockId,
        file_mgr::{FileMgr, FileMgrError},
        page::{Page, PageError},
    },
};
use std::sync::{Arc, Mutex, MutexGuard};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogMgrError {
    #[error("{0:?}")]
    IO(#[from] std::io::Error),

    #[error("{0:?}")]
    Page(#[from] PageError),

    #[error("{0:?}")]
    FileMgr(#[from] FileMgrError),
}

pub type Result<T> = core::result::Result<T, LogMgrError>;

pub type LSN = i64;

pub struct LogMgr<'p> {
    fm: Arc<FileMgr>,
    logfile: String,
    data: Mutex<LogMgrData<'p>>,
}

struct LogMgrData<'p> {
    logpage: Page<'p>,
    currentblk: Option<BlockId>,
    latest_lsn: LSN,
    last_saved_lsn: LSN,
}

impl<'p> LogMgrData<'p> {
    pub(crate) fn new(logpage: Page<'p>) -> Self {
        Self {
            logpage,
            currentblk: None,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }
}

impl<'p> LogMgr<'p> {
    pub fn new(fm: Arc<FileMgr>, logfile: &str) -> Self {
        let blocksize = fm.blocksize();
        let logsize: i64 = fm.length(logfile).unwrap().try_into().unwrap();

        let lm = Self {
            fm: fm.clone(),
            logfile: logfile.to_string(),
            data: Mutex::new(LogMgrData::new(Page::for_data(blocksize))),
        };
        {
            let mut lmd = lm.data.lock().unwrap();
            if logsize == 0 {
                let new_blk = lm.append_new_block(&mut lmd).unwrap().clone();
                lmd.currentblk = Some(new_blk);
            } else {
                let block = BlockId::new(logfile, logsize - 1);
                fm.read(&block, &mut lmd.logpage).unwrap();
                lmd.currentblk = Some(block);
            }
        }
        lm
    }

    fn append_new_block(&self, data: &mut MutexGuard<LogMgrData>) -> Result<BlockId> {
        let block = self.fm.append(&self.logfile)?;
        let blocksize = self.fm.blocksize().try_into().unwrap();
        data.logpage.set_i32(0, blocksize)?;
        self.fm.write(&block, &mut data.logpage)?;
        Ok(block)
    }

    pub fn apppend(&self, logrec: &[u8]) -> Result<LSN> {
        let mut data = self.data.lock().unwrap();

        let mut boundary = data.logpage.get_i32(0)?;
        let recsize: i32 = logrec.len().try_into().unwrap();
        let bytesneeded: i32 = recsize + I32_BYTE_SIZE;
        if boundary - bytesneeded < I32_BYTE_SIZE {
            self._flush(&mut data)?;
            data.currentblk = Some(self.append_new_block(&mut data)?);
            boundary = data.logpage.get_i32(0)?;
        }

        let recpos = boundary - bytesneeded;
        let recpos_usize = usize::try_from(boundary - bytesneeded).unwrap();
        data.logpage.set_bytes(recpos_usize, logrec)?;
        data.logpage.set_i32(0, recpos)?;
        data.latest_lsn += 1;
        Ok(data.latest_lsn)
    }

    pub fn flush(&self, lsn: LSN) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        if lsn >= data.last_saved_lsn {
            self._flush(&mut data)?;
        }
        Ok(())
    }

    fn _flush(&self, data: &mut MutexGuard<LogMgrData>) -> Result<()> {
        let block = data.currentblk.as_ref().unwrap().clone();
        self.fm.write(&block, &mut data.logpage)?;
        data.last_saved_lsn = data.latest_lsn;
        Ok(())
    }

    pub fn reverse_iter(&self) -> Result<LogIterator<'_>> {
        let mut data = self.data.lock().unwrap();
        self._flush(&mut data)?;

        let block = data.currentblk.as_ref().unwrap().clone();
        Ok(LogIterator::new(self.fm.clone(), block))
    }
}

pub struct LogIterator<'lm> {
    fm: Arc<FileMgr>,
    block: BlockId,
    page: Page<'lm>,
    currentpos: i32,
    boundary: i32,
}

impl<'lm> LogIterator<'lm> {
    pub fn new(fm: Arc<FileMgr>, blk: BlockId) -> Self {
        let blocksize = fm.blocksize();

        let mut iter = Self {
            fm,
            block: BlockId::new(blk.filename(), blk.number()),
            page: Page::for_data(blocksize),
            currentpos: 0,
            boundary: 0,
        };
        iter.move_to_block(&blk);
        iter
    }

    fn move_to_block(&mut self, block: &BlockId) {
        self.fm
            .read(block, &mut self.page)
            .expect(format!("failed to read the block at {:?}", block).as_str());
        self.boundary = self
            .page
            .get_i32(0)
            .expect("failed to get a boundary value from the current page");
        self.currentpos = self.boundary;
    }

    pub fn has_next(&self) -> bool {
        usize::try_from(self.currentpos).unwrap() < self.fm.blocksize() || self.block.number() > 0
    }
}

impl Iterator for LogIterator<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        let blocksize = self.fm.blocksize();
        if self.currentpos == blocksize.try_into().unwrap() {
            let newblock = BlockId::new(self.block.filename(), self.block.number() - 1);
            self.move_to_block(&newblock);
            self.block = newblock;
        }
        let rec = self
            .page
            .get_bytes(self.currentpos.try_into().unwrap())
            .expect(format!("failed to get a record at {}", self.currentpos).as_str());
        self.currentpos += I32_BYTE_SIZE + i32::try_from(rec.len()).unwrap();
        Some(rec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_append() -> Result<()> {
        let dir = tempdir()?;
        assert_eq!(dir.path().exists(), true);

        let fm = Arc::new(FileMgr::new(dir.path(), 4096));
        assert_eq!(fm.is_new(), false);

        let lm = LogMgr::new(fm, "test_logmgr.log");

        let logrec = [1u8, 2u8, 3u8];
        let lsn = lm.apppend(&logrec)?;
        assert_eq!(lsn, 1);

        let mut it = lm.reverse_iter()?;
        assert_eq!(it.has_next(), true);

        let rec = match it.next() {
            Some(r) => r,
            None => vec![],
        };
        assert_eq!(rec, logrec);

        dir.close()?;
        Ok(())
    }
}
