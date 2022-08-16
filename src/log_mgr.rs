// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    constants::I32_BYTE_SIZE,
    file_mgr::{FileMgr, FileMgrError},
    page::{Page, PageError},
    BlockId,
};
use std::sync::Arc;
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
    logpage: Page<'p>,
    currentblk: Option<BlockId>,
    latest_lsn: LSN,
    last_saved_lsn: LSN,
}

impl<'p> LogMgr<'p> {
    pub fn new(fm: Arc<FileMgr>, logfile: &str) -> Self {
        let blocksize = fm.blocksize();
        let logsize = fm
            .length(logfile)
            .expect("failed to get the size of logfile");

        let mut lm = Self {
            fm: fm.clone(),
            logfile: logfile.to_string(),
            logpage: Page::for_data(blocksize),
            currentblk: None,
            latest_lsn: 0,
            last_saved_lsn: 0,
        };

        if logsize == 0 {
            lm.currentblk = Some(
                lm.append_new_block()
                    .expect("failed to call append_new_block"),
            );
        } else {
            let block = BlockId::new(logfile, logsize - 1);
            fm.read(&block, &mut lm.logpage)
                .expect(format!("failed to read the block at {:?}", block).as_str());
            lm.currentblk = Some(block);
        }

        lm
    }

    fn append_new_block(&mut self) -> Result<BlockId> {
        let block = self.fm.append(&self.logfile)?;
        let blocksize = self.fm.blocksize().try_into().unwrap();
        self.logpage.set_i32(0, blocksize)?;
        self.fm.write(&block, &mut self.logpage)?;
        Ok(block)
    }

    pub fn apppend(&mut self, logrec: &[u8]) -> Result<LSN> {
        let mut boundary = self.logpage.get_i32(0)?;
        let recsize: i32 = logrec.len().try_into().unwrap();
        let bytesneeded: i32 = recsize + I32_BYTE_SIZE;
        if boundary - bytesneeded < I32_BYTE_SIZE {
            self._flush()?;
            self.currentblk = Some(self.append_new_block()?);
            boundary = self.logpage.get_i32(0)?;
        }

        let recpos = boundary - bytesneeded;
        let recpos_usize = usize::try_from(boundary - bytesneeded).unwrap();
        self.logpage.set_bytes(recpos_usize, logrec)?;
        self.logpage.set_i32(0, recpos)?;
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    pub fn flush(&mut self, lsn: LSN) -> Result<()> {
        if lsn >= self.last_saved_lsn {
            self._flush()?;
        }
        Ok(())
    }

    fn _flush(&mut self) -> Result<()> {
        let block = self.currentblk.as_ref().expect("illegal state");
        self.fm.write(block, &mut self.logpage)?;
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }

    pub fn reverse_iter(&mut self) -> Result<LogIterator<'_>> {
        self._flush()?;
        let block = self.currentblk.as_ref().expect("illegal state");
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
    pub fn new(fm: Arc<FileMgr>, blk: &'lm BlockId) -> Self {
        let blocksize = fm.blocksize();

        let mut iter = Self {
            fm,
            block: BlockId {
                filename: blk.filename.to_string(),
                blknum: blk.blknum,
            },
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

        let mut lm = LogMgr::new(fm, "redo.log");

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
