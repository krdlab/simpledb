// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    file::{
        block_id::BlockId,
        file_mgr::{FileMgr, FileMgrError},
        page::Page,
    },
    log_mgr::{LogMgr, LogMgrError, LSN},
};
use std::{
    iter::repeat_with,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, SystemTime},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BufferError {
    #[error("{0:?}")]
    IO(#[from] std::io::Error),

    #[error("{0:?}")]
    LogMgr(#[from] LogMgrError),

    #[error("{0:?}")]
    FileMgr(#[from] FileMgrError),

    #[error("unpinned buffer not found")]
    Aborted,
}

pub type Result<T> = core::result::Result<T, BufferError>;

pub struct Buffer<'b, 'lm> {
    fm: Arc<FileMgr>,
    lm: Arc<LogMgr<'lm>>,
    contents: Page<'b>,
    blk: Option<BlockId>,
    pins: i32,
    txnum: i32,
    lsn: LSN,
}

impl<'b, 'lm> Buffer<'b, 'lm> {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<LogMgr<'lm>>) -> Self {
        let blocksize = fm.blocksize();
        Self {
            fm,
            lm,
            contents: Page::for_data(blocksize),
            blk: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn contents_as_ref(&self) -> &Page<'b> {
        &self.contents
    }

    pub fn contents_as_mut(&mut self) -> &mut Page<'b> {
        &mut self.contents
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

    pub(crate) fn assign_to_block(&mut self, b: BlockId) -> Result<()> {
        self.flush()?;
        self.blk = Some(b);
        self.fm
            .read(self.blk.as_ref().unwrap(), &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.txnum >= 0 {
            self.lm.flush(self.lsn)?;
            self.fm
                .write(self.blk.as_ref().unwrap(), &mut self.contents)?;
            self.txnum = -1;
        }
        Ok(())
    }

    pub(crate) fn pin(&mut self) {
        self.pins += 1;
    }

    pub(crate) fn unpin(&mut self) {
        self.pins -= 1;
    }
}

pub struct BufferMgr<'b, 'lm> {
    data: Mutex<BufferMgrData<'b, 'lm>>,
    waiting: Condvar,
}

struct BufferMgrData<'b, 'lm> {
    bufferpool: Vec<Arc<Mutex<Buffer<'b, 'lm>>>>,
    num_available: usize,
}

const MAX_TIME: u64 = 10_000; // 10 seconds

impl<'b, 'lm> BufferMgr<'b, 'lm> {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<LogMgr<'lm>>, numbuffs: usize) -> Self {
        let pool = repeat_with(|| Arc::new(Mutex::new(Buffer::new(fm.clone(), lm.clone()))))
            .take(numbuffs)
            .collect::<Vec<_>>();
        Self {
            data: Mutex::new(BufferMgrData::<'b, 'lm> {
                bufferpool: pool,
                num_available: numbuffs,
            }),
            waiting: Condvar::new(),
        }
    }

    pub fn available(&self) -> usize {
        let data = self.data.lock().unwrap();
        data.num_available
    }

    pub fn flush_all(&self, txnum: i32) -> Result<()> {
        let data = self.data.lock().unwrap();
        for buff in data.bufferpool.iter() {
            let mut b = buff.lock().unwrap();
            if b.modifying_tx() == txnum {
                b.flush()?;
            }
        }
        Ok(())
    }

    pub fn unpin(&self, buff: Arc<Mutex<Buffer<'b, 'lm>>>) {
        let mut data = self.data.lock().unwrap();

        let mut b = buff.lock().unwrap();
        b.unpin();
        if !b.is_pinned() {
            data.num_available += 1;
            self.waiting.notify_all();
        }
    }

    pub fn pin(&self, blk: &BlockId) -> Result<Arc<Mutex<Buffer<'b, 'lm>>>> {
        let mut data = self.data.lock().unwrap();

        let begintime = SystemTime::now();

        let mut buff = data.try_to_pin(blk);
        while buff.is_none() && !self.waiting_too_long(begintime) {
            let result = self
                .waiting
                .wait_timeout(data, Duration::from_millis(MAX_TIME))
                .unwrap();
            if result.1.timed_out() {
                return Err(BufferError::Aborted);
            } else {
                data = result.0;
                buff = data.try_to_pin(blk);
            }
        }

        match buff {
            Some(found) => Ok(found.clone()),
            None => Err(BufferError::Aborted),
        }
    }

    fn waiting_too_long(&self, begintime: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(begintime)
            .unwrap()
            .as_millis()
            > MAX_TIME.into()
    }
}

impl<'b, 'lm> BufferMgrData<'b, 'lm> {
    pub(crate) fn try_to_pin(&mut self, blk: &BlockId) -> Option<Arc<Mutex<Buffer<'b, 'lm>>>> {
        let buff = if let Some(buff) = self.find_existing_buffer(blk) {
            buff
        } else {
            if let Some(buff) = self.choose_unpinned_buffer() {
                buff.lock().unwrap().assign_to_block(blk.clone()).unwrap();
                buff
            } else {
                return None;
            }
        };

        let mut b = buff.lock().unwrap();
        if !b.is_pinned() {
            self.num_available -= 1;
        }
        b.pin();

        Some(buff.clone())
    }

    pub(crate) fn find_existing_buffer(
        &self,
        blk: &BlockId,
    ) -> Option<Arc<Mutex<Buffer<'b, 'lm>>>> {
        for buff in self.bufferpool.iter() {
            let b = buff.lock().unwrap();
            if let Some(bufblk) = b.block() {
                if bufblk == blk {
                    return Some(buff.clone());
                }
            }
        }
        None
    }

    pub(crate) fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer<'b, 'lm>>>> {
        for buff in self.bufferpool.iter() {
            let b = buff.lock().unwrap();
            if !b.is_pinned() {
                return Some(buff.clone());
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn buffer_mgr<'b, 'lm>(
        dir_path: &Path,
        blocksize: usize,
        logfile: &str,
        numbuffs: usize,
    ) -> BufferMgr<'b, 'lm> {
        let fm = Arc::new(FileMgr::new(dir_path, blocksize));
        let lm = Arc::new(LogMgr::new(fm.clone(), logfile));
        BufferMgr::new(fm.clone(), lm.clone(), numbuffs)
    }

    #[test]
    fn test_buffer() -> Result<()> {
        let dir = tempdir()?;
        assert_eq!(dir.path().exists(), true);
        {
            let bm = buffer_mgr(dir.path(), 400, "test_buffer.log", 3);

            let buff1 = bm.pin(&BlockId::new("test_buffer_mgr", 1))?;
            {
                let mut b1 = buff1.lock().unwrap();
                let p = b1.contents_as_mut();
                let n = p.get_i32(80).unwrap();
                p.set_i32(80, n + 1).unwrap();
                b1.set_modified(1, 0);
                // what's n + 1?
            }
            bm.unpin(buff1);

            let mut buff2 = bm.pin(&BlockId::new("test_buffer_mgr", 2))?;
            let _buff3 = bm.pin(&BlockId::new("test_buffer_mgr", 3))?;
            let _buff4 = bm.pin(&BlockId::new("test_buffer_mgr", 4))?;

            bm.unpin(buff2);

            buff2 = bm.pin(&BlockId::new("test_buffer_mgr", 1))?;
            {
                let mut b2 = buff2.lock().unwrap();
                let p2 = b2.contents_as_mut();
                p2.set_i32(80, 9999).unwrap();
                b2.set_modified(1, 0);
            }
            bm.unpin(buff2);
        }
        dir.close()?;
        Ok(())
    }

    use array_macro::*;

    #[test]
    fn test_buffermgr() -> Result<()> {
        let dir = tempdir()?;
        assert_eq!(dir.path().exists(), true);
        {
            let bm = buffer_mgr(dir.path(), 400, "test_buffermgr.log", 3);

            let mut buff = array![None; 6];
            buff[0] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 0))?);
            buff[1] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 1))?);
            buff[2] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 2))?);

            let b1 = buff[1].take().unwrap();
            bm.unpin(b1);

            buff[3] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 0))?);
            buff[4] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 1))?);
            assert_eq!(bm.available(), 0); // TODO
            {
                let result = bm.pin(&BlockId::new("test_buffer_mgr", 3));
                assert_eq!(result.is_err(), true);
                assert_eq!(
                    result.err().unwrap().to_string(),
                    BufferError::Aborted.to_string()
                );
            }

            let b2 = buff[2].take().unwrap();
            bm.unpin(b2);

            buff[5] = Some(bm.pin(&BlockId::new("test_buffer_mgr", 3))?);
        }
        dir.close()?;
        Ok(())
    }
}
