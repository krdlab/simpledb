// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{path::Path, sync::Arc};

use crate::{
    buffer_mgr::BufferMgr, file_mgr::FileMgr, log_mgr::LogMgr, tx::transaction::Transaction,
};

pub struct SimpleDB<'lm, 'bm> {
    fm: Arc<FileMgr>,
    lm: Arc<LogMgr<'lm>>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
}

impl<'lm, 'bm> SimpleDB<'lm, 'bm> {
    const BLOCK_SIZE: usize = 400;
    const LOG_FILE: &str = "simpledb.log";
    const BUFFER_SIZE: usize = 8;

    pub fn new(db_dir_path: &Path, blocksize: usize, buffersize: usize) -> Self {
        let fm = Arc::new(FileMgr::new(db_dir_path, blocksize));
        let lm = Arc::new(LogMgr::new(fm.clone(), SimpleDB::LOG_FILE));
        let bm = Arc::new(BufferMgr::new(fm.clone(), lm.clone(), buffersize));
        Self { fm, lm, bm }
    }

    pub fn new_tx(&self) -> Transaction<'lm, 'bm> {
        Transaction::new(self.fm.clone(), self.lm.clone(), self.bm.clone())
    }

    pub fn file_mgr(&self) -> Arc<FileMgr> {
        self.fm.clone()
    }

    pub fn log_mgr(&self) -> Arc<LogMgr<'lm>> {
        self.lm.clone()
    }

    pub fn buffer_mgr(&self) -> Arc<BufferMgr<'bm, 'lm>> {
        self.bm.clone()
    }
}
