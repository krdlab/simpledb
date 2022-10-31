// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{path::Path, sync::Arc};

use crate::{
    buffer_mgr::BufferMgr,
    file_mgr::FileMgr,
    log_mgr::LogMgr,
    tx::{
        lock_table::LockTable,
        transaction::{Transaction, TxNumber},
    },
};

pub struct SimpleDB<'lm, 'bm> {
    fm: Arc<FileMgr>,
    lm: Arc<LogMgr<'lm>>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
    tn: TxNumber,
    lt: LockTable,
}

impl<'lm, 'bm> SimpleDB<'lm, 'bm> {
    const BLOCK_SIZE: usize = 400;
    const LOG_FILE: &str = "simpledb.log";
    const BUFFER_SIZE: usize = 8;

    pub fn new(db_dir_path: &Path, blocksize: usize, buffersize: usize) -> Self {
        let fm = Arc::new(FileMgr::new(db_dir_path, blocksize));
        let lm = Arc::new(LogMgr::new(fm.clone(), SimpleDB::LOG_FILE));
        let bm = Arc::new(BufferMgr::new(fm.clone(), lm.clone(), buffersize));
        let tn = TxNumber::new();
        let lt = LockTable::new();
        Self { fm, lm, bm, tn, lt }
    }

    pub fn new_for_test(db_dir_path: &Path, logfile: &str) -> Self {
        let fm = Arc::new(FileMgr::new(db_dir_path, SimpleDB::BLOCK_SIZE));
        let lm = Arc::new(LogMgr::new(fm.clone(), logfile));
        let bm = Arc::new(BufferMgr::new(
            fm.clone(),
            lm.clone(),
            SimpleDB::BUFFER_SIZE,
        ));
        let tn = TxNumber::new();
        let lt = LockTable::new();
        Self { fm, lm, bm, tn, lt }
    }

    pub fn new_tx<'lt, 's: 'lt>(&'s self) -> Transaction<'lm, 'bm, 'lt> {
        Transaction::new(
            self.tn.next(),
            self.fm.clone(),
            self.lm.clone(),
            self.bm.clone(),
            &self.lt,
        )
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
