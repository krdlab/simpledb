// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    buffer_mgr::BufferMgr,
    file::file_mgr::FileMgr,
    log_mgr::LogMgr,
    metadata::metadata_mgr::MetadataMgr,
    tx::{
        lock_table::LockTable,
        transaction::{Transaction, TxNumber},
    },
};
use std::{cell::RefCell, path::Path, rc::Rc, sync::Arc};

pub struct SimpleDB<'lm, 'bm> {
    fm: Arc<FileMgr>,
    lm: Arc<LogMgr<'lm>>,
    bm: Arc<BufferMgr<'bm, 'lm>>,
    tn: TxNumber,
    lt: Arc<LockTable>,
    mm: Option<MetadataMgr>,
}

impl<'lm, 'bm> SimpleDB<'lm, 'bm> {
    const BLOCK_SIZE: usize = 4096;
    const LOG_FILE: &str = "simpledb.log";
    const BUFFER_SIZE: usize = 8;

    pub fn new(db_dir_path: &Path, blocksize: usize, buffersize: usize) -> Self {
        let fm = Arc::new(FileMgr::new(db_dir_path, blocksize));
        let lm = Arc::new(LogMgr::new(fm.clone(), SimpleDB::LOG_FILE));
        let bm = Arc::new(BufferMgr::new(fm.clone(), lm.clone(), buffersize));
        let tn = TxNumber::new();
        let lt = Arc::new(LockTable::new());
        Self {
            fm,
            lm,
            bm,
            tn,
            lt,
            mm: None,
        }
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
        let lt = Arc::new(LockTable::new());
        Self {
            fm,
            lm,
            bm,
            tn,
            lt,
            mm: None,
        }
    }

    pub fn init(&mut self) {
        let is_new = self.fm.is_new();

        let tx = self.new_tx();
        let mm = MetadataMgr::new(is_new, tx.clone());
        tx.borrow_mut().commit().unwrap();

        self.mm = Some(mm);
    }

    pub fn new_tx(&self) -> Rc<RefCell<Transaction<'lm, 'bm>>> {
        Rc::new(RefCell::new(Transaction::new(
            self.tn.next(),
            self.fm.clone(),
            self.lm.clone(),
            self.bm.clone(),
            self.lt.clone(),
        )))
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

    pub fn metadata_mgr(&self) -> &MetadataMgr {
        self.mm.as_ref().unwrap()
    }
}
