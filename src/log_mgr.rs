// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::file_mgr::FileMgr;
use std::path::Path;

pub struct LogMgr {}

type LSN = u64;

impl LogMgr {
    pub fn new(fm: FileMgr, logfile: &Path) -> Self {
        Self {}
    }

    pub fn apppend(&self, rec: &[u8]) -> LSN {
        todo!()
    }

    pub fn flush(&self, lsn: LSN) {
        todo!()
    }

    pub fn reverse_iter(&self) -> impl Iterator<Item = &[u8]> {
        todo!()
    }
}
