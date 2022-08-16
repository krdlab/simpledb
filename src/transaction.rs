// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::sync::{Arc, Mutex};

use crate::{buffer_mgr::BufferMgr, file_mgr::FileMgr, log_mgr::LogMgr, BlockId};

pub struct Transaction {}

impl Transaction {
    pub fn new(fm: Arc<FileMgr>, lm: Arc<Mutex<LogMgr>>, bm: BufferMgr) -> Self {
        todo!()
    }

    pub fn commit(&self) {
        todo!()
    }

    pub fn rollback(&self) {
        todo!()
    }

    pub fn recover(&self) {
        todo!()
    }

    pub fn pin(&self, blk: &BlockId) {
        todo!()
    }

    pub fn unpin(&self, blk: &BlockId) {
        todo!()
    }

    pub fn get_i32(&self, blk: &BlockId, offset: usize) -> i32 {
        todo!()
    }

    pub fn get_string(&self, blk: &BlockId, offset: usize) -> String {
        todo!()
    }

    pub fn set_i32(&self, blk: &BlockId, offset: usize, val: i32, ok_to_log: bool) {
        todo!()
    }

    pub fn set_string(&self, blk: &BlockId, offset: usize, val: &str, ok_to_log: bool) {
        todo!()
    }

    pub fn available_buffs() -> i32 {
        todo!()
    }

    pub fn size(&self, filename: &str) -> i32 {
        todo!()
    }

    pub fn append(&self, filename: &str) -> BlockId {
        todo!()
    }

    pub fn block_size(&self) -> i32 {
        todo!()
    }
}
