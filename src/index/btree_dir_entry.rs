// Copyright (c) 2024 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::query::predicate::Constant;

pub(crate) struct DirEntry {
    dataval: Constant,
    block_num: i32,
}

impl DirEntry {
    pub fn new(dataval: Constant, block_num: i32) -> Self {
        Self { dataval, block_num }
    }

    pub fn get_data_val(&self) -> &Constant {
        &self.dataval
    }

    pub fn get_block_num(&self) -> i32 {
        self.block_num
    }
}
