// Copyright (c) 2024 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    btree_dir_entry::DirEntry,
    btree_page::{BTreePage, Result},
};
use crate::{
    file::block_id::BlockId,
    query::{predicate::Constant, scan::RID},
    record::schema::Layout,
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc};

pub(crate) struct BTreeLeaf<'lm, 'bm> {
    tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    layout: Layout,
    search_key: Constant,
    contents: BTreePage<'lm, 'bm>,
    current_slot: i32,
    filename: String,
}

impl<'lm, 'bm> BTreeLeaf<'lm, 'bm> {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        block_id: BlockId,
        layout: Layout,
        search_key: Constant,
    ) -> Result<Self> {
        let contents = BTreePage::new(tx.clone(), block_id.clone(), layout.clone())?;
        let current_slot = contents.find_slot_before(search_key.clone())?;
        let filename = block_id.filename().to_string();
        Ok(Self {
            tx,
            layout,
            search_key,
            contents,
            current_slot,
            filename,
        })
    }

    pub fn close(&mut self) {
        self.contents.close();
    }

    pub fn next(&mut self) -> Result<bool> {
        self.current_slot += 1;
        if self.current_slot >= self.contents.get_num_recs()? {
            self.try_overflow()
        } else if self.contents.get_data_val(self.current_slot)? == self.search_key {
            Ok(true)
        } else {
            self.try_overflow()
        }
    }

    fn try_overflow(&mut self) -> Result<bool> {
        let first_key = self.contents.get_data_val(0)?;
        let flag = self.contents.get_flag()?;
        if self.search_key != first_key || flag < 0 {
            Ok(false)
        } else {
            self.contents.close();
            let next_block_id = BlockId::new(&self.filename, flag as i64);
            self.contents = BTreePage::new(self.tx.clone(), next_block_id, self.layout.clone())?;
            self.current_slot = 0;
            Ok(true)
        }
    }

    pub fn get_data_rid(&self) -> Result<RID> {
        self.contents.get_data_rid(self.current_slot)
    }

    pub fn delete(&mut self, data_rid: RID) -> Result<()> {
        while self.next()? {
            if self.get_data_rid()? == data_rid {
                self.contents.delete(self.current_slot)?;
                break;
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, data_rid: RID) -> Result<Option<DirEntry>> {
        if self.contents.get_flag()? >= 0 && self.contents.get_data_val(0)? > self.search_key {
            let first_val = self.contents.get_data_val(0)?;
            let new_block_id = self.contents.split(0, self.contents.get_flag()?)?;
            self.current_slot = 0;
            self.contents.set_flag(-1)?;
            self.contents
                .insert_leaf(0, self.search_key.clone(), data_rid)?;
            return Ok(Some(DirEntry::new(first_val, new_block_id.number() as i32)));
        }

        self.current_slot += 1;
        self.contents
            .insert_leaf(self.current_slot, self.search_key.clone(), data_rid)?;
        if !self.contents.is_full()? {
            return Ok(None);
        }

        let first_key = self.contents.get_data_val(0)?;
        let last_key = self
            .contents
            .get_data_val(self.contents.get_num_recs()? - 1)?;
        if last_key == first_key {
            let new_block_id = self.contents.split(1, self.contents.get_flag()?)?;
            self.contents.set_flag(new_block_id.number() as i32)?;
            return Ok(None);
        } else {
            let mut split_pos = self.contents.get_num_recs()? / 2;
            let mut split_key = self.contents.get_data_val(split_pos)?;
            if split_key == first_key {
                while self.contents.get_data_val(split_pos)? == split_key {
                    split_pos += 1;
                }
                split_key = self.contents.get_data_val(split_pos)?;
            } else {
                while self.contents.get_data_val(split_pos - 1)? == split_key {
                    split_pos -= 1;
                }
            }
            let new_block_id = self.contents.split(split_pos, -1)?;
            return Ok(Some(DirEntry::new(split_key, new_block_id.number() as i32)));
        }
    }
}

impl<'lm, 'bm> Drop for BTreeLeaf<'lm, 'bm> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // TODO:
    }
}
