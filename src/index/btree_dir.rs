// Copyright (c) 2024 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    btree_dir_entry::DirEntry,
    btree_page::{BTreePage, Result},
};
use crate::{
    file::block_id::BlockId, query::predicate::Constant, record::schema::Layout,
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc};

pub(crate) struct BTreeDir<'lm, 'bm> {
    tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    layout: Layout,
    contents: BTreePage<'lm, 'bm>,
    filename: String,
}

impl<'lm, 'bm> BTreeDir<'lm, 'bm> {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        block_id: BlockId,
        layout: Layout,
    ) -> Result<Self> {
        let contents = BTreePage::new(tx.clone(), block_id.clone(), layout.clone())?;
        Ok(Self {
            tx,
            layout,
            contents,
            filename: block_id.filename().to_string(),
        })
    }

    pub fn close(&mut self) {
        self.contents.close();
    }

    pub fn search(&mut self, search_key: Constant) -> Result<i32> {
        let mut child_block_id = self.find_child_block(&search_key)?;
        while self.contents.get_flag()? > 0 {
            self.contents = BTreePage::new(self.tx.clone(), child_block_id, self.layout.clone())?;
            child_block_id = self.find_child_block(&search_key)?;
        }
        Ok(child_block_id.number() as i32)
    }

    fn find_child_block(&self, search_key: &Constant) -> Result<BlockId> {
        let mut slot = self.contents.find_slot_before(search_key.clone())?;
        if self.contents.get_data_val(slot + 1)? == *search_key {
            slot += 1;
        }
        let block_num = self.contents.get_child_num(slot)?;
        Ok(BlockId::new(&self.filename, block_num as i64))
    }

    pub fn make_new_root(&self, child: DirEntry) -> Result<()> {
        let first_val = self.contents.get_data_val(0)?;
        let level = self.contents.get_flag()?;
        let new_block_id = self.contents.split(0, level)?;
        let old_root = DirEntry::new(first_val, new_block_id.number() as i32);
        self.insert_entry(old_root)?;
        self.insert_entry(child)?;
        self.contents.set_flag(level + 1)?;
        Ok(())
    }

    fn insert_entry(&self, entry: DirEntry) -> Result<Option<DirEntry>> {
        let new_slot = self
            .contents
            .find_slot_before(entry.get_data_val().clone())?;
        self.contents.insert_dir(
            new_slot,
            entry.get_data_val().clone(),
            entry.get_block_num(),
        )?;
        if !self.contents.is_full()? {
            return Ok(None);
        }

        let level = self.contents.get_flag()?;
        let split_pos = self.contents.get_num_recs()? / 2;
        let split_val = self.contents.get_data_val(split_pos)?;
        let new_block_id = self.contents.split(split_pos, level)?;
        Ok(Some(DirEntry::new(split_val, new_block_id.number() as i32)))
    }

    pub fn insert(&self, entry: DirEntry) -> Result<Option<DirEntry>> {
        if self.contents.get_flag()? == 0 {
            return self.insert_entry(entry);
        }
        let child_block_id = self.find_child_block(entry.get_data_val())?;
        let mut child =
            BTreeDir::new(self.tx.clone(), child_block_id.clone(), self.layout.clone())?;
        let my_entry = child.insert(entry)?;
        child.close();
        match my_entry {
            Some(e) => self.insert_entry(e),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // TODO:
    }
}
