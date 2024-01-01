// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{cell::RefCell, rc::Rc};

use crate::{
    constants::I32_BYTES_USIZE,
    file::block_id::BlockId,
    query::{predicate::Constant, scan::RID},
    record::schema::{Layout, SqlType},
    tx::transaction::{Transaction, TransactionError},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BTreePageError {
    #[error("{0:?}")]
    Transaction(#[from] TransactionError),

    #[error("BtreePage.current_block is none")]
    BlockNotFound,
}

pub type Result<T> = core::result::Result<T, BTreePageError>;

pub(crate) struct BTreePage<'lm, 'bm> {
    tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    current_block: Option<BlockId>,
    layout: Layout,
}

impl<'lm, 'bm> BTreePage<'lm, 'bm> {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        current_block: BlockId,
        layout: Layout,
    ) -> Result<Self> {
        tx.borrow_mut().pin(&current_block)?;
        Ok(Self {
            tx,
            current_block: Some(current_block),
            layout,
        })
    }

    pub fn find_slot_before(&self, search_key: Constant) -> Result<i32> {
        let mut slot = 0;
        while slot < self.get_num_recs()? && self.get_data_val(slot)? < search_key {
            slot += 1;
        }
        Ok(slot - 1)
    }

    pub fn close(&mut self) {
        if let Some(block) = self.current_block.as_ref() {
            self.tx.borrow_mut().unpin(block);
        }
        self.current_block = None;
    }

    pub fn is_full(&self) -> Result<bool> {
        self.get_num_recs()
            .map(|n| self.slot_pos(n + 1))
            .map(|pos| pos >= self.tx.borrow().block_size())
    }

    pub fn split(&self, split_pos: i32, flag: i32) -> Result<BlockId> {
        let new_block_id = self.append_new(flag)?;
        let mut new_page =
            BTreePage::new(self.tx.clone(), new_block_id.clone(), self.layout.clone())?;
        self.transfer_recs(split_pos, &new_page)?;
        new_page.set_flag(flag)?;
        new_page.close();
        Ok(new_block_id)
    }

    pub fn set_flag(&self, flag: i32) -> Result<()> {
        Ok(self.tx.borrow_mut().set_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            0,
            flag,
            true,
        )?)
    }

    pub fn get_flag(&self) -> Result<i32> {
        Ok(self.tx.borrow().get_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            0,
        )?)
    }

    fn transfer_recs(&self, slot: i32, dest: &BTreePage<'lm, 'bm>) -> Result<()> {
        let mut dest_slot = 0;
        while slot < self.get_num_recs()? {
            dest.insert(dest_slot)?;
            let schema = self.layout.schema();
            for field_name in schema.fields_iter() {
                dest.set_val(dest_slot, field_name, self.get_val(slot, field_name)?)?;
            }
            self.delete(slot)?;
            dest_slot += 1;
        }
        Ok(())
    }

    fn insert(&self, slot: i32) -> Result<()> {
        let num = self.get_num_recs()?;
        for i in ((slot + 1)..=num).rev() {
            self.copy_record(i - 1, i)?;
        }
        self.set_num_recs(num + 1)
    }

    pub fn delete(&self, slot: i32) -> Result<()> {
        let num = self.get_num_recs()?;
        for i in (slot + 1)..num {
            self.copy_record(i, i - 1)?;
        }
        self.set_num_recs(num - 1)
    }

    fn copy_record(&self, from: i32, to: i32) -> Result<()> {
        let schema = self.layout.schema();
        for field_name in schema.fields_iter() {
            self.set_val(to, field_name, self.get_val(from, field_name)?)?;
        }
        Ok(())
    }

    pub fn append_new(&self, flag: i32) -> Result<BlockId> {
        let block_id = self.tx.borrow_mut().append(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?
                .filename(),
        )?;
        self.tx.borrow_mut().pin(&block_id)?;
        self.format(&block_id, flag)?;
        Ok(block_id)
    }

    fn make_default_record(&self, block_id: &BlockId, pos: usize) -> Result<()> {
        for field_name in self.layout.schema().fields_iter() {
            let offset = self.layout.field_offset(&field_name).unwrap();
            let field_type = self.layout.schema().field_type(&field_name).unwrap();
            if field_type == SqlType::Integer {
                self.tx
                    .borrow_mut()
                    .set_i32(block_id, pos + offset, 0, false)?;
            }
            if field_type == SqlType::VarChar {
                self.tx
                    .borrow_mut()
                    .set_string(block_id, pos + offset, "", false)?;
            }
        }
        Ok(())
    }

    pub fn format(&self, block_id: &BlockId, flag: i32) -> Result<()> {
        let mut offset = 0;
        self.tx
            .borrow_mut()
            .set_i32(block_id, offset, flag, false)?;
        offset += I32_BYTES_USIZE;
        self.tx.borrow_mut().set_i32(block_id, offset, 0, false)?; // the number of records is 0
        offset += I32_BYTES_USIZE;

        let record_size = self.layout.slotsize();
        let mut pos = offset;
        while pos + record_size <= self.tx.borrow().block_size() {
            self.make_default_record(block_id, pos)?;
            pos += record_size;
        }
        Ok(())
    }

    pub fn set_num_recs(&self, num: i32) -> Result<()> {
        Ok(self.tx.borrow_mut().set_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            I32_BYTES_USIZE,
            num,
            true,
        )?)
    }

    pub fn get_num_recs(&self) -> Result<i32> {
        Ok(self.tx.borrow_mut().get_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            I32_BYTES_USIZE,
        )?)
    }

    pub fn get_data_val(&self, slot: i32) -> Result<Constant> {
        self.get_val(slot, "dataval")
    }

    fn set_val(&self, slot: i32, field_name: &str, val: Constant) -> Result<()> {
        let field_type = self.layout.schema().field_type(field_name).unwrap();
        match val {
            Constant::Int(i) if field_type == SqlType::Integer => {
                Ok(self.set_i32(slot, field_name, i)?)
            }
            Constant::String(s) if field_type == SqlType::VarChar => {
                Ok(self.set_string(slot, field_name, s)?)
            }
            _ => panic!("mismatched type: field_name={field_name}, val={val:?}"),
        }
    }

    fn set_i32(&self, slot: i32, field_name: &str, val: i32) -> Result<()> {
        let pos = self.field_pos(slot, field_name);
        Ok(self.tx.borrow_mut().set_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            pos,
            val,
            true,
        )?)
    }

    fn set_string(&self, slot: i32, field_name: &str, val: String) -> Result<()> {
        let pos = self.field_pos(slot, field_name);
        Ok(self.tx.borrow_mut().set_string(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            pos,
            &val,
            true,
        )?)
    }

    fn get_val(&self, slot: i32, field_name: &str) -> Result<Constant> {
        let field_type = self.layout.schema().field_type(field_name);
        if let Some(ft) = field_type {
            if ft == SqlType::Integer {
                self.get_i32(slot, field_name).map(Constant::Int)
            } else {
                self.get_string(slot, field_name).map(Constant::String)
            }
        } else {
            panic!("unknown type: slot {}, field {}", slot, field_name);
        }
    }

    fn get_i32(&self, slot: i32, field_name: &str) -> Result<i32> {
        let pos = self.field_pos(slot, field_name);
        Ok(self.tx.borrow().get_i32(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            pos,
        )?)
    }

    fn get_string(&self, slot: i32, field_name: &str) -> Result<String> {
        let pos = self.field_pos(slot, field_name);
        Ok(self.tx.borrow().get_string(
            self.current_block
                .as_ref()
                .ok_or(BTreePageError::BlockNotFound)?,
            pos,
        )?)
    }

    fn field_pos(&self, slot: i32, field_name: &str) -> usize {
        let offset = self.layout.field_offset(field_name);
        self.slot_pos(slot) + offset.unwrap()
    }

    fn slot_pos(&self, slot: i32) -> usize {
        let slotsize = self.layout.slotsize();
        I32_BYTES_USIZE + I32_BYTES_USIZE + (slot as usize * slotsize)
    }

    // TODO: these methods only called by BTreeDir

    pub fn get_child_num(&self, slot: i32) -> Result<i32> {
        self.get_i32(slot, "block")
    }

    pub fn insert_dir(&self, slot: i32, val: Constant, blknum: i32) -> Result<()> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_i32(slot, "block", blknum)?;
        Ok(())
    }

    // TODO: these methods only called by BTreeLeaf

    pub fn get_data_rid(&self, slot: i32) -> Result<RID> {
        let blknum = self.get_i32(slot, "block")?;
        let id = self.get_i32(slot, "id")?;
        Ok(RID::new(blknum as i64, Some(id)))
    }

    pub fn insert_leaf(&self, slot: i32, val: Constant, rid: RID) -> Result<()> {
        self.insert(slot)?;
        self.set_val(slot, "dataval", val)?;
        self.set_i32(slot, "block", rid.block_number_as_i32())?;
        self.set_i32(slot, "id", rid.slot().unwrap())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::BTreePage;
    use crate::{
        record::schema::{Layout, Schema},
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let mut db = SimpleDB::new_for_test(dir.path(), "btree_page_test.log");
            db.init();

            let tx = db.new_tx();
            {
                let block_id = tx.borrow_mut().append("btree_page_test_file").unwrap();
                let layout = {
                    let mut schema = Schema::new();
                    schema.add_i32_field("block");
                    schema.add_i32_field("dataval");
                    Layout::new(schema)
                };
                let mut page = BTreePage::new(tx.clone(), block_id, layout).unwrap();
                page.close();

                // TODO: add more testcases
            }
        }
    }
}
