// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::schema::{Layout, SqlType};
use crate::{
    file::block_id::BlockId,
    tx::transaction::{Transaction, TransactionError},
};
use std::convert::Into;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RecordPageError {
    #[error("illegal slot: {0:?}")]
    IllegalSlot(i32),

    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("{0:?}")]
    Transaction(#[from] TransactionError),
}

pub type Result<T> = core::result::Result<T, RecordPageError>;

#[derive(Debug, PartialEq, Eq)]
enum SlotFlag {
    Empty = 0,
    Used = 1,
}

impl Into<i32> for SlotFlag {
    fn into(self) -> i32 {
        self as i32
    }
}

pub struct RecordPage<'ly> {
    // tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
    block: BlockId,
    layout: &'ly Layout,
}

impl<'ly, 'tx, 'lm, 'bm, 'lt> RecordPage<'ly> {
    pub fn new(block: BlockId, layout: &'ly Layout) -> Self {
        Self { block, layout }
    }

    fn slot_offset(&self, slot: i32) -> Result<usize> {
        // assert!(slot >= 0);
        if slot >= 0 {
            Ok((slot as usize) * self.layout.slotsize())
        } else {
            Err(RecordPageError::IllegalSlot(slot))
        }
    }

    fn is_valid_slot(&self, tx: &'tx Transaction<'lm, 'bm, 'lt>, slot: i32) -> bool {
        self.slot_offset(slot + 1)
            .map_or(false, |o| o <= tx.block_size())
    }

    pub fn block(&self) -> &BlockId {
        &self.block
    }

    fn field_offset(&self, slot: i32, fname: &str) -> Result<usize> {
        let fpos = self.slot_offset(slot)?
            + self
                .layout
                .field_offset(fname)
                .ok_or(RecordPageError::FieldNotFound(fname.into()))?;
        Ok(fpos)
    }

    pub fn get_i32(
        &self,
        tx: &'tx Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
    ) -> Result<i32> {
        let foffset = self.field_offset(slot, fname)?;
        Ok(tx.get_i32(&self.block, foffset)?)
    }

    pub fn set_i32(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
        value: i32,
    ) -> Result<()> {
        let foffset = self.field_offset(slot, fname)?;
        Ok(tx.set_i32(&self.block, foffset, value, true)?)
    }

    pub fn get_string(
        &self,
        tx: &'tx Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
    ) -> Result<String> {
        let foffset = self.field_offset(slot, fname)?;
        Ok(tx.get_string(&self.block, foffset)?)
    }

    pub fn set_string(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
        value: String,
    ) -> Result<()> {
        let foffset = self.field_offset(slot, fname)?;
        Ok(tx.set_string(&self.block, foffset, &value, true)?)
    }

    pub fn delete(&self, tx: &'tx mut Transaction<'lm, 'bm, 'lt>, slot: i32) -> Result<()> {
        Ok(self.set_flag(tx, slot, SlotFlag::Empty)?)
    }

    pub fn format(&self, tx: &'tx mut Transaction<'lm, 'bm, 'lt>) -> Result<()> {
        let mut slot = 0;
        while self.is_valid_slot(tx, slot) {
            tx.set_i32(
                &self.block,
                self.slot_offset(slot)?,
                SlotFlag::Empty.into(),
                false,
            )?;
            let schema = self.layout.schema();
            for fname in schema.fields_iter() {
                let foffset = self.field_offset(slot, fname)?;
                let ftype = schema.field_type(fname).unwrap();
                if ftype == SqlType::Integer {
                    tx.set_i32(&self.block, foffset, 0, false)?;
                } else {
                    tx.set_string(&self.block, foffset, "", false)?;
                }
            }
            slot += 1;
        }
        Ok(())
    }

    pub fn next_after(
        &self,
        tx: &'tx Transaction<'lm, 'bm, 'lt>,
        slot: Option<i32>,
    ) -> Option<i32> {
        self.search_after(tx, slot, SlotFlag::Used)
    }

    pub fn insert_after(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: Option<i32>,
    ) -> Option<i32> {
        if let Some(newslot) = self.search_after(tx, slot, SlotFlag::Empty) {
            self.set_flag(tx, newslot, SlotFlag::Used).unwrap(); // TODO
            Some(newslot)
        } else {
            None
        }
    }

    fn set_flag(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        flag: SlotFlag,
    ) -> Result<()> {
        Ok(tx.set_i32(&self.block, self.slot_offset(slot)?, flag.into(), true)?)
    }

    fn search_after(
        &self,
        tx: &'tx Transaction<'lm, 'bm, 'lt>,
        slot: Option<i32>,
        flag: SlotFlag,
    ) -> Option<i32> {
        let mut next = slot.map(|s| s + 1).unwrap_or(0);
        let flag_i32: i32 = flag.into();
        while self.is_valid_slot(tx, next) {
            if tx
                .get_i32(&self.block, self.slot_offset(next).unwrap())
                .unwrap()
                == flag_i32
            {
                return Some(next);
            }
            next += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::RecordPage;
    use crate::{
        record::{
            record_page::SlotFlag,
            schema::{Layout, Schema},
        },
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "record_page_test.log");

            let mut schema = Schema::new();
            schema.add_i32_field("A");
            schema.add_string_field("B", 9);
            let layout = Layout::new(schema);

            let tx = db.new_tx();
            {
                let block = tx.borrow_mut().append("record_page_text").unwrap();
                tx.borrow_mut().pin(&block).unwrap();

                let rp = RecordPage::new(block.clone(), &layout);
                rp.format(&mut tx.borrow_mut()).unwrap();

                let mut slot = rp.insert_after(&mut tx.borrow_mut(), None);
                while slot.is_some() {
                    let n = slot.unwrap();
                    rp.set_i32(&mut tx.borrow_mut(), n, "A", n).unwrap();
                    rp.set_string(&mut tx.borrow_mut(), n, "B", format!("rec{}", n))
                        .unwrap();
                    slot = rp.insert_after(&mut tx.borrow_mut(), slot);
                }

                let mut prev_slot = None;
                while let Some(slot) = rp.next_after(&tx.borrow(), prev_slot) {
                    let a = rp.get_i32(&tx.borrow(), slot, "A").unwrap();
                    assert_eq!(a, slot);
                    let b = rp.get_string(&tx.borrow(), slot, "B").unwrap();
                    assert_eq!(b, format!("rec{}", slot));
                    prev_slot = Some(slot);
                }

                let slot_num = db.file_mgr().blocksize() / layout.slotsize();
                let target_slot = (slot_num / 2) as i32;
                rp.delete(&mut tx.borrow_mut(), target_slot).unwrap();

                let prev_slot_a = rp.get_i32(&tx.borrow(), target_slot - 1, "A").unwrap();
                assert_eq!(prev_slot_a, target_slot - 1);
                let next_slot =
                    rp.search_after(&tx.borrow(), Some(target_slot - 1), SlotFlag::Used);
                assert_eq!(next_slot, Some(target_slot + 1));

                tx.borrow_mut().unpin(&block);
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
