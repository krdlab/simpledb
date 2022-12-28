// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::schema::{Layout, SqlType};
use crate::{file::block_id::BlockId, tx::transaction::Transaction};
use std::convert::Into;

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

    fn get_offset(&self, slot: i32) -> usize {
        assert!(slot >= 0);
        (slot as usize) * self.layout.get_slotsize()
    }

    fn is_valid_slot(&self, tx: &'tx mut Transaction<'lm, 'bm, 'lt>, slot: i32) -> bool {
        self.get_offset(slot + 1) <= tx.block_size()
    }

    pub fn get_block(&self) -> &BlockId {
        &self.block
    }

    pub fn get_i32(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
    ) -> Result<i32, crate::tx::transaction::TransactionError> {
        let fpos = self.get_offset(slot) + self.layout.get_offset(fname).unwrap();
        tx.get_i32(&self.block, fpos)
    }

    pub fn set_i32(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
        value: i32,
    ) -> Result<(), crate::tx::transaction::TransactionError> {
        let fpos = self.get_offset(slot) + self.layout.get_offset(fname).unwrap();
        tx.set_i32(&self.block, fpos, value, true)
    }

    pub fn get_string(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
    ) -> Result<String, crate::tx::transaction::TransactionError> {
        let fpos = self.get_offset(slot) + self.layout.get_offset(fname).unwrap();
        tx.get_string(&self.block, fpos)
    }

    pub fn set_string(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        fname: &str,
        value: String,
    ) -> Result<(), crate::tx::transaction::TransactionError> {
        let fpos = self.get_offset(slot) + self.layout.get_offset(fname).unwrap();
        tx.set_string(&self.block, fpos, &value, true)
    }

    pub fn delete(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
    ) -> Result<(), crate::tx::transaction::TransactionError> {
        self.set_flag(tx, slot, SlotFlag::Empty)
    }

    pub fn format(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
    ) -> Result<(), crate::tx::transaction::TransactionError> {
        let mut slot = 0;
        while self.is_valid_slot(tx, slot) {
            tx.set_i32(
                &self.block,
                self.get_offset(slot),
                SlotFlag::Empty.into(),
                false,
            )?;
            let schema = self.layout.get_schema();
            for fname in schema.fields_iter() {
                let fpos = self.get_offset(slot) + self.layout.get_offset(fname).unwrap();
                let ftype = schema.get_type(fname).unwrap();
                if ftype == SqlType::Integer {
                    tx.set_i32(&self.block, fpos, 0, false)?;
                } else {
                    tx.set_string(&self.block, fpos, "", false)?;
                }
            }
            slot += 1;
        }
        Ok(())
    }

    pub fn next_after(&self, tx: &'tx mut Transaction<'lm, 'bm, 'lt>, slot: i32) -> Option<i32> {
        self.search_after(tx, slot, SlotFlag::Used)
    }

    pub fn insert_after(&self, tx: &'tx mut Transaction<'lm, 'bm, 'lt>, slot: i32) -> Option<i32> {
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
    ) -> Result<(), crate::tx::transaction::TransactionError> {
        tx.set_i32(&self.block, self.get_offset(slot), flag.into(), true)
    }

    fn search_after(
        &self,
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        slot: i32,
        flag: SlotFlag,
    ) -> Option<i32> {
        let mut next = slot + 1;
        let flag_i32: i32 = flag.into();
        while self.is_valid_slot(tx, next) {
            if tx.get_i32(&self.block, self.get_offset(next)).unwrap() == flag_i32 {
                return Some(next);
            }
            next += 1;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        record::{
            record_page::SlotFlag,
            schema::{Layout, Schema},
        },
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    use super::RecordPage;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "record_page_test.log");

            let mut schema = Schema::new();
            schema.add_i32_field("A");
            schema.add_string_field("B", 9);
            let layout = Layout::new(schema);

            let mut tx = db.new_tx();
            {
                let block = tx.append("record_page_text").unwrap();
                tx.pin(&block).unwrap();

                let rp = RecordPage::new(block.clone(), &layout);
                rp.format(&mut tx).unwrap();

                let mut prev_slot = -1;
                while let Some(slot) = rp.insert_after(&mut tx, prev_slot) {
                    let n = slot;
                    rp.set_i32(&mut tx, slot, "A", n).unwrap();
                    rp.set_string(&mut tx, slot, "B", format!("rec{}", n))
                        .unwrap();
                    prev_slot = slot;
                }

                prev_slot = -1;
                while let Some(slot) = rp.next_after(&mut tx, prev_slot) {
                    let a = rp.get_i32(&mut tx, slot, "A").unwrap();
                    assert_eq!(a, slot);
                    let b = rp.get_string(&mut tx, slot, "B").unwrap();
                    assert_eq!(b, format!("rec{}", slot));
                    prev_slot = slot;
                }

                let slot_num = db.file_mgr().blocksize() / layout.get_slotsize();
                let target_slot = (slot_num / 2) as i32;
                rp.delete(&mut tx, target_slot).unwrap();

                let prev_slot_a = rp.get_i32(&mut tx, target_slot - 1, "A").unwrap();
                assert_eq!(prev_slot_a, target_slot - 1);
                let next_slot = rp.search_after(&mut tx, target_slot - 1, SlotFlag::Used);
                assert_eq!(next_slot, Some(target_slot + 1));

                tx.unpin(&block);
            }
            tx.commit().unwrap();
        }
        dir.close().unwrap();
    }
}
