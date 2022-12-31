// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    record_page::RecordPage,
    schema::{Layout, SqlType},
};
use crate::{file::block_id::BlockId, tx::transaction::Transaction};
use std::fmt::Display;

#[derive(Debug, PartialEq, Eq)]
pub struct RID {
    blknum: i64,
    slot: i32,
}

impl RID {
    pub fn new(blknum: i64, slot: i32) -> Self {
        RID { blknum, slot }
    }

    pub fn block_number(&self) -> i64 {
        self.blknum
    }

    pub fn slot(&self) -> i32 {
        self.slot
    }
}

impl Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.blknum, self.slot)
    }
}

pub struct TableScan<'tx, 'lm, 'bm, 'lt, 'ly> {
    tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
    layout: &'ly Layout,
    filename: String,
    rp: RecordPage<'ly>,
    current_slot: Option<i32>,
}
// TODO: impl UpdateScan for TableScan

#[derive(Debug)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl<'tx, 'lm, 'bm, 'lt, 'ly> TableScan<'tx, 'lm, 'bm, 'lt, 'ly> {
    pub fn new(
        tx: &'tx mut Transaction<'lm, 'bm, 'lt>,
        tblname: &str,
        layout: &'ly Layout,
    ) -> Self {
        let filename = format!("{tblname}.tbl");
        let rp = if tx.size(&filename).unwrap() == 0 {
            let block = tx.append(&filename).unwrap(); // TODO
            tx.pin(&block).unwrap(); // TODO
            let rp = RecordPage::new(block, layout);
            rp.format(tx).unwrap(); // TODO
            rp
        } else {
            let block = BlockId::new(&filename, 0);
            tx.pin(&block).unwrap(); // TODO
            RecordPage::new(block, layout)
        };

        Self {
            tx,
            layout,
            filename,
            rp,
            current_slot: None,
        }
    }

    pub fn close(&mut self) {
        self.tx.unpin(self.rp.get_block());
    }

    fn move_to_block(&mut self, blknum: i64) {
        self.close();
        let block = BlockId::new(&self.filename, blknum);
        self.tx.pin(&block).unwrap(); // TODO
        self.rp = RecordPage::new(block, self.layout);
        self.current_slot = None;
    }

    fn move_to_new_block(&mut self) {
        self.close();
        let block = self.tx.append(&self.filename).unwrap(); // TODO
        self.tx.pin(&block).unwrap(); // TODO
        self.rp = RecordPage::new(block, self.layout);
        self.rp.format(self.tx).unwrap(); // TODO
        self.current_slot = None;
    }

    pub fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn as_last_block(&mut self) -> bool {
        self.rp.get_block().number() as u64 == self.tx.size(&self.filename).unwrap() - 1
    }

    pub fn next(&mut self) -> bool {
        self.current_slot = self.rp.next_after(self.tx, self.current_slot);
        while self.current_slot.is_none() {
            if self.as_last_block() {
                return false;
            }
            self.move_to_block(self.rp.get_block().number() + 1);
            self.current_slot = self.rp.next_after(self.tx, self.current_slot);
        }
        true
    }

    pub fn get_i32(&mut self, fname: &str) -> i32 {
        let slot = self.current_slot.as_ref().unwrap();
        self.rp.get_i32(self.tx, *slot, fname).unwrap()
    }

    pub fn get_string(&mut self, fname: &str) -> String {
        let slot = self.current_slot.as_ref().unwrap();
        self.rp.get_string(self.tx, *slot, fname).unwrap()
    }

    pub fn get_val(&mut self, fname: &str) -> Constant {
        if self.layout.schema().field_type(fname).unwrap() == SqlType::Integer {
            return Constant::Int(self.get_i32(fname));
        } else {
            return Constant::String(self.get_string(fname));
        }
    }

    pub fn has_field(&self, fname: &str) -> bool {
        self.layout.schema().has_field(fname)
    }

    pub fn set_i32(&mut self, fname: &str, val: i32) {
        let slot = self.current_slot.as_ref().unwrap();
        self.rp.set_i32(self.tx, *slot, fname, val);
    }

    pub fn set_string(&mut self, fname: &str, val: String) {
        let slot = self.current_slot.as_ref().unwrap();
        self.rp.set_string(self.tx, *slot, fname, val);
    }

    pub fn set_val(&mut self, fname: &str, val: Constant) {
        let ftype = self.layout.schema().field_type(fname);
        match val {
            Constant::Int(v) if ftype == Some(SqlType::Integer) => self.set_i32(fname, v),
            Constant::String(v) if ftype == Some(SqlType::VarChar) => self.set_string(fname, v),
            _ => panic!("mismatched type: fname={fname}, val={val:?}"),
        }
    }

    pub fn insert(&mut self) {
        self.current_slot = self.rp.insert_after(self.tx, self.current_slot);
        while self.current_slot.is_none() {
            if self.as_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.rp.get_block().number() + 1);
            }
            self.current_slot = self.rp.insert_after(self.tx, self.current_slot);
        }
    }

    pub fn delete(&mut self) {
        let slot = self.current_slot.as_ref().unwrap();
        self.rp.delete(self.tx, *slot).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::TableScan;
    use crate::{
        record::schema::{Layout, Schema},
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "table_scan_test.log");

            let mut schema = Schema::new();
            schema.add_i32_field("A");
            schema.add_string_field("B", 9);
            let layout = Layout::new(schema);

            let mut tx = db.new_tx();
            {
                let mut ts = TableScan::new(&mut tx, "T", &layout);
                for i in 0..50 {
                    ts.insert();
                    ts.set_i32("A", i);
                    ts.set_string("B", format!("rec{i}"));
                }

                let mut i = 0;
                ts.before_first();
                while ts.next() {
                    assert_eq!(ts.get_i32("A"), i);
                    assert_eq!(ts.get_string("B"), format!("rec{i}"));
                    i += 1;
                }
                assert_eq!(i, 50);

                i = 1;
                ts.before_first();
                while ts.next() {
                    ts.delete();
                    i += 2;
                }

                i = 0;
                ts.before_first();
                while ts.next() {
                    assert_eq!(ts.get_i32("A"), i);
                    assert_eq!(ts.get_string("B"), format!("rec{i}"));
                    i += 2;
                }
                ts.close();
            }
            tx.commit().unwrap();
        }
    }
}
