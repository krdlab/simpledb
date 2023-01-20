// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{cell::RefCell, rc::Rc};

use super::{
    record_page::RecordPage,
    schema::{Layout, SqlType},
};
use crate::{
    file::block_id::BlockId,
    query::{
        predicate::Constant,
        scan::{Result, Scan, UpdateScan, RID},
    },
    tx::transaction::Transaction,
};

pub struct TableScan<'lm, 'bm, 'lt, 'ly> {
    tx: Rc<RefCell<Transaction<'lm, 'bm, 'lt>>>,
    layout: &'ly Layout,
    filename: String,
    rp: RecordPage<'ly>,
    current_slot: Option<i32>,
}

impl<'tx, 'lm, 'bm, 'lt, 'ly> TableScan<'lm, 'bm, 'lt, 'ly> {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm, 'lt>>>,
        tblname: &str,
        layout: &'ly Layout,
    ) -> Self {
        let filename = format!("{tblname}.tbl");
        let rp = {
            let mut tx = tx.borrow_mut();
            if tx.size(&filename).unwrap() == 0 {
                let block = tx.append(&filename).unwrap(); // TODO
                tx.pin(&block).unwrap(); // TODO
                let rp = RecordPage::new(block, layout);
                rp.format(&mut *tx).unwrap(); // TODO
                rp
            } else {
                let block = BlockId::new(&filename, 0);
                tx.pin(&block).unwrap(); // TODO
                RecordPage::new(block, layout)
            }
        };

        Self {
            tx,
            layout,
            filename,
            rp,
            current_slot: None,
        }
    }

    fn close(&mut self) {
        self.tx.borrow_mut().unpin(self.rp.block());
    }

    fn move_to_block(&mut self, blknum: i64) {
        self.close();
        let block = BlockId::new(&self.filename, blknum);
        self.tx.borrow_mut().pin(&block).unwrap(); // TODO
        self.rp = RecordPage::new(block, self.layout);
        self.current_slot = None;
    }

    fn move_to_new_block(&mut self) {
        self.close();
        {
            let mut tx = self.tx.borrow_mut();
            let block = tx.append(&self.filename).unwrap(); // TODO
            tx.pin(&block).unwrap(); // TODO
            self.rp = RecordPage::new(block, self.layout);
            self.rp.format(&mut *tx).unwrap(); // TODO
        }
        self.current_slot = None;
    }

    pub fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn as_last_block(&self) -> bool {
        self.rp.block().number() as u64 == self.tx.borrow().size(&self.filename).unwrap() - 1
    }

    pub fn next(&mut self) -> bool {
        // let tx = self.tx.borrow();
        self.current_slot = self.rp.next_after(&self.tx.borrow(), self.current_slot);
        while self.current_slot.is_none() {
            if self.as_last_block() {
                return false;
            }
            self.move_to_block(self.rp.block().number() + 1);
            self.current_slot = self.rp.next_after(&self.tx.borrow(), self.current_slot);
        }
        true
    }

    pub fn get_i32(&self, fname: &str) -> Result<i32> {
        let slot = self.current_slot.as_ref().unwrap();
        Ok(self.rp.get_i32(&*self.tx.borrow(), *slot, fname)?)
    }

    pub fn get_string(&self, fname: &str) -> Result<String> {
        let slot = self.current_slot.as_ref().unwrap();
        Ok(self.rp.get_string(&*self.tx.borrow(), *slot, fname)?)
    }

    pub fn get_val(&self, fname: &str) -> Result<Constant> {
        if self.layout.schema().field_type(fname).unwrap() == SqlType::Integer {
            self.get_i32(fname).map(Constant::Int)
        } else {
            self.get_string(fname).map(Constant::String)
        }
    }

    pub fn has_field(&self, fname: &str) -> bool {
        self.layout.schema().has_field(fname)
    }

    pub fn set_i32(&mut self, fname: &str, val: i32) -> Result<()> {
        let slot = self.current_slot.as_ref().unwrap();
        Ok(self
            .rp
            .set_i32(&mut *self.tx.borrow_mut(), *slot, fname, val)?)
    }

    pub fn set_string(&mut self, fname: &str, val: String) -> Result<()> {
        let slot = self.current_slot.as_ref().unwrap();
        Ok(self
            .rp
            .set_string(&mut *self.tx.borrow_mut(), *slot, fname, val)?)
    }

    pub fn set_val(&mut self, fname: &str, val: Constant) -> Result<()> {
        let ftype = self.layout.schema().field_type(fname);
        match val {
            Constant::Int(v) if ftype == Some(SqlType::Integer) => self.set_i32(fname, v),
            Constant::String(v) if ftype == Some(SqlType::VarChar) => self.set_string(fname, v),
            _ => panic!("mismatched type: fname={fname}, val={val:?}"),
        }
    }

    pub fn insert(&mut self) {
        self.current_slot = self
            .rp
            .insert_after(&mut self.tx.borrow_mut(), self.current_slot);
        while self.current_slot.is_none() {
            if self.as_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.rp.block().number() + 1);
            }
            self.current_slot = self
                .rp
                .insert_after(&mut self.tx.borrow_mut(), self.current_slot);
        }
    }

    pub fn delete(&mut self) -> Result<()> {
        if let Some(slot) = self.current_slot.as_ref() {
            Ok(self.rp.delete(&mut *self.tx.borrow_mut(), *slot)?)
        } else {
            Ok(())
        }
    }

    pub fn move_to_rid(&mut self, rid: RID) -> Result<()> {
        self.close();
        let block = BlockId::new(&self.filename, rid.block_number());
        self.tx.borrow_mut().pin(&block)?;
        self.rp = RecordPage::new(block, self.layout);
        self.current_slot = rid.slot();
        Ok(())
    }

    pub fn current_rid(&self) -> RID {
        RID::new(self.rp.block().number(), self.current_slot)
    }
}

impl<'tx, 'lm, 'bm, 'lt, 'ly> Scan for TableScan<'lm, 'bm, 'lt, 'ly> {
    fn before_first(&mut self) {
        TableScan::before_first(self);
    }

    fn next(&mut self) -> bool {
        TableScan::next(self)
    }

    fn get_i32(&self, field_name: &str) -> crate::query::scan::Result<i32> {
        TableScan::get_i32(self, field_name)
    }

    fn get_string(&self, field_name: &str) -> crate::query::scan::Result<String> {
        TableScan::get_string(self, field_name)
    }

    fn get_val(&self, field_name: &str) -> crate::query::scan::Result<Constant> {
        TableScan::get_val(self, field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        TableScan::has_field(self, field_name)
    }

    fn close(&mut self) {
        TableScan::close(self);
    }
}

impl<'tx, 'lm, 'bm, 'lt, 'ly> UpdateScan for TableScan<'lm, 'bm, 'lt, 'ly> {
    fn set_val(&mut self, field_name: &str, value: Constant) -> crate::query::scan::Result<()> {
        TableScan::set_val(self, field_name, value)
    }

    fn set_i32(&mut self, field_name: &str, value: i32) -> crate::query::scan::Result<()> {
        TableScan::set_i32(self, field_name, value)
    }

    fn set_string(&mut self, field_name: &str, value: String) -> crate::query::scan::Result<()> {
        TableScan::set_string(self, field_name, value)
    }

    fn insert(&mut self) -> crate::query::scan::Result<()> {
        TableScan::insert(self);
        Ok(())
    }

    fn delete(&mut self) -> crate::query::scan::Result<()> {
        TableScan::delete(self)
    }

    fn get_rid(&self) -> RID {
        TableScan::current_rid(self)
    }

    fn move_to_rid(&mut self, rid: RID) -> crate::query::scan::Result<()> {
        TableScan::move_to_rid(self, rid)
    }
}

impl<'tx, 'lm, 'bm, 'lt, 'ly> Drop for TableScan<'lm, 'bm, 'lt, 'ly> {
    fn drop(&mut self) {
        self.close();
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

            let tx = db.new_tx();
            {
                let mut ts = TableScan::new(tx.clone(), "T", &layout);
                for i in 0..50 {
                    ts.insert();
                    ts.set_i32("A", i).unwrap();
                    ts.set_string("B", format!("rec{i}")).unwrap();
                }

                let mut i = 0;
                ts.before_first();
                while ts.next() {
                    assert_eq!(ts.get_i32("A").unwrap(), i);
                    assert_eq!(ts.get_string("B").unwrap(), format!("rec{i}"));
                    i += 1;
                }
                assert_eq!(i, 50);

                i = 1;
                ts.before_first();
                while ts.next() {
                    ts.delete().unwrap();
                    i += 2;
                }

                i = 0;
                ts.before_first();
                while ts.next() {
                    assert_eq!(ts.get_i32("A").unwrap(), i);
                    assert_eq!(ts.get_string("B").unwrap(), format!("rec{i}"));
                    i += 2;
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
