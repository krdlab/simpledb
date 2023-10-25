// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::Index;
use crate::{
    query::{
        predicate::Constant,
        scan::{Scan, RID},
    },
    record::{schema::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};
use std::hash::{Hash, Hasher};
use std::{cell::RefCell, collections::hash_map::DefaultHasher, rc::Rc};

pub struct HashIndex<'lm, 'bm> {
    name: String,
    layout: Layout,
    search_key: Option<Constant>,
    ts: Option<TableScan<'lm, 'bm>>,
}

impl<'lm, 'bm> HashIndex<'lm, 'bm> {
    const NUM_BUCKETS: u64 = 100;

    pub fn new(name: String, layout: Layout) -> Self {
        Self {
            name,
            layout,
            search_key: None,
            ts: None,
        }
    }

    fn hash_code(obj: &Constant) -> u64 {
        let mut hasher = DefaultHasher::new();
        obj.hash(&mut hasher);
        hasher.finish()
    }

    fn next_record(ts: &mut TableScan<'_, '_>, search_key: &Constant) -> super::Result<bool> {
        while ts.next()? {
            if ts.get_val("dataval")? == *search_key {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn current_rid(ts: &TableScan<'_, '_>) -> super::Result<RID> {
        let block_num = ts.get_i32("block")?;
        let id = ts.get_i32("id")?;
        Ok(RID::from_index(block_num, id))
    }

    pub fn search_cost(num_blocks: usize, rpb: usize) -> usize {
        num_blocks / rpb
    }
}

impl<'lm, 'bm> Index<'lm, 'bm> for HashIndex<'lm, 'bm> {
    fn before_first(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        search_key: crate::query::predicate::Constant,
    ) {
        self.close();
        let bucket = HashIndex::hash_code(&search_key) % HashIndex::NUM_BUCKETS;
        self.search_key = Some(search_key);
        let table_name = format!("{}{}", self.name, bucket);
        self.ts = Some(TableScan::new(tx, table_name, self.layout.clone()));
    }

    fn next(&mut self) -> super::Result<bool> {
        let ts = self.ts.as_mut().expect("before_first is not called");
        let search_key = self.search_key.as_ref().unwrap();
        HashIndex::next_record(ts, search_key)
    }

    fn rid(&self) -> super::Result<RID> {
        let ts = self.ts.as_ref().expect("before_first is not called");
        HashIndex::current_rid(ts)
    }

    fn insert(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: crate::query::predicate::Constant,
        rid: crate::query::scan::RID,
    ) -> super::Result<()> {
        self.before_first(tx, val.clone());

        let ts = self.ts.as_mut().unwrap();
        ts.insert()?;
        ts.set_i32("block", rid.block_number_as_i32())?;
        ts.set_i32("id", rid.slot().unwrap())?;
        ts.set_val("dataval", val)?;
        Ok(())
    }

    fn delete(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: crate::query::predicate::Constant,
        rid: crate::query::scan::RID,
    ) -> super::Result<()> {
        self.before_first(tx, val);

        let ts = self.ts.as_mut().unwrap();
        let search_key = self.search_key.as_ref().unwrap();
        while HashIndex::next_record(ts, search_key)? {
            if HashIndex::current_rid(ts)? == rid {
                ts.delete()?;
                break;
            }
        }
        Ok(())
    }

    fn close(&mut self) {
        if let Some(ts) = self.ts.as_mut() {
            ts.close();
            self.ts = None;
            self.search_key = None;
        }
    }
}

impl<'lm, 'bm> Drop for HashIndex<'lm, 'bm> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        index::Index,
        plan::plan::{Plan, TablePlan},
        query::predicate::Constant,
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let mut db = SimpleDB::new_for_test(dir.path(), "hash_index_test.log");
            db.init();

            let mdm = db.metadata_mgr();
            let planner = db.planner();
            let tx = db.new_tx();
            {
                // 1. prepare table 'T'
                {
                    planner
                        .execute_update("create table T (A int, B varchar(9))", tx.clone())
                        .unwrap();
                    for i in 0..50 {
                        let cmd = format!("insert into T (A, B) values ({i}, 'rec{i}')");
                        planner.execute_update(&cmd, tx.clone()).unwrap();
                    }
                    planner
                        .execute_update("create index T_A_idx on T (A)", tx.clone())
                        .unwrap();
                }

                // 2. retrieve T's records
                {
                    let table_name = "t"; // NOTE: tokenizer is lower case mode

                    let tp = TablePlan::new(tx.clone(), table_name, mdm.clone());
                    let mut ts = tp.open(tx.clone());

                    let indexes = mdm.table_index_info(table_name, tx.clone()).unwrap();
                    {
                        let info = indexes.get("a".into()).unwrap();
                        let mut index = info.open();
                        index.before_first(tx.clone(), Constant::Int(20));
                        while index.next().unwrap() {
                            let rid = index.rid().unwrap();
                            ts.move_to_rid(rid).unwrap();
                            assert_eq!(ts.get_string("b").unwrap(), "rec20");
                        }
                    }
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
    }
}
