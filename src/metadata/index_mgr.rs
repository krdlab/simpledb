// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    common::Result,
    stat_mgr::{StatInfo, StatMgr},
    table_mgr::{TableMgr, MAX_NAME_LENGTH},
};
use crate::{
    index::{hash::HashIndex, Index},
    record::{
        schema::{Layout, Schema, SqlType},
        table_scan::TableScan,
    },
    tx::transaction::Transaction,
};
use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

pub struct IndexInfo {
    index_name: String,
    field_name: String,
    _table_schema: Schema,
    block_size: usize,
    index_layout: Layout,
    stat_info: StatInfo,
}

impl IndexInfo {
    pub fn new(
        index_name: &str,
        field_name: &str,
        table_schema: Schema,
        block_size: usize,
        stat_info: StatInfo,
    ) -> Self {
        let index_layout = IndexInfo::create_index_layout(&table_schema, field_name);
        Self {
            index_name: index_name.into(),
            field_name: field_name.into(),
            _table_schema: table_schema,
            block_size,
            index_layout,
            stat_info,
        }
    }

    fn create_index_layout(table_schema: &Schema, field_name: &str) -> Layout {
        let mut schema = Schema::new();
        schema.add_i32_field("block");
        schema.add_i32_field("id");

        if table_schema.field_type(field_name).unwrap() == SqlType::Integer {
            schema.add_i32_field("dataval");
        } else {
            let flength = table_schema.field_length(field_name).unwrap();
            schema.add_string_field("dataval", flength);
        }

        Layout::new(schema)
    }

    pub fn open<'lm, 'bm>(&self) -> impl Index<'lm, 'bm> {
        let index_name = self.index_name.to_owned();
        let index_layout = self.index_layout.to_owned();
        HashIndex::new(index_name, index_layout)
    }

    pub fn blocks_accessed(&self) -> usize {
        let rec_per_blk = self.block_size / self.index_layout.slotsize();
        let num_blocks = self.stat_info.records_output() / rec_per_blk;
        HashIndex::search_cost(num_blocks, rec_per_blk)
    }

    pub fn records_output(&self) -> usize {
        self.stat_info.records_output() / self.stat_info.distinct_values(&self.field_name)
    }

    pub fn distinct_values(&self, fname: &str) -> usize {
        if self.field_name == fname {
            1
        } else {
            self.stat_info.distinct_values(&self.field_name)
        }
    }
}

const INDEX_CATALOG_TABLE_NAME: &str = "idxcat";

pub struct IndexMgr {
    // layout: Layout,
    tm: Arc<TableMgr>,
    sm: Arc<StatMgr>,
}

impl IndexMgr {
    pub fn new(tm: Arc<TableMgr>, sm: Arc<StatMgr>) -> Self {
        Self { tm, sm }
    }

    pub fn init(&self, tx: Rc<RefCell<Transaction>>) {
        let mut schema = Schema::new();
        schema.add_string_field("indexname", MAX_NAME_LENGTH);
        schema.add_string_field("tablename", MAX_NAME_LENGTH);
        schema.add_string_field("fieldname", MAX_NAME_LENGTH);
        self.tm
            .create_table(INDEX_CATALOG_TABLE_NAME, schema, tx)
            .unwrap();
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<()> {
        let layout = self.index_catalog_layout(&tx)?;
        let mut ts = TableScan::new(tx, INDEX_CATALOG_TABLE_NAME.into(), layout);
        ts.insert()?;
        ts.set_string("indexname", index_name.into())?;
        ts.set_string("tablename", table_name.into())?;
        ts.set_string("fieldname", field_name.into())?;
        Ok(())
    }

    fn index_catalog_layout(&self, tx: &Rc<RefCell<Transaction>>) -> Result<Layout> {
        self.tm.layout(INDEX_CATALOG_TABLE_NAME, tx.clone())
    }

    pub fn index_info(
        &self,
        table_name: String,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<HashMap<String, IndexInfo>> {
        let mut result = HashMap::new();

        let idx_fld_pairs = {
            let tblname: String = table_name.clone();
            let mut names = Vec::new();

            let layout = self.index_catalog_layout(&tx)?;
            let mut ts = TableScan::new(tx.clone(), INDEX_CATALOG_TABLE_NAME.into(), layout);
            while ts.next()? {
                if ts.get_string("tablename").unwrap() == tblname {
                    names.push((
                        ts.get_string("indexname").unwrap(),
                        ts.get_string("fieldname").unwrap(),
                    ));
                }
            }

            names
        };

        for (idxname, fldname) in idx_fld_pairs {
            let tbl_layout = self.tm.layout(&table_name, tx.clone()).unwrap();
            let tbl_stat_info =
                self.sm
                    .table_stat_info(&table_name, tbl_layout.clone(), tx.clone());
            let index_info = IndexInfo::new(
                &idxname,
                &fldname,
                tbl_layout.schema().clone(),
                tx.borrow().block_size(),
                tbl_stat_info,
            );
            result.insert(fldname, index_info);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::IndexMgr;
    use crate::{
        metadata::{stat_mgr::StatMgr, table_mgr::TableMgr},
        record::schema::Schema,
        server::simple_db::SimpleDB,
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "index_mgr_test.log");
            let tx = db.new_tx();
            {
                let tm = Arc::new(TableMgr::new());
                tm.init(tx.clone());
                let sm = Arc::new(StatMgr::new(tm.clone()));
                sm.init(tx.clone());
                {
                    let mut schema = Schema::new();
                    schema.add_i32_field("id");
                    tm.create_table("MyTable", schema, tx.clone()).unwrap();
                }

                let im = IndexMgr::new(tm.clone(), sm.clone());
                im.init(tx.clone());
                im.create_index("my-index", "MyTable", "id", tx.clone())
                    .unwrap();

                let ii_map = im.index_info("MyTable".into(), tx.clone()).unwrap();
                assert_eq!(ii_map.len(), 1);

                let id = ii_map.get("id").unwrap();
                assert_eq!(id.index_name, "my-index");
            }
            tx.borrow_mut().commit().unwrap();
        }
    }
}
