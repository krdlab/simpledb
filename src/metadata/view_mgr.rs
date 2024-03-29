// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::common::{MetadataError, Result};
use super::table_mgr::{TableMgr, MAX_NAME_LENGTH};
use crate::{
    record::{schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc, sync::Arc};

pub struct ViewMgr {
    tm: Arc<TableMgr>,
}

const VIEW_CATALOG_TABLE_NAME: &str = "viewcat";
const MAX_VIEW_DEF: usize = 100; // CAVEAT: The DB BLOCK_SIZE must be sufficiently larger than four times this value.

impl ViewMgr {
    pub fn new(tm: Arc<TableMgr>) -> Self {
        Self { tm }
    }

    pub fn init(&self, tx: Rc<RefCell<Transaction>>) {
        let mut schema = Schema::new();
        schema.add_string_field("viewname", MAX_NAME_LENGTH);
        schema.add_string_field("viewdef", MAX_VIEW_DEF);
        self.tm
            .create_table(VIEW_CATALOG_TABLE_NAME, schema, tx)
            .unwrap();
    }

    pub fn create_view(&self, vname: &str, vdef: &str, tx: Rc<RefCell<Transaction>>) -> Result<()> {
        let layout = self.tm.layout(VIEW_CATALOG_TABLE_NAME, tx.clone())?;
        let mut ts = TableScan::new(tx, VIEW_CATALOG_TABLE_NAME.into(), layout);
        ts.insert()?;
        ts.set_string("viewname", vname.into())?;
        ts.set_string("viewdef", vdef.into())?;
        Ok(())
    }

    pub fn view_def(&self, vname: &str, tx: Rc<RefCell<Transaction>>) -> Result<String> {
        let layout = self.tm.layout(VIEW_CATALOG_TABLE_NAME, tx.clone())?;
        let mut ts = TableScan::new(tx, VIEW_CATALOG_TABLE_NAME.into(), layout);
        while ts.next()? {
            if let Ok(vn) = ts.get_string("viewname") {
                if vn == vname {
                    return Ok(ts.get_string("viewdef").unwrap());
                }
            }
        }
        Err(MetadataError::ViewNotFound(vname.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::ViewMgr;
    use crate::{metadata::table_mgr::TableMgr, server::simple_db::SimpleDB};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "view_mgr_test.log");
            let tx = db.new_tx();
            {
                let tm = Arc::new(TableMgr::new());
                tm.init(tx.clone());
                let vm = ViewMgr::new(tm.clone());
                vm.init(tx.clone());

                vm.create_view("FirstView", "SELECT * FROM t", tx.clone())
                    .unwrap();
                vm.create_view(
                    "MyView",
                    "SELECT qty, price, qty*price AS value FROM t",
                    tx.clone(),
                )
                .unwrap();

                let viewdef = vm.view_def("MyView", tx.clone()).unwrap();
                assert_eq!(viewdef, "SELECT qty, price, qty*price AS value FROM t");
            }
            tx.borrow_mut().commit().unwrap();
        }
    }
}
