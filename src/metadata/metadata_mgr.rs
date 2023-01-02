// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    index_mgr::{IndexInfo, IndexMgr},
    stat_mgr::{StatInfo, StatMgr},
    table_mgr::TableMgr,
    view_mgr::ViewMgr,
};
use crate::{
    record::schema::{Layout, Schema},
    tx::transaction::Transaction,
};
use std::{collections::HashMap, sync::Arc};

pub struct MetadataMgr {
    tm: Arc<TableMgr>,
    vm: Arc<ViewMgr>,
    sm: Arc<StatMgr>,
    im: Arc<IndexMgr>,
}

impl MetadataMgr {
    pub fn new(is_new: bool, tx: &mut Transaction) -> Self {
        let tm = Arc::new(TableMgr::new());
        if is_new {
            tm.init(tx);
        }

        let vm = Arc::new(ViewMgr::new(tm.clone()));
        if is_new {
            vm.init(tx);
        }

        let sm = Arc::new(StatMgr::new(tm.clone()));
        if is_new {
            sm.init(tx);
        }

        let im = Arc::new(IndexMgr::new(is_new, tm.clone(), sm.clone(), tx));

        Self { tm, vm, sm, im }
    }

    pub fn create_table(&self, table_name: &str, schema: Schema, tx: &mut Transaction) {
        self.tm.create_table(table_name, schema, tx);
    }

    pub fn table_layout(&self, table_name: &str, tx: &mut Transaction) -> Option<Layout> {
        self.tm.layout(table_name, tx)
    }

    pub fn create_view(&self, view_name: &str, view_def: &str, tx: &mut Transaction) {
        self.vm.create_view(view_name, view_def, tx);
    }

    pub fn view_def(&self, view_name: &str, tx: &mut Transaction) -> Option<String> {
        self.vm.view_def(view_name, tx)
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        tx: &mut Transaction,
    ) {
        self.im.create_index(index_name, table_name, field_name, tx);
    }

    pub fn table_index_info(
        &self,
        table_name: &str,
        tx: &mut Transaction,
    ) -> HashMap<String, IndexInfo> {
        self.im.index_info(table_name, tx)
    }

    pub fn table_stat_info(
        &self,
        table_name: &str,
        layout: &Layout,
        tx: &mut Transaction,
    ) -> StatInfo {
        self.sm.table_stat_info(table_name, layout, tx)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        record::{
            schema::{Schema, SqlType},
            table_scan::TableScan,
        },
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let mut db = SimpleDB::new_for_test(
                dir.path().join("testdb").as_path(),
                "metadata_mgr_test.log",
            );
            db.init();

            let mm = db.metadata_mgr();
            {
                let mut tx = db.new_tx();
                {
                    let mut schema = Schema::new();
                    schema.add_i32_field("A");
                    schema.add_string_field("B", 9);

                    {
                        // part 1: table metadata
                        mm.create_table("MyTable", schema, &mut tx);
                        let layout = mm.table_layout("MyTable", &mut tx).unwrap();
                        assert_eq!(layout.slotsize(), 4 + 4 + (4 + 9 * 4));

                        let schema = layout.schema();
                        let fields: Vec<&String> = schema.fields_iter().collect();
                        assert_eq!(fields, vec!["A", "B"]);
                        assert_eq!(schema.field_type("A").unwrap(), SqlType::Integer);
                        assert_eq!(schema.field_type("B").unwrap(), SqlType::VarChar);
                    }
                    {
                        // part 2: statistics metadata
                        let layout = mm.table_layout("MyTable", &mut tx).unwrap();
                        {
                            let mut ts = TableScan::new(&mut tx, "MyTable", &layout);
                            for i in 0..50 {
                                ts.insert();
                                ts.set_i32("A", i);
                                ts.set_string("B", format!("rec{i}"));
                            }
                        }
                        let stat = mm.table_stat_info("MyTable", &layout, &mut tx);
                        assert_eq!(
                            stat.blocks_accessed(),
                            layout.slotsize() * 50 / tx.block_size() + 1
                        );
                        assert_eq!(stat.records_output(), 50);
                        assert_eq!(stat.distinct_values("A"), 50 / 3 + 1);
                        assert_eq!(stat.distinct_values("B"), 50 / 3 + 1);
                    }
                    {
                        // part 3: view metadata
                        let view_def = "SELECT b FROM MyTable WHERE A = 1";
                        mm.create_view("viewA", view_def, &mut tx);
                        let result = mm.view_def("viewA", &mut tx).unwrap();
                        assert_eq!(result, view_def);
                    }
                    {
                        // part 4: index metadata
                        let layout = mm.table_layout("MyTable", &mut tx).unwrap();
                        let stat = mm.table_stat_info("MyTable", &layout, &mut tx);

                        mm.create_index("indexA", "MyTable", "A", &mut tx);
                        mm.create_index("indexB", "MyTable", "B", &mut tx);
                        let indexes = mm.table_index_info("MyTable", &mut tx);
                        assert_eq!(indexes.len(), 2);
                        {
                            let index_a = indexes.get("A").unwrap();
                            // TODO: assert_eq!(index_a.blocks_accessed(), )
                            assert_eq!(
                                index_a.records_output(),
                                stat.records_output() / stat.distinct_values("A")
                            );
                            assert_eq!(index_a.distinct_values("A"), 1);
                            assert_eq!(index_a.distinct_values("B"), stat.distinct_values("B"));
                        }
                        {
                            let index_b = indexes.get("B").unwrap();
                            // TODO: assert_eq!(index_b.blocks_accessed(), )
                            assert_eq!(
                                index_b.records_output(),
                                stat.records_output() / stat.distinct_values("B")
                            );
                            assert_eq!(index_b.distinct_values("A"), stat.distinct_values("A"));
                            assert_eq!(index_b.distinct_values("B"), 1);
                        }
                    }
                }
                tx.commit().unwrap();
            }
        }
    }
}
