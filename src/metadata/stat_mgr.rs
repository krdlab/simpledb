// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::table_mgr::{TableMgr, TABLE_CATALOG_TABLE_NAME, TABLE_NAME_FIELD};
use crate::{
    record::{schema::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatInfo {
    num_blocks: usize,
    num_records: usize,
}

impl StatInfo {
    pub fn new(num_blocks: usize, num_records: usize) -> Self {
        Self {
            num_blocks,
            num_records,
        }
    }

    pub fn blocks_accessed(&self) -> usize {
        self.num_blocks
    }

    pub fn records_output(&self) -> usize {
        self.num_records
    }

    pub fn distinct_values(&self, _field_name: &str) -> usize {
        1 + self.num_records / 3 // NOTE: this is widely inaccurate
    }
}

pub struct StatMgrData {
    tm: Arc<TableMgr>,
    table_stats: HashMap<String, StatInfo>,
    num_calls: usize,
}
pub struct StatMgr {
    data: Mutex<StatMgrData>,
}

impl StatMgrData {
    pub(crate) fn refresh_statistics(&mut self, tx: &mut Transaction) {
        self.table_stats.clear();
        self.num_calls = 0;

        let mut table_names: Vec<String> = Vec::new();
        {
            let layout = self.tm.layout(TABLE_CATALOG_TABLE_NAME, tx).unwrap();
            let mut tcat = TableScan::new(tx, TABLE_CATALOG_TABLE_NAME, &layout);
            while tcat.next() {
                let table_name = tcat.get_string(TABLE_NAME_FIELD).unwrap();
                table_names.push(table_name);
            }
        }
        for table_name in table_names {
            if let Some(layout) = self.tm.layout(&table_name, tx) {
                let stats = StatMgrData::calc_table_stats(&table_name, &layout, tx);
                self.table_stats.insert(table_name, stats);
            }
        }
    }

    pub(crate) fn get_or_create_table_stat_info(
        &mut self,
        table_name: &str,
        layout: &Layout,
        tx: &mut Transaction,
    ) -> StatInfo {
        let si = match self.table_stats.entry(table_name.into()) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(ve) => {
                let si = StatMgrData::calc_table_stats(table_name, layout, tx);
                ve.insert(si)
            }
        };
        si.clone()
    }

    pub(crate) fn calc_table_stats(
        table_name: &str,
        layout: &Layout,
        tx: &mut Transaction,
    ) -> StatInfo {
        let mut num_records = 0;
        let mut num_blocks = 0;

        let mut ts = TableScan::new(tx, table_name.into(), layout);
        while ts.next() {
            num_records += 1;
            num_blocks = ts.current_rid().block_number() + 1;
        }

        StatInfo {
            num_blocks: num_blocks.try_into().unwrap(),
            num_records,
        }
    }
}

const STATS_REFRESH_THRESHOLD: usize = 100;

impl StatMgr {
    pub fn new(tm: Arc<TableMgr>) -> Self {
        Self {
            data: Mutex::new(StatMgrData {
                tm,
                table_stats: HashMap::new(),
                num_calls: 0,
            }),
        }
    }

    pub fn init(&self, tx: &mut Transaction) {
        let mut data = self.data.lock().unwrap();
        data.refresh_statistics(tx);
    }

    pub fn table_stat_info(
        &self,
        table_name: &str,
        layout: &Layout,
        tx: &mut Transaction,
    ) -> StatInfo {
        let mut data = self.data.lock().unwrap();

        data.num_calls += 1;
        if data.num_calls > STATS_REFRESH_THRESHOLD {
            data.refresh_statistics(tx);
        }

        // FIXME: If TableMgr has the specified table records, StatMgr should create a StatInfo. If not, should not create it.
        data.get_or_create_table_stat_info(table_name, layout, tx)
    }
}

#[cfg(test)]
mod tests {
    use super::StatMgr;
    use crate::{
        metadata::{
            stat_mgr::STATS_REFRESH_THRESHOLD,
            table_mgr::{TableMgr, TABLE_CATALOG_TABLE_NAME, TABLE_NAME_FIELD},
        },
        server::simple_db::SimpleDB,
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "stat_mgr_test.log");
            let mut tx = db.new_tx();
            {
                let tm = Arc::new(TableMgr::new());
                tm.init(&mut tx);
                let sm = StatMgr::new(tm.clone());
                sm.init(&mut tx);

                let layout = tm.layout(TABLE_CATALOG_TABLE_NAME, &mut tx).unwrap();
                let stats1 = sm.table_stat_info(TABLE_CATALOG_TABLE_NAME, &layout, &mut tx);
                assert_eq!(stats1.blocks_accessed(), 1);
                assert_eq!(stats1.records_output(), 2);
                assert_eq!(stats1.distinct_values(TABLE_NAME_FIELD), 1);

                for _ in 0..(STATS_REFRESH_THRESHOLD + 1) {
                    let _ = sm.table_stat_info(TABLE_CATALOG_TABLE_NAME, &layout, &mut tx);
                }

                let stats2 = sm.table_stat_info(TABLE_CATALOG_TABLE_NAME, &layout, &mut tx);
                assert_eq!(stats2, stats1);
            }
            tx.commit().unwrap();
        }
    }
}
