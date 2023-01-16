// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::collections::HashMap;

use crate::{
    record::{
        schema::{Layout, Schema},
        table_scan::TableScan,
    },
    tx::transaction::Transaction,
};

pub struct TableMgr {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

pub const MAX_NAME_LENGTH: usize = 16;
pub const TABLE_CATALOG_TABLE_NAME: &str = "tblcat";
pub const TABLE_NAME_FIELD: &str = "tblname";
const FIELD_CATALOG_TABLE_NAME: &str = "fldcat";

impl TableMgr {
    pub fn new() -> Self {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_string_field(TABLE_NAME_FIELD, MAX_NAME_LENGTH);
        tcat_schema.add_i32_field("slotsize");
        let tcat_layout = Layout::new(tcat_schema);

        let mut fcat_schema = Schema::new();
        fcat_schema.add_string_field(TABLE_NAME_FIELD, MAX_NAME_LENGTH);
        fcat_schema.add_string_field("fldname", MAX_NAME_LENGTH);
        fcat_schema.add_i32_field("type");
        fcat_schema.add_i32_field("length");
        fcat_schema.add_i32_field("offset");
        let fcat_layout = Layout::new(fcat_schema);

        Self {
            tcat_layout,
            fcat_layout,
        }
    }

    pub fn init(&self, tx: &mut Transaction) {
        self.create_table(
            TABLE_CATALOG_TABLE_NAME,
            self.tcat_layout.schema().clone(),
            tx,
        );
        self.create_table(
            FIELD_CATALOG_TABLE_NAME,
            self.fcat_layout.schema().clone(),
            tx,
        );
    }

    pub fn create_table(&self, tblname: &str, schema: Schema, tx: &mut Transaction) {
        let layout = Layout::new(schema.clone());
        {
            let mut tcat = TableScan::new(tx, TABLE_CATALOG_TABLE_NAME, &self.tcat_layout);
            tcat.insert();
            tcat.set_string(TABLE_NAME_FIELD, tblname.into());
            tcat.set_i32("slotsize", layout.slotsize().try_into().unwrap()); // TODO
        }
        {
            let mut fcat = TableScan::new(tx, FIELD_CATALOG_TABLE_NAME, &self.fcat_layout);
            for fldname in schema.fields_iter() {
                let ftype = schema.field_type(fldname).unwrap(); // NOTE: If the returned value is None, it's a bug.
                let flength = schema.field_length(fldname).unwrap(); // NOTE: same as above
                let foffset = layout.field_offset(fldname).unwrap();
                fcat.insert();
                fcat.set_string(TABLE_NAME_FIELD, tblname.into());
                fcat.set_string("fldname", fldname.into());
                fcat.set_i32("type", ftype.into());
                fcat.set_i32("length", flength.try_into().unwrap()); // TODO
                fcat.set_i32("offset", foffset.try_into().unwrap()); // TODO
            }
        }
    }

    fn table_slotsize(&self, tblname: &str, tx: &mut Transaction) -> Option<usize> {
        let mut tcat = TableScan::new(tx, TABLE_CATALOG_TABLE_NAME, &self.tcat_layout);
        while tcat.next() {
            if let Ok(tn) = tcat.get_string(TABLE_NAME_FIELD) {
                if tn == tblname {
                    let size = tcat.get_i32("slotsize").unwrap().try_into().unwrap();
                    return Some(size);
                }
            }
        }
        None
    }

    pub fn layout(&self, tblname: &str, tx: &mut Transaction) -> Option<Layout> {
        if let Some(size) = self.table_slotsize(tblname, tx) {
            let mut schema = Schema::new();
            let mut offsets = HashMap::new();

            let mut fcat = TableScan::new(tx, FIELD_CATALOG_TABLE_NAME, &self.fcat_layout);
            while fcat.next() {
                if let Ok(tn) = fcat.get_string(TABLE_NAME_FIELD) {
                    if tn == tblname {
                        let fname = fcat.get_string("fldname").unwrap();
                        let ftype = fcat.get_i32("type").unwrap();
                        let flength = fcat.get_i32("length").unwrap();
                        let foffset = fcat.get_i32("offset").unwrap();
                        offsets.insert(fname.clone(), foffset.try_into().unwrap());
                        schema.add_field(
                            &fname,
                            ftype.try_into().unwrap(),
                            flength.try_into().unwrap(),
                        ); // TODO
                    }
                }
            }

            Some(Layout::from_metadata(schema, offsets, size))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{TableMgr, FIELD_CATALOG_TABLE_NAME, TABLE_CATALOG_TABLE_NAME};
    use crate::{
        metadata::table_mgr::{MAX_NAME_LENGTH, TABLE_NAME_FIELD},
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
            let db = SimpleDB::new_for_test(dir.path(), "table_mgr_test.log");
            let mut tx = db.new_tx();

            let tm = TableMgr::new();
            tm.init(&mut tx);
            {
                let mut schema = Schema::new();
                schema.add_i32_field("A");
                schema.add_string_field("B", 9);
                tm.create_table("MyTable", schema, &mut tx);

                let layout = tm.layout("MyTable", &mut tx).unwrap();
                assert_eq!(layout.slotsize(), 48); // NOTE: 4 + 4 + 4 (area of string bytes length) + (9 (field length) * 4 (bytes/char))

                let schema2 = layout.schema();
                let mut field_iter = schema2.fields_iter();

                let f1 = field_iter.next().unwrap();
                assert_eq!(f1, "A");
                assert_eq!(schema2.field_type(f1), Some(SqlType::Integer));
                assert_eq!(schema2.field_length(f1), Some(0)); // NOTE: an integer value is 0 length

                let f2 = field_iter.next().unwrap();
                assert_eq!(f2, "B");
                assert_eq!(schema2.field_type(f2), Some(SqlType::VarChar));
                assert_eq!(schema2.field_length(f2), Some(9));

                assert_eq!(field_iter.next(), None);
            }

            tx.commit().unwrap();
        }
        dir.close().unwrap();
    }

    #[test]
    fn test_catalog() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "table_mgr_test_catalog.log");
            let mut tx = db.new_tx();
            {
                let tm = TableMgr::new();
                tm.init(&mut tx);
                let tcat_layout = tm.layout(TABLE_CATALOG_TABLE_NAME, &mut tx).unwrap();
                let fcat_layout = tm.layout(FIELD_CATALOG_TABLE_NAME, &mut tx).unwrap();
                {
                    let mut ts = TableScan::new(&mut tx, TABLE_CATALOG_TABLE_NAME, &tcat_layout);
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        TABLE_CATALOG_TABLE_NAME
                    );
                    assert_eq!(
                        ts.get_i32("slotsize").unwrap() as usize,
                        tcat_layout.slotsize()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(
                        ts.get_i32("slotsize").unwrap() as usize,
                        fcat_layout.slotsize()
                    );
                    assert_eq!(ts.next(), false);
                }
                {
                    let mut ts = TableScan::new(&mut tx, FIELD_CATALOG_TABLE_NAME, &fcat_layout);

                    // NOTE: table catalog's fields
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        TABLE_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), TABLE_NAME_FIELD);
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::VarChar.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, MAX_NAME_LENGTH);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        tcat_layout.field_offset(TABLE_NAME_FIELD).unwrap()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        TABLE_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), "slotsize");
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::Integer.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, 0);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        tcat_layout.field_offset("slotsize").unwrap()
                    );

                    // NOTE: field catalog's fields
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), TABLE_NAME_FIELD);
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::VarChar.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, MAX_NAME_LENGTH);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        fcat_layout.field_offset(TABLE_NAME_FIELD).unwrap()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), "fldname");
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::VarChar.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, MAX_NAME_LENGTH);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        fcat_layout.field_offset("fldname").unwrap()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), "type");
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::Integer.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, 0);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        fcat_layout.field_offset("type").unwrap()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), "length");
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::Integer.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, 0);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        fcat_layout.field_offset("length").unwrap()
                    );
                    assert_eq!(ts.next(), true);
                    assert_eq!(
                        ts.get_string(TABLE_NAME_FIELD).unwrap(),
                        FIELD_CATALOG_TABLE_NAME
                    );
                    assert_eq!(ts.get_string("fldname").unwrap(), "offset");
                    assert_eq!(ts.get_i32("type").unwrap(), SqlType::Integer.into());
                    assert_eq!(ts.get_i32("length").unwrap() as usize, 0);
                    assert_eq!(
                        ts.get_i32("offset").unwrap() as usize,
                        fcat_layout.field_offset("offset").unwrap()
                    );

                    assert_eq!(ts.next(), false);
                }
            }
            tx.commit().unwrap();
        }
        dir.close().unwrap();
    }
}
