// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    predicate::{Constant, Predicate},
    scan::{Scan, ScanError, UpdateScan},
};

// select operator

pub struct SelectScan<S> {
    scan: S,
    pred: Predicate,
}

impl<S> SelectScan<S> {
    pub fn new(scan: S, pred: Predicate) -> Self {
        Self { scan, pred }
    }
}

impl<S> Scan for SelectScan<S>
where
    S: Scan,
{
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> bool {
        while self.scan.next() {
            if self.pred.is_satisfied(&mut self.scan) {
                return true;
            }
        }
        return false;
    }

    fn get_i32(&self, field_name: &str) -> super::scan::Result<i32> {
        self.scan.get_i32(field_name)
    }

    fn get_string(&self, field_name: &str) -> super::scan::Result<String> {
        self.scan.get_string(field_name)
    }

    fn get_val(&self, field_name: &str) -> super::scan::Result<Constant> {
        self.scan.get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan.close();
    }
}

impl<S> UpdateScan for SelectScan<S>
where
    S: UpdateScan,
{
    fn set_val(&mut self, field_name: &str, value: Constant) -> super::scan::Result<()> {
        self.scan.set_val(field_name, value)
    }

    fn set_i32(&mut self, field_name: &str, value: i32) -> super::scan::Result<()> {
        self.scan.set_i32(field_name, value)
    }

    fn set_string(&mut self, field_name: &str, value: String) -> super::scan::Result<()> {
        self.scan.set_string(field_name, value)
    }

    fn insert(&mut self) -> super::scan::Result<()> {
        self.scan.insert()
    }

    fn delete(&mut self) -> super::scan::Result<()> {
        self.scan.delete()
    }

    fn get_rid(&self) -> super::scan::RID {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: super::scan::RID) -> super::scan::Result<()> {
        self.scan.move_to_rid(rid)
    }
}

// project operator

pub struct ProjectScan<S> {
    scan: S,
    fields: Vec<String>,
}

impl<S> ProjectScan<S> {
    pub fn new(scan: S, fields: Vec<String>) -> Self {
        Self { scan, fields }
    }
}

impl<S> Scan for ProjectScan<S>
where
    S: Scan,
{
    fn before_first(&mut self) {
        self.scan.before_first();
    }

    fn next(&mut self) -> bool {
        self.scan.next()
    }

    fn get_i32(&self, field_name: &str) -> super::scan::Result<i32> {
        if self.has_field(field_name) {
            self.scan.get_i32(field_name)
        } else {
            Err(ScanError::FieldNotFound(field_name.into()))
        }
    }

    fn get_string(&self, field_name: &str) -> super::scan::Result<String> {
        if self.has_field(field_name) {
            self.scan.get_string(field_name)
        } else {
            Err(ScanError::FieldNotFound(field_name.into()))
        }
    }

    fn get_val(&self, field_name: &str) -> super::scan::Result<Constant> {
        if self.has_field(field_name) {
            self.scan.get_val(field_name)
        } else {
            Err(ScanError::FieldNotFound(field_name.into()))
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.fields.contains(&field_name.into())
    }

    fn close(&mut self) {
        self.scan.close()
    }
}

// product operator

pub struct ProductScan<S> {
    scan1: S,
    scan2: S,
}

impl<S> ProductScan<S> {
    pub fn new(scan1: S, scan2: S) -> Self {
        Self { scan1, scan2 }
    }
}

impl<S> Scan for ProductScan<S>
where
    S: Scan,
{
    fn before_first(&mut self) {
        self.scan1.before_first();
        self.scan1.next();
        self.scan2.before_first();
    }

    fn next(&mut self) -> bool {
        if self.scan2.next() {
            true
        } else {
            self.scan2.before_first();
            self.scan2.next() && self.scan1.next()
        }
    }

    fn get_i32(&self, field_name: &str) -> super::scan::Result<i32> {
        if self.scan1.has_field(field_name) {
            self.scan1.get_i32(field_name)
        } else {
            self.scan2.get_i32(field_name)
        }
    }

    fn get_string(&self, field_name: &str) -> super::scan::Result<String> {
        if self.scan1.has_field(field_name) {
            self.scan1.get_string(field_name)
        } else {
            self.scan2.get_string(field_name)
        }
    }

    fn get_val(&self, field_name: &str) -> super::scan::Result<Constant> {
        if self.scan1.has_field(field_name) {
            self.scan1.get_val(field_name)
        } else {
            self.scan2.get_val(field_name)
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.scan1.has_field(field_name) || self.scan2.has_field(field_name)
    }

    fn close(&mut self) {
        self.scan1.close();
        self.scan2.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{ProductScan, ProjectScan, SelectScan};
    use crate::{
        query::{
            predicate::{Constant, Term, Predicate, Expression},
            scan::Scan,
        },
        record::{
            schema::{Layout, Schema},
            table_scan::TableScan,
        },
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test1() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "operators_test1.log");

            let mut schema = Schema::new();
            schema.add_i32_field("A");
            schema.add_string_field("B", 9);
            let layout = Layout::new(schema);

            let tx = db.new_tx();
            {
                let mut s1 = TableScan::new(tx.clone(), "T", &layout);
                s1.before_first();
                for i in 0..200 {
                    s1.insert();
                    s1.set_i32("A", i).unwrap();
                    s1.set_string("B", format!("rec{}", i)).unwrap();
                }
            }
            {
                let s2 = TableScan::new(tx.clone(), "T", &layout);
                let c = Constant::Int(10);
                let t = Expression::new(Term::FieldName("A".into()), Term::Constant(c));
                let pred = Predicate::new(t);

                let s3 = SelectScan::new(s2, pred);
                let mut s4 = ProjectScan::new(s3, vec!["B".into()]);
                s4.before_first();

                assert!(s4.next());
                assert_eq!(s4.get_string("B").unwrap(), "rec10");
                assert!(!s4.next());
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }

    #[test]
    fn test2() {
        let dir = tempdir().unwrap();
        {
            let db = SimpleDB::new_for_test(dir.path(), "operators_test2.log");
            let tx = db.new_tx();

            {
                let mut schema1 = Schema::new();
                schema1.add_i32_field("A");
                schema1.add_string_field("B", 9);
                let layout1 = Layout::new(schema1);
                {
                    let mut us1 = TableScan::new(tx.clone(), "T1", &layout1);
                    us1.before_first();

                    for i in 0..200 {
                        us1.insert();
                        us1.set_i32("A", i).unwrap();
                        us1.set_string("B", format!("str{}", i)).unwrap();
                    }
                }

                let mut schema2 = Schema::new();
                schema2.add_i32_field("C");
                schema2.add_string_field("D", 9);
                let layout2 = Layout::new(schema2);
                {
                    let mut us2 = TableScan::new(tx.clone(), "T2", &layout2);
                    us2.before_first();
                    for i in 0..200 {
                        us2.insert();
                        let num = 200 - (i - 1);
                        us2.set_i32("C", num).unwrap();
                        us2.set_string("D", format!("str{}", num)).unwrap();
                    }
                }

                {
                    let s1 = TableScan::new(tx.clone(), "T1", &layout1);
                    let s2 = TableScan::new(tx.clone(), "T2", &layout2);
                    let s3 = ProductScan::new(s1, s2);

                    let t = Expression::new(
                        Term::FieldName("A".into()),
                        Term::FieldName("C".into()),
                    );
                    let pred = Predicate::new(t);

                    let s4 = SelectScan::new(s3, pred);
                    let mut s5 = ProjectScan::new(s4, vec!["B".into(), "D".into()]);
                    s5.before_first();
                    while s5.next() {
                        assert_eq!(s5.get_string("B").unwrap(), s5.get_string("D").unwrap());
                    }
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
