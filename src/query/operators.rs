// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    predicate::{Constant, Predicate},
    scan::{Result, Scan, ScanError, UpdateScan, RID},
};

// select operator

pub struct SelectScan<'s> {
    scan: Box<dyn UpdateScan + 's>,
    pred: Predicate,
}

impl<'s> SelectScan<'s> {
    pub fn new(scan: Box<dyn UpdateScan + 's>, pred: Predicate) -> Self {
        Self { scan, pred }
    }
}

impl<'s> Scan for SelectScan<'s> {
    fn before_first(&mut self) -> Result<()> {
        self.scan.before_first()
    }

    fn next(&mut self) -> Result<bool> {
        while self.scan.next()? {
            if self.pred.is_satisfied(&self.scan) {
                return Ok(true);
            }
        }
        return Ok(false);
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

impl<'s> UpdateScan for SelectScan<'s> {
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

    fn get_rid(&self) -> Result<RID> {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: super::scan::RID) -> super::scan::Result<()> {
        self.scan.move_to_rid(rid)
    }
}

// project operator

pub struct ProjectScan<'s> {
    scan: Box<dyn UpdateScan + 's>,
    fields: Vec<String>,
}

impl<'s> ProjectScan<'s> {
    pub fn new(scan: Box<dyn UpdateScan + 's>, fields: Vec<String>) -> Self {
        Self { scan, fields }
    }
}

impl<'s> Scan for ProjectScan<'s> {
    fn before_first(&mut self) -> Result<()> {
        self.scan.before_first()
    }

    fn next(&mut self) -> Result<bool> {
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

impl<'s> UpdateScan for ProjectScan<'s> {
    fn set_val(&mut self, _field_name: &str, _value: Constant) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_val".into()))
    }

    fn set_i32(&mut self, _field_name: &str, _value: i32) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_i32".into()))
    }

    fn set_string(&mut self, _field_name: &str, _value: String) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_string".into()))
    }

    fn insert(&mut self) -> Result<()> {
        Err(ScanError::UnsupportedOperation("insert".into()))
    }

    fn delete(&mut self) -> Result<()> {
        Err(ScanError::UnsupportedOperation("delete".into()))
    }

    fn get_rid(&self) -> Result<RID> {
        Err(ScanError::UnsupportedOperation("get_rid".into()))
    }

    fn move_to_rid(&mut self, _rid: super::scan::RID) -> Result<()> {
        Err(ScanError::UnsupportedOperation("move_to_rid".into()))
    }
}

// product operator

pub struct ProductScan<'s> {
    scan1: Box<dyn UpdateScan + 's>,
    scan2: Box<dyn UpdateScan + 's>,
}

impl<'s> ProductScan<'s> {
    pub fn new(scan1: Box<dyn UpdateScan + 's>, scan2: Box<dyn UpdateScan + 's>) -> Self {
        let mut this = Self { scan1, scan2 };
        this.before_first().unwrap(); // TODO
        this
    }
}

impl<'s> Scan for ProductScan<'s> {
    fn before_first(&mut self) -> Result<()> {
        self.scan1.before_first()?;
        self.scan1.next()?;
        self.scan2.before_first()?;
        Ok(())
    }

    fn next(&mut self) -> Result<bool> {
        if self.scan2.next()? {
            Ok(true)
        } else {
            self.scan2.before_first()?;
            Ok(self.scan2.next()? && self.scan1.next()?)
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

impl<'s> UpdateScan for ProductScan<'s> {
    fn set_val(&mut self, _field_name: &str, _value: Constant) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_val".into()))
    }

    fn set_i32(&mut self, _field_name: &str, _value: i32) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_i32".into()))
    }

    fn set_string(&mut self, _field_name: &str, _value: String) -> Result<()> {
        Err(ScanError::UnsupportedOperation("set_string".into()))
    }

    fn insert(&mut self) -> Result<()> {
        Err(ScanError::UnsupportedOperation("insert".into()))
    }

    fn delete(&mut self) -> Result<()> {
        Err(ScanError::UnsupportedOperation("delete".into()))
    }

    fn get_rid(&self) -> Result<RID> {
        Err(ScanError::UnsupportedOperation("get_rid".into()))
    }

    fn move_to_rid(&mut self, _rid: super::scan::RID) -> Result<()> {
        Err(ScanError::UnsupportedOperation("move_to_rid".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::{ProductScan, ProjectScan, SelectScan};
    use crate::{
        query::{
            predicate::{Constant, Expression, Predicate, Term},
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
            let layout = {
                let mut schema = Schema::new();
                schema.add_i32_field("A");
                schema.add_string_field("B", 9);
                Layout::new(schema)
            };

            let tx = db.new_tx();
            {
                let mut s1 = TableScan::new(tx.clone(), "T".into(), layout.clone());
                s1.before_first().unwrap();
                for i in 0..200 {
                    s1.insert().unwrap();
                    s1.set_i32("A", i).unwrap();
                    s1.set_string("B", format!("rec{}", i)).unwrap();
                }
            }
            {
                let s2 = Box::new(TableScan::new(tx.clone(), "T".into(), layout.clone()));
                let pred = {
                    let c = Constant::Int(10);
                    let t = Expression::new(Term::FieldName("A".into()), Term::Constant(c));
                    Predicate::new(t)
                };

                let s3 = Box::new(SelectScan::new(s2, pred));
                let mut s4 = ProjectScan::new(s3, vec!["B".into()]);
                s4.before_first().unwrap();

                assert!(s4.next().unwrap());
                assert_eq!(s4.get_string("B").unwrap(), "rec10");
                assert!(!s4.next().unwrap());
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
                    let mut us1 = TableScan::new(tx.clone(), "T1".into(), layout1.clone());
                    us1.before_first().unwrap();

                    for i in 0..200 {
                        us1.insert().unwrap();
                        us1.set_i32("A", i).unwrap();
                        us1.set_string("B", format!("str{}", i)).unwrap();
                    }
                }

                let mut schema2 = Schema::new();
                schema2.add_i32_field("C");
                schema2.add_string_field("D", 9);
                let layout2 = Layout::new(schema2);
                {
                    let mut us2 = TableScan::new(tx.clone(), "T2".into(), layout2.clone());
                    us2.before_first().unwrap();
                    for i in 0..200 {
                        us2.insert().unwrap();
                        let num = 200 - (i - 1);
                        us2.set_i32("C", num).unwrap();
                        us2.set_string("D", format!("str{}", num)).unwrap();
                    }
                }

                {
                    let s1 = Box::new(TableScan::new(tx.clone(), "T1".into(), layout1.clone()));
                    let s2 = Box::new(TableScan::new(tx.clone(), "T2".into(), layout2.clone()));
                    let s3 = Box::new(ProductScan::new(s1, s2));

                    let t =
                        Expression::new(Term::FieldName("A".into()), Term::FieldName("C".into()));
                    let pred = Predicate::new(t);

                    let s4 = Box::new(SelectScan::new(s3, pred));
                    let mut s5 = ProjectScan::new(s4, vec!["B".into(), "D".into()]);
                    s5.before_first().unwrap();
                    while s5.next().unwrap() {
                        assert_eq!(s5.get_string("B").unwrap(), s5.get_string("D").unwrap());
                    }
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
