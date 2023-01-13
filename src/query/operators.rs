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
            if self.pred.is_satisfied(&self.scan) {
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

    fn close(&self) {
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

    fn close(&self) {
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

    fn close(&self) {
        self.scan1.close();
        self.scan2.close();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
