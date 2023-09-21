// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    metadata::{metadata_mgr::MetadataMgr, stat_mgr::StatInfo},
    query::{
        operators::{ProductScan, ProjectScan, SelectScan},
        predicate::Predicate,
        scan::UpdateScan,
    },
    record::{
        schema::{Layout, Schema},
        table_scan::TableScan,
    },
    tx::transaction::Transaction,
};
use std::{cell::RefCell, cmp::min, rc::Rc, sync::Arc};

pub trait Plan {
    fn open<'lm, 'bm, 'scan>(
        &self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn UpdateScan + 'scan>
    where
        'lm: 'scan,
        'bm: 'scan;
    fn blocks_accessed(&self) -> usize;
    fn records_output(&self) -> usize;
    fn distinct_values(&self, field_name: &str) -> usize;
    fn schema(&self) -> Schema;
}

pub struct TablePlan {
    table_name: String,
    layout: Layout,
    stat_info: StatInfo,
}

impl<'lm, 'bm> TablePlan {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        table_name: &str,
        meta_mgr: Arc<MetadataMgr>,
    ) -> Self {
        let layout = meta_mgr.table_layout(table_name, tx.clone()).unwrap(); // FIXME:
        let stat_info = meta_mgr.table_stat_info(table_name, layout.clone(), tx.clone());
        Self {
            table_name: table_name.into(),
            layout,
            stat_info,
        }
    }
}

impl Plan for TablePlan {
    fn open<'lm, 'bm, 'scan>(
        &self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn UpdateScan + 'scan>
    where
        'lm: 'scan,
        'bm: 'scan,
    {
        Box::new(TableScan::new(
            tx.clone(),
            self.table_name.clone(),
            self.layout.clone(),
        ))
    }

    fn blocks_accessed(&self) -> usize {
        self.stat_info.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.stat_info.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.stat_info.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.layout.schema().clone()
    }
}

pub struct SelectPlan<'p> {
    plan: Box<dyn Plan + 'p>,
    pred: Predicate,
}

impl<'p> SelectPlan<'p> {
    pub fn new(plan: Box<dyn Plan + 'p>, pred: Predicate) -> Self {
        Self { plan, pred }
    }
}

impl<'p> Plan for SelectPlan<'p> {
    fn open<'lm, 'bm, 'scan>(
        &self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn UpdateScan + 'scan>
    where
        'lm: 'scan,
        'bm: 'scan,
    {
        let s = self.plan.open(tx);
        Box::new(SelectScan::new(s, self.pred.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        let reduction_factor = self.pred.reduction_factor(&self.plan);
        self.plan.records_output() / reduction_factor
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.pred.equates_with_constant(field_name).is_some() {
            1
        } else if let Some(field_name2) = self.pred.equates_with_field(field_name) {
            min(
                self.plan.distinct_values(field_name),
                self.plan.distinct_values(&field_name2),
            )
        } else {
            self.plan.distinct_values(field_name)
        }
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }
}

pub struct ProjectPlan<'p> {
    plan: Box<dyn Plan + 'p>,
    schema: Schema,
}

impl<'p> ProjectPlan<'p> {
    pub fn new(plan: Box<dyn Plan + 'p>, fields: Vec<&str>) -> Self {
        let ps = plan.schema();
        let mut schema = Schema::new();
        for f in fields {
            schema.add_field_from(f, &ps);
        }
        Self { plan, schema }
    }
}

impl<'p> Plan for ProjectPlan<'p> {
    fn open<'lm, 'bm, 'scan>(
        &self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn UpdateScan + 'scan>
    where
        'lm: 'scan,
        'bm: 'scan,
    {
        let scan = self.plan.open(tx);
        let fields: Vec<String> = self.schema.fields_iter().map(|f| f.into()).collect();
        Box::new(ProjectScan::new(scan, fields))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.plan.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

pub struct ProductPlan<'p> {
    plan1: Box<dyn Plan + 'p>,
    plan2: Box<dyn Plan + 'p>,
    schema: Schema,
}

impl<'p> ProductPlan<'p> {
    pub fn new(plan1: Box<dyn Plan + 'p>, plan2: Box<dyn Plan + 'p>) -> Self {
        let mut schema = Schema::new();
        schema.add_all(&plan1.schema());
        schema.add_all(&plan2.schema());
        Self {
            plan1,
            plan2,
            schema,
        }
    }
}

impl<'p> Plan for ProductPlan<'p> {
    fn open<'lm, 'bm, 'scan>(
        &self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn UpdateScan + 'scan>
    where
        'lm: 'scan,
        'bm: 'scan,
    {
        let s1 = self.plan1.open(tx.clone());
        let s2 = self.plan2.open(tx.clone());
        Box::new(ProductScan::new(s1, s2))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan1.blocks_accessed() + self.plan1.records_output() * self.plan2.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan1.records_output() * self.plan2.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.plan1.schema().has_field(field_name) {
            self.plan1.distinct_values(field_name)
        } else {
            self.plan2.distinct_values(field_name)
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{Plan, ProductPlan, SelectPlan, TablePlan};
    use crate::{
        query::predicate::{Expression, Predicate, Term},
        record::{schema::Schema, table_scan::TableScan},
        server::simple_db::SimpleDB,
    };
    use tempfile::tempdir;

    #[test]
    fn test() {
        let dir = tempdir().unwrap();
        println!("{:?}", dir);
        {
            let mut db = SimpleDB::new_for_test(dir.path(), "plan_test.log");
            db.init();

            let mdm = db.metadata_mgr();
            let tx = db.new_tx();
            {
                // 1. create tables (studuent and dept)
                {
                    let mut student = Schema::new();
                    student.add_i32_field("sid");
                    student.add_string_field("sname", 10);
                    student.add_i32_field("majorid");
                    student.add_i32_field("gradyear");
                    mdm.create_table("student", student, tx.clone()).unwrap();

                    let mut dept = Schema::new();
                    dept.add_i32_field("did");
                    dept.add_string_field("dname", 8);
                    mdm.create_table("dept", dept, tx.clone()).unwrap();
                }

                // 2. insert records
                {
                    // dept
                    let layout = mdm.table_layout("dept", tx.clone()).unwrap();
                    let mut ts = TableScan::new(tx.clone(), "dept".into(), layout);
                    ts.insert().unwrap();
                    ts.set_i32("did", 1).unwrap();
                    ts.set_string("dname", "Math".into()).unwrap();
                    ts.insert().unwrap();
                    ts.set_i32("did", 2).unwrap();
                    ts.set_string("dname", "CS".into()).unwrap();
                }
                {
                    // student
                    let layout = mdm.table_layout("student", tx.clone()).unwrap();
                    let mut ts = TableScan::new(tx.clone(), "student".into(), layout);
                    ts.insert().unwrap();
                    ts.set_i32("sid", 1).unwrap();
                    ts.set_string("sname", "Tom".into()).unwrap();
                    ts.set_i32("majorid", 2).unwrap();
                    ts.set_i32("gradyear", 2023).unwrap();
                    ts.insert().unwrap();
                    ts.set_i32("sid", 2).unwrap();
                    ts.set_string("sname", "John".into()).unwrap();
                    ts.set_i32("majorid", 1).unwrap();
                    ts.set_i32("gradyear", 2023).unwrap();
                }

                // 3. test
                let p1 = Box::new(TablePlan::new(tx.clone(), "student", mdm.clone()));
                let p2 = Box::new(TablePlan::new(tx.clone(), "dept", mdm.clone()));
                let p3 = Box::new(ProductPlan::new(p1, p2));

                let expr = Expression::new(
                    Term::FieldName("majorid".into()),
                    Term::FieldName("did".into()),
                );
                let pred = Predicate::new(expr);
                let p4 = SelectPlan::new(p3, pred);
                {
                    let mut s = p4.open(tx.clone());
                    assert!(s.before_first().is_ok());
                    assert!(s.next().unwrap());
                    assert_eq!(s.get_i32("sid").unwrap(), 1);
                    assert_eq!(s.get_string("sname").unwrap(), "Tom");
                    assert_eq!(s.get_i32("did").unwrap(), 2);
                    assert_eq!(s.get_string("dname").unwrap(), "CS");
                    assert!(s.next().unwrap());
                    assert_eq!(s.get_i32("sid").unwrap(), 2);
                    assert_eq!(s.get_string("sname").unwrap(), "John");
                    assert_eq!(s.get_i32("did").unwrap(), 1);
                    assert_eq!(s.get_string("dname").unwrap(), "Math");
                    assert!(!s.next().unwrap());
                    // s.close();
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
        dir.close().unwrap();
    }
}
