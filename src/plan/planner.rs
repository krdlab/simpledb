// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::plan::{Plan, SelectPlan, TablePlan};
use crate::{
    metadata::{common::MetadataError, metadata_mgr::MetadataMgr},
    parse::{
        data::{QueryData, UpdateCmd},
        lexer::LexerError,
        parser::Parser,
    },
    plan::plan::{ProductPlan, ProjectPlan},
    query::{
        predicate::{Constant, Predicate, Term},
        scan::ScanError,
    },
    record::schema::Schema,
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlannerError {
    #[error("{0:?}")]
    Metadata(#[from] MetadataError),

    #[error("{0:?}")]
    Scan(#[from] ScanError),

    #[error("{0:?}")]
    Lexer(#[from] LexerError),
}

pub type Result<T> = core::result::Result<T, PlannerError>;

// traits

pub trait QueryPlanner {
    fn create_plan<'s, 'lm: 's, 'bm: 's>(
        &'s self,
        data: QueryData,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn Plan + '_>;
}

pub trait UpdatePlanner {
    fn execute<'lm, 'bm>(
        &self,
        data: UpdateCmd,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64>;
}

// query impl

pub struct BasicQueryPlanner {
    mdm: Arc<MetadataMgr>,
}

impl BasicQueryPlanner {
    pub fn new(mdm: Arc<MetadataMgr>) -> Self {
        Self { mdm }
    }
}

impl QueryPlanner for BasicQueryPlanner {
    fn create_plan<'s, 'lm: 's, 'bm: 's>(
        &'s self,
        data: QueryData,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Box<dyn Plan + '_> {
        let mut plans = Vec::new();
        for table_name in data.tables() {
            if let Ok(viewdef) = self.mdm.view_def(table_name, tx.clone()) {
                let mut parser = Parser::new(&viewdef).unwrap(); // TODO
                let viewdata = parser.query().unwrap(); // TODO
                plans.push(self.create_plan(viewdata, tx.clone()));
            } else {
                plans.push(Box::new(TablePlan::new(
                    tx.clone(),
                    &table_name,
                    self.mdm.clone(),
                )));
            }
        }

        let mut plan = plans
            .into_iter()
            .reduce(|acc, p| Box::new(ProductPlan::new(acc, p)))
            .unwrap();
        plan = Box::new(SelectPlan::new(plan, data.pred().clone()));
        let fields = data.fields().iter().map(|f| &**f).collect();
        Box::new(ProjectPlan::new(plan, fields))
    }
}

// update impl

pub struct BasicUpdatePlanner {
    mdm: Arc<MetadataMgr>,
}

impl BasicUpdatePlanner {
    pub fn new(mdm: Arc<MetadataMgr>) -> Self {
        Self { mdm }
    }
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute<'lm, 'bm>(
        &self,
        data: UpdateCmd,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        match data {
            UpdateCmd::DeleteData { table_name, pred } => {
                self.execute_delete(table_name, pred, &tx)
            }
            UpdateCmd::ModifyData {
                table_name,
                field,
                value,
                pred,
            } => self.execute_modify(&table_name, &field, &value, pred, &tx),
            UpdateCmd::InsertData {
                table_name,
                fields,
                values,
            } => self.execute_insert(&table_name, &fields, &values, &tx),
            UpdateCmd::CreateTableData { table_name, schema } => {
                self.execute_create_table(&table_name, schema, &tx)
            }
            UpdateCmd::CreateViewData { view_name, query } => {
                self.execute_create_view(&view_name, &query, &tx)
            }
            UpdateCmd::CreateIndexData {
                index_name,
                table_name,
                field,
            } => self.execute_create_index(&index_name, &table_name, &field, &tx),
        }
    }
}

impl BasicUpdatePlanner {
    fn execute_delete<'lm, 'bm>(
        &self,
        table_name: String,
        pred: Predicate,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        let tp = Box::new(TablePlan::new(tx.clone(), &table_name, self.mdm.clone()));
        let sp = SelectPlan::new(tp, pred);
        let mut s = sp.open(tx.clone());
        let mut count = 0;
        while s.next()? {
            s.delete()?;
            count += 1;
        }
        Ok(count)
    }

    fn execute_modify<'lm, 'bm>(
        &self,
        table_name: &str,
        field: &str,
        value: &Term,
        pred: Predicate,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        let tp = Box::new(TablePlan::new(tx.clone(), table_name, self.mdm.clone()));
        let sp = SelectPlan::new(tp, pred);
        let mut s = sp.open(tx.clone());
        let mut count = 0;
        while s.next()? {
            let new_value = value.evaluate(&s);
            s.set_val(field, new_value)?;
            count += 1;
        }
        Ok(count)
    }

    fn execute_insert<'lm, 'bm>(
        &self,
        table_name: &str,
        fields: &Vec<String>,
        values: &Vec<Constant>,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        let p = TablePlan::new(tx.clone(), table_name, self.mdm.clone());
        let mut s = p.open(tx.clone());
        s.insert()?;
        let mut v = values.iter();
        for f in fields {
            let val = v.next().unwrap();
            s.set_val(f, val.clone()).unwrap();
        }
        Ok(1)
    }

    fn execute_create_table<'lm, 'bm>(
        &self,
        table_name: &str,
        schema: Schema,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        self.mdm.create_table(table_name, schema, tx.clone())?;
        Ok(0)
    }

    fn execute_create_view<'lm, 'bm>(
        &self,
        view_name: &str,
        query: &QueryData,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        self.mdm
            .create_view(view_name, &query.to_string(), tx.clone())?;
        Ok(0)
    }

    fn execute_create_index<'lm, 'bm>(
        &self,
        index_name: &str,
        table_name: &str,
        field: &str,
        tx: &Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        self.mdm
            .create_index(index_name, table_name, field, tx.clone())?;
        Ok(0)
    }
}

pub struct Planner {
    qp: Box<dyn QueryPlanner + Send + Sync>,
    up: Box<dyn UpdatePlanner + Send + Sync>,
}

impl<'s> Planner {
    pub fn new(
        qp: impl QueryPlanner + Send + Sync + 'static,
        up: impl UpdatePlanner + Send + Sync + 'static,
    ) -> Self {
        Self {
            qp: Box::new(qp),
            up: Box::new(up),
        }
    }

    pub fn create_query_plan<'lm: 's, 'bm: 's>(
        &'s self,
        query: &str,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<Box<dyn Plan + '_>> {
        let mut parser = Parser::new(query)?;
        let qry = parser.query()?;
        self.verify_query(&qry);
        let plan = self.qp.create_plan(qry, tx);
        Ok(plan)
        // see https://users.rust-lang.org/t/how-to-return-value-referencing-local-variable/60650/5
        // ガードの中身の参照を引き連れていくので，今のままだとムリ
        // plan の実装を外部から tx を与えて実行するような形式に修正する必要があるかも
    }

    fn verify_query(&self, data: &QueryData) {
        // TODO
    }

    pub fn execute_update<'lm, 'bm>(
        &self,
        command: &str,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
    ) -> Result<u64> {
        let mut parser = Parser::new(command)?;
        let cmd = parser.update_cmd()?;
        self.verify_update(&cmd);

        self.up.execute(cmd, tx)
    }

    fn verify_update(&self, data: &UpdateCmd) {
        // TODO
    }
}

#[cfg(test)]
mod tests {
    use crate::server::simple_db::SimpleDB;
    use tempfile::tempdir;

    #[test]
    fn test1() {
        let dir = tempdir().unwrap();
        {
            let mut db = SimpleDB::new_for_test(dir.path(), "planner_test1.log");
            db.init();

            let planner = db.planner();
            let tx = db.new_tx();
            {
                planner
                    .execute_update("create table T1(A int, B varchar(9))", tx.clone())
                    .unwrap();

                for i in 0..200 {
                    let cmd = format!("insert into T1(A, B) values ({}, 'rec{}')", i, i);
                    planner.execute_update(&cmd, tx.clone()).unwrap();
                }

                let query = "select B from T1 where A = 10";
                let plan = planner.create_query_plan(query, tx.clone()).unwrap();
                let mut scan = plan.open(tx.clone());
                assert!(scan.next().unwrap());
                assert_eq!(scan.get_string("b").unwrap(), "rec10"); // TODO: case insensitive
                assert!(!scan.next().unwrap());
            }
            tx.borrow_mut().commit().unwrap();
        }
    }

    #[test]
    fn test2() {
        let dir = tempdir().unwrap();
        {
            let mut db = SimpleDB::new_for_test(dir.path(), "planner_test2.log");
            db.init();

            let planner = db.planner();
            let tx = db.new_tx();
            {
                planner
                    .execute_update("create table T1(A int, B varchar(9))", tx.clone())
                    .unwrap();
                for i in 0..200 {
                    let cmd = format!("insert into T1(A, B) values ({}, 'rec{}')", i, i);
                    planner.execute_update(&cmd, tx.clone()).unwrap();
                }

                planner
                    .execute_update("create table T2(C int, D varchar(9))", tx.clone())
                    .unwrap();
                for i in 0..200 {
                    let c = 200 - i - 1;
                    let cmd = format!("insert into T2(C, D) values ({}, 'rec{}')", c, c);
                    planner.execute_update(&cmd, tx.clone()).unwrap();
                }

                let query = "select B, D from T1, T2 where A = C";
                let plan = planner.create_query_plan(query, tx.clone()).unwrap();
                let mut scan = plan.open(tx.clone());
                while scan.next().unwrap() {
                    assert_eq!(scan.get_string("b").unwrap(), scan.get_string("d").unwrap());
                    // TODO: case insensitive
                }
            }
            tx.borrow_mut().commit().unwrap();
        }
    }
}
