// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::plan::planner::Planner;
use crate::query::scan::UpdateScan;
use crate::rdbc;
use crate::rdbc::api::{Connection, ResultSet, ResultSetMetaData};
use crate::record::schema::{Schema, SqlType};
use crate::server::simple_db::SimpleDB;
use crate::tx::transaction::Transaction;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

pub struct EmbeddedDriver {}

impl rdbc::api::Driver for EmbeddedDriver {
    fn connect(&self, url: &str) -> rdbc::api::Result<Box<dyn Connection + 'static>> {
        let path = url
            .split(":")
            .last()
            .ok_or(rdbc::api::Error::General(format!("invalid url: {}", url)))?;
        let mut db = SimpleDB::new(Path::new(path), 4096, 16);
        db.init();
        Ok(Box::new(EmbeddedConnection::new(db)))
    }
}

struct EmbeddedConnection<'lm, 'bm> {
    db: SimpleDB<'lm, 'bm>,
    tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
}

impl<'lm, 'bm> EmbeddedConnection<'lm, 'bm> {
    pub fn new(db: SimpleDB<'lm, 'bm>) -> Self {
        let tx = db.new_tx();
        Self { db, tx }
    }

    pub(crate) fn transaction(&self) -> Rc<RefCell<Transaction<'lm, 'bm>>> {
        self.tx.clone()
    }

    pub(crate) fn planner(&self) -> Arc<Planner> {
        self.db.planner()
    }

    fn close(&self) -> rdbc::api::Result<()> {
        self.tx
            .borrow_mut()
            .commit()
            .map_err(|e| e.into())
            .map_err(rdbc::api::Error::Internal)
    }
}

impl Connection for EmbeddedConnection<'_, '_> {
    fn create_statement(&mut self) -> rdbc::api::Result<Box<dyn rdbc::api::Statement + '_>> {
        Ok(Box::new(EmbeddedStatement { conn: self }))
    }

    fn prepare_statement(
        &mut self,
        _sql: &str,
    ) -> rdbc::api::Result<Box<dyn rdbc::api::PreparedStatement + '_>> {
        todo!() // TODO:
    }

    fn commit(&mut self) -> rdbc::api::Result<()> {
        self.tx
            .borrow_mut()
            .commit()
            .map_err(|e| e.into())
            .map_err(rdbc::api::Error::Internal)?;
        self.tx = self.db.new_tx();
        Ok(())
    }

    fn rollback(&mut self) -> rdbc::api::Result<()> {
        self.tx
            .borrow_mut()
            .rollback()
            .map_err(|e| e.into())
            .map_err(rdbc::api::Error::Internal)?;
        self.tx = self.db.new_tx();
        Ok(())
    }
}

struct EmbeddedStatement<'lm, 'bm, 'c> {
    conn: &'c mut EmbeddedConnection<'lm, 'bm>,
}

impl<'lm, 'bm, 'c> EmbeddedStatement<'lm, 'bm, 'c> {
    fn close(&self) -> rdbc::api::Result<()> {
        Ok(())
    }
}

impl<'lm, 'bm, 'c> rdbc::api::Statement for EmbeddedStatement<'lm, 'bm, 'c> {
    fn execute_query(&mut self, sql: &str) -> rdbc::api::Result<Box<dyn ResultSet + '_>> {
        let tx = self.conn.transaction();
        match self.conn.planner().create_query_plan(sql, tx.clone()) {
            Ok(plan) => {
                let scan = plan.open(tx.clone());
                let schema = plan.schema();
                Ok(Box::new(EmbeddedResultSet::new(self.conn, scan, schema)))
            }
            Err(pe) => {
                let e = if let Err(re) = self.conn.rollback() {
                    let ae: anyhow::Error = pe.into();
                    ae.context(re)
                } else {
                    pe.into()
                };
                Err(rdbc::api::Error::Internal(e))
            }
        }
    }

    fn execute_update(&mut self, sql: &str) -> rdbc::api::Result<u64> {
        let tx = self.conn.transaction();
        match self.conn.planner().execute_update(sql, tx) {
            Ok(num) => {
                self.conn.commit()?;
                Ok(num)
            }
            Err(pe) => {
                let e = if let Err(re) = self.conn.rollback() {
                    let ae: anyhow::Error = pe.into();
                    ae.context(re)
                } else {
                    pe.into()
                };
                Err(rdbc::api::Error::Internal(e))
            }
        }
    }
}

struct EmbeddedResultSet<'lm, 'bm, 'c, 'scan> {
    conn: &'c mut EmbeddedConnection<'lm, 'bm>,
    scan: Box<dyn UpdateScan + 'scan>,
    schema: Schema,
}

impl<'lm, 'bm, 'c, 'scan> EmbeddedResultSet<'lm, 'bm, 'c, 'scan> {
    pub fn new(
        conn: &'c mut EmbeddedConnection<'lm, 'bm>,
        scan: Box<dyn UpdateScan + 'scan>,
        schema: Schema,
    ) -> Self {
        Self { conn, scan, schema }
    }

    fn to_rdbc_datatype(sql_type: SqlType) -> rdbc::api::DataType {
        match sql_type {
            SqlType::Integer => rdbc::api::DataType::Integer,
            SqlType::VarChar => rdbc::api::DataType::Utf8,
        }
    }

    fn close(&mut self) -> rdbc::api::Result<()> {
        // self.scan.close();
        // self.conn.commit()?;
        Ok(())
    }
}

impl<'lm, 'bm, 'c, 'scan> ResultSet for EmbeddedResultSet<'lm, 'bm, 'c, 'scan> {
    fn meta_data(&self) -> rdbc::api::Result<Box<dyn ResultSetMetaData>> {
        let meta: Vec<rdbc::api::Column> = self
            .schema
            .fields_iter()
            .map(|name| {
                rdbc::api::Column::new(
                    name.as_str(),
                    EmbeddedResultSet::to_rdbc_datatype(
                        self.schema.field_type(name.as_str()).unwrap(),
                    ),
                    self.schema
                        .field_length(name.as_str())
                        .unwrap_or(11)
                        .try_into()
                        .unwrap(),
                )
            })
            .collect();
        Ok(Box::new(meta))
    }

    fn next(&mut self) -> rdbc::api::Result<bool> {
        match self.scan.next() {
            Ok(has) => Ok(has),
            Err(se) => {
                let e = if let Err(re) = self.conn.rollback() {
                    let ae: anyhow::Error = se.into();
                    ae.context(re)
                } else {
                    se.into()
                };
                Err(rdbc::api::Error::Internal(e))
            }
        }
    }

    fn get_i32(&mut self, i: usize) -> rdbc::api::Result<Option<i32>> {
        let name = self.schema.field_name(i);
        if let Some(name) = name {
            match self.scan.get_i32(name) {
                Ok(value) => Ok(Some(value)),
                Err(se) => {
                    let e = if let Err(re) = self.conn.rollback() {
                        let ae: anyhow::Error = se.into();
                        ae.context(re)
                    } else {
                        se.into()
                    };
                    Err(rdbc::api::Error::Internal(e))
                }
            }
        } else {
            return Ok(None);
        }
    }

    fn get_string(&mut self, i: usize) -> rdbc::api::Result<Option<String>> {
        let name = self.schema.field_name(i);
        if let Some(name) = name {
            match self.scan.get_string(name) {
                Ok(value) => Ok(Some(value)),
                Err(se) => {
                    let e = if let Err(re) = self.conn.rollback() {
                        let ae: anyhow::Error = se.into();
                        ae.context(re)
                    } else {
                        se.into()
                    };
                    Err(rdbc::api::Error::Internal(e))
                }
            }
        } else {
            return Ok(None);
        }
    }
}

impl Drop for EmbeddedConnection<'_, '_> {
    fn drop(&mut self) {
        self.close().unwrap(); // TODO
    }
}

impl Drop for EmbeddedStatement<'_, '_, '_> {
    fn drop(&mut self) {
        self.close().unwrap(); // TODO
    }
}

impl Drop for EmbeddedResultSet<'_, '_, '_, '_> {
    fn drop(&mut self) {
        self.close().unwrap(); // TODO
    }
}
