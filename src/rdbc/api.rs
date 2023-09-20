// This module is written based on https://github.com/tokio-rs/rdbc/blob/dcaa6fff9445123d3d829aedbf9e8fdcc473d504/rdbc/src/lib.rs

use anyhow;
use thiserror;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    General(String),

    #[error("{0}")]
    Internal(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Utf8,
}

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    String(String),
}

impl ToString for Value {
    fn to_string(&self) -> String {
        match self {
            Value::Int32(n) => format!("{}", n),
            Value::String(s) => format!("'{}'", s),
        }
    }
}

pub trait Driver: Sync + Send {
    fn connect(&self, url: &str) -> Result<Box<dyn Connection>>;
}

pub trait Connection {
    fn create_statement(&mut self) -> Result<Box<dyn Statement + '_>>;
    fn prepare_statement(&mut self, sql: &str) -> Result<Box<dyn PreparedStatement + '_>>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
}

pub trait Statement {
    fn execute_query(&mut self, sql: &str, params: &[Value]) -> Result<Box<dyn ResultSet + '_>>;
    fn execute_update(&mut self, sql: &str, params: &[Value]) -> Result<u64>;
}

pub trait PreparedStatement {
    fn execute_query(&mut self, params: &[Value]) -> Result<Box<dyn ResultSet + '_>>;
    fn execute_update(&mut self, params: &[Value]) -> Result<u64>;
}

pub trait ResultSet {
    fn meta_data(&self) -> Result<Box<dyn ResultSetMetaData>>;
    fn next(&mut self) -> Result<bool>;
    fn get_i32(&mut self, i: usize) -> Result<Option<i32>>;
    fn get_string(&mut self, i: usize) -> Result<Option<String>>;
}

pub trait ResultSetMetaData {
    fn num_columns(&self) -> usize;
    fn column_name(&self, i: usize) -> String;
    fn column_type(&self, i: usize) -> DataType;
    fn column_display_size(&self, i: usize) -> usize;
}

#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    data_type: DataType,
    display_size: usize,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, display_size: usize) -> Self {
        Column {
            name: name.to_owned(),
            data_type,
            display_size,
        }
    }
}

impl ResultSetMetaData for Vec<Column> {
    fn num_columns(&self) -> usize {
        self.len()
    }

    fn column_name(&self, i: usize) -> String {
        self[i].name.clone()
    }

    fn column_type(&self, i: usize) -> DataType {
        self[i].data_type
    }

    fn column_display_size(&self, i: usize) -> usize {
        self[i].display_size
    }
}
