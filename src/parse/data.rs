// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::fmt::Display;

use crate::{
    query::predicate::{Constant, Predicate, Term},
    record::schema::Schema,
};

pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    pred: Predicate,
}

impl Display for QueryData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols = self.fields.join(", ");
        let tbls = self.tables.join(", ");
        if !self.pred.is_empty() {
            write!(f, "select {} from {} where {}", cols, tbls, self.pred)
        } else {
            write!(f, "select {} from {}", cols, tbls)
        }
    }
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, pred: Predicate) -> Self {
        Self {
            fields,
            tables,
            pred,
        }
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn tables(&self) -> &Vec<String> {
        &self.tables
    }

    pub fn pred(&self) -> &Predicate {
        &self.pred
    }
}

pub enum UpdateCmd {
    DeleteData {
        table_name: String,
        pred: Predicate,
    },
    InsertData {
        table_name: String,
        fields: Vec<String>,
        values: Vec<Constant>,
    },
    ModifyData {
        table_name: String,
        field: String,
        value: Term,
        pred: Predicate,
    },
    CreateTableData {
        table_name: String,
        schema: Schema,
    },
    CreateViewData {
        view_name: String,
        query: QueryData,
    },
    CreateIndexData {
        index_name: String,
        table_name: String,
        field: String,
    },
}
