// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::query::scan::ScanError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetadataError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("view not found: {0}")]
    ViewNotFound(String),

    #[error("{0:?}")]
    Scan(#[from] ScanError),
}

pub type Result<T> = core::result::Result<T, MetadataError>;
