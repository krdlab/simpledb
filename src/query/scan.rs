// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::predicate::Constant;
use crate::{record::record_page::RecordPageError, tx::transaction::TransactionError};
use std::fmt::Display;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ScanError {
    #[error("field not found: {0}")]
    FieldNotFound(String),

    #[error("{0:?}")]
    Transaction(#[from] TransactionError),

    #[error("{0:?}")]
    RecordPage(#[from] RecordPageError),

    #[error("{0:?}")]
    UnsupportedOperation(String),
}

pub type Result<T> = core::result::Result<T, ScanError>;

pub trait Scan {
    fn before_first(&mut self) -> Result<()>;
    fn next(&mut self) -> Result<bool>;
    fn get_i32(&self, field_name: &str) -> Result<i32>;
    fn get_string(&self, field_name: &str) -> Result<String>;
    fn get_val(&self, field_name: &str) -> Result<Constant>;
    fn has_field(&self, field_name: &str) -> bool;
    fn close(&mut self);
}

pub trait UpdateScan: Scan {
    fn set_val(&mut self, field_name: &str, value: Constant) -> Result<()>;
    fn set_i32(&mut self, field_name: &str, value: i32) -> Result<()>;
    fn set_string(&mut self, field_name: &str, value: String) -> Result<()>;
    fn insert(&mut self) -> Result<()>;
    fn delete(&mut self) -> Result<()>;

    fn get_rid(&self) -> Result<RID>;
    fn move_to_rid(&mut self, rid: RID) -> Result<()>;
}

#[derive(Debug, PartialEq, Eq)]
pub struct RID {
    blknum: i64,
    slot: Option<i32>,
}

impl RID {
    pub fn new(blknum: i64, slot: Option<i32>) -> Self {
        RID { blknum, slot }
    }

    pub fn from_index(block_num: i32, slot: i32) -> Self {
        let blknum = block_num.into();
        Self {
            blknum,
            slot: Some(slot),
        }
    }

    pub fn block_number(&self) -> i64 {
        self.blknum
    }

    pub fn block_number_as_i32(&self) -> i32 {
        self.blknum.try_into().unwrap()
    }

    pub fn slot(&self) -> Option<i32> {
        self.slot
    }
}

impl Display for RID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}, {:?}]", self.blknum, self.slot)
    }
}

#[cfg(test)]
mod tests {
    use super::RID;

    #[test]
    fn test_rid_basic() {
        let rid = RID::new(-1, Some(0));

        assert_eq!(rid.block_number(), -1);
        assert_eq!(rid.slot(), Some(0));
    }

    #[test]
    fn test_rid_i32() {
        let rid = RID::from_index(i32::MAX, i32::MAX);
        assert_eq!(rid.block_number(), i32::MAX as i64);
        assert_eq!(rid.block_number_as_i32(), i32::MAX);
        assert_eq!(rid.slot(), Some(i32::MAX));
    }
}
