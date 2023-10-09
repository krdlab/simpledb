// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    query::{
        predicate::Constant,
        scan::{ScanError, RID},
    },
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("{0}")]
    ScanFailed(#[from] ScanError),
}

pub type Result<T> = std::result::Result<T, IndexError>;

pub trait Index<'lm, 'bm> {
    fn before_first(&mut self, tx: Rc<RefCell<Transaction<'lm, 'bm>>>, search_key: Constant);
    fn next(&mut self) -> Result<bool>;
    fn rid(&self) -> Result<RID>;
    fn insert(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: Constant,
        rid: RID,
    ) -> Result<()>;
    fn delete(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: Constant,
        rid: RID,
    ) -> Result<()>;
    fn close(&mut self);
}

mod hash;
