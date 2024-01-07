// Copyright (c) 2024 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::{
    btree_dir::BTreeDir,
    btree_leaf::BTreeLeaf,
    btree_page::{BTreePage, Result},
    Index,
};
use crate::{
    file::block_id::BlockId,
    query::predicate::Constant,
    record::schema::{Layout, Schema},
    tx::transaction::Transaction,
};
use std::{cell::RefCell, rc::Rc};

pub(crate) struct BTreeIndex<'lm, 'bm> {
    dir_layout: Layout,
    leaf_layout: Layout,
    leaf_table: String,
    leaf: Option<BTreeLeaf<'lm, 'bm>>,
    root_block_id: BlockId,
}

impl<'lm, 'bm> BTreeIndex<'lm, 'bm> {
    pub fn new(
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        index_name: String,
        leaf_layout: Layout,
    ) -> Result<Self> {
        let leaf_table = format!("{index_name}_leaf");
        if tx.borrow().size(&leaf_table)? == 0 {
            let block_id = tx.borrow_mut().append(&leaf_table)?;
            let node = BTreePage::new(tx.clone(), block_id.clone(), leaf_layout.clone())?;
            node.format(&block_id, -1)?;
        }

        let mut dir_schema = Schema::new();
        dir_schema.add_field_from("block", leaf_layout.schema());
        dir_schema.add_field_from("dataval", leaf_layout.schema());
        let dir_table = format!("{index_name}_dir");
        let dir_layout = Layout::new(dir_schema);
        let root_block_id = BlockId::new(&dir_table, 0);

        if tx.borrow().size(&dir_table)? == 0 {
            tx.borrow_mut().append(&dir_table)?;
            let node = BTreePage::new(tx.clone(), root_block_id.clone(), dir_layout.clone())?;
            node.format(&root_block_id, 0)?;
            let field_type = dir_layout.schema().field_type("dataval").unwrap(); // TODO
            let min_val = match field_type {
                crate::record::schema::SqlType::Integer => Constant::Int(i32::MIN),
                crate::record::schema::SqlType::VarChar => Constant::String("".to_owned()),
            };
            node.insert_dir(0, min_val, 0)?;
        }

        Ok(Self {
            dir_layout,
            leaf_layout,
            leaf_table,
            leaf: None,
            root_block_id,
        })
    }

    pub fn search_cost(num_blocks: usize, rpb: usize) -> usize {
        1 + ((num_blocks as f64).ln() / (rpb as f64).ln()) as usize // TODO
    }
}

impl<'lm, 'bm> Index<'lm, 'bm> for BTreeIndex<'lm, 'bm> {
    fn before_first(&mut self, tx: Rc<RefCell<Transaction<'lm, 'bm>>>, search_key: Constant) {
        self.close();
        let mut root = BTreeDir::new(
            tx.clone(),
            self.root_block_id.clone(),
            self.dir_layout.clone(),
        )
        .unwrap();
        let block_num = root.search(search_key.clone()).unwrap();
        root.close();
        let leaf_block_id = BlockId::new(&self.leaf_table, block_num as i64);
        self.leaf =
            Some(BTreeLeaf::new(tx, leaf_block_id, self.leaf_layout.clone(), search_key).unwrap());
    }

    fn next(&mut self) -> super::Result<bool> {
        self.next()
    }

    fn rid(&self) -> super::Result<crate::query::scan::RID> {
        Ok(self
            .leaf
            .as_ref()
            .map(|leaf| leaf.get_data_rid())
            .unwrap()?) // TODO
    }

    fn insert(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: Constant,
        rid: crate::query::scan::RID,
    ) -> super::Result<()> {
        self.before_first(tx.clone(), val);

        let leaf = self.leaf.as_mut().unwrap();
        let e = leaf.insert(rid)?;
        leaf.close();
        let entry1 = if let Some(_e) = e {
            _e
        } else {
            return Ok(());
        };

        let root = BTreeDir::new(
            tx.clone(),
            self.root_block_id.clone(),
            self.dir_layout.clone(),
        )?;
        if let Some(e) = root.insert(entry1)? {
            root.make_new_root(e)?;
        }

        Ok(())
    }

    fn delete(
        &mut self,
        tx: Rc<RefCell<Transaction<'lm, 'bm>>>,
        val: Constant,
        rid: crate::query::scan::RID,
    ) -> super::Result<()> {
        self.before_first(tx, val);
        let leaf = self.leaf.as_mut().unwrap();
        leaf.delete(rid)?;
        leaf.close();
        Ok(())
    }

    fn close(&mut self) {
        if let Some(leaf) = self.leaf.as_mut() {
            leaf.close();
            self.leaf = None;
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // TODO:
    }
}
