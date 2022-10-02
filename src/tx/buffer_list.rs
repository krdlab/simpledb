// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    buffer_mgr::{Buffer, BufferMgr, Result},
    BlockId,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub(crate) struct BufferList<'b, 'lm> {
    buffers: HashMap<BlockId, Arc<Mutex<Buffer<'b, 'lm>>>>,
    pins: Vec<BlockId>,
    bm: Arc<BufferMgr<'b, 'lm>>,
}

impl<'b, 'lm> BufferList<'b, 'lm> {
    pub(crate) fn new(bm: Arc<BufferMgr<'b, 'lm>>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            bm,
        }
    }

    pub(crate) fn get_buffer(&self, block: &BlockId) -> Option<&Arc<Mutex<Buffer<'b, 'lm>>>> {
        self.buffers.get(block)
    }

    pub(crate) fn pin(&mut self, block: &BlockId) -> Result<()> {
        let buff = self.bm.pin(block)?;
        self.buffers.insert(block.clone(), buff);
        self.pins.push(block.clone());
        Ok(())
    }

    fn remove_from_pins(&mut self, block: &BlockId) {
        let index = self.pins.iter().position(|b| *b == *block);
        if let Some(i) = index {
            self.pins.remove(i);
        }
    }

    pub(crate) fn unpin(&mut self, block: &BlockId) {
        let buff = self.buffers.get(block).unwrap();
        self.bm.unpin(buff.clone());
        self.remove_from_pins(block);
        if !self.pins.contains(block) {
            self.buffers.remove(block);
        }
    }

    pub(crate) fn unpin_all(&mut self) {
        for block in self.pins.iter() {
            let buff = self.buffers.get(block).unwrap();
            self.bm.unpin(buff.clone());
        }
        self.buffers.clear();
        self.pins.clear();
    }
}
