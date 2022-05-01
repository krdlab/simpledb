// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::io::{Read, Write};
use std::vec;
use std::{cell::RefCell, fs::File, rc::Rc};

use crate::byte_buffer::*;

pub struct Page<'a> {
    buf: Rc<RefCell<Box<dyn ByteBuffer + 'a>>>,
}

impl<'a> Page<'a> {
    pub fn for_data(blocksize: usize) -> Self {
        Page {
            buf: Rc::new(RefCell::new(Box::new(AllocatedBuffer::new(blocksize)))),
        }
    }

    pub fn for_log(bytes: &'a mut [u8]) -> Self {
        Page {
            buf: Rc::new(RefCell::from(Box::new(WrappedBuffer::new(bytes)))),
        }
    }

    pub fn set_i32(&mut self, offset: usize, n: i32) {
        self.buf.borrow_mut().put_i32_to(offset, n).unwrap()
    }

    pub fn get_i32(&self, offset: usize) -> i32 {
        self.buf.borrow().get_i32_from(offset).unwrap()
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        let mut bb = self.buf.borrow_mut();
        bb.set_position(offset).unwrap();
        bb.put_i32(bytes.len().try_into().unwrap()).unwrap();
        bb.put(bytes).unwrap()
    }

    pub fn get_bytes(&mut self, offset: usize) -> Vec<u8> {
        let mut bb = self.buf.borrow_mut();
        bb.set_position(offset).unwrap();
        let len = bb.get_i32().unwrap().try_into().unwrap();
        let mut res = vec![0u8; len];
        bb.get(&mut res).unwrap();
        res
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        let bs = s.as_bytes();
        self.set_bytes(offset, bs)
    }

    pub fn get_string(&mut self, offset: usize) -> String {
        let bs = self.get_bytes(offset);
        String::from_utf8(bs).unwrap()
    }

    fn max_length(strlen: usize) -> usize {
        let bytes_per_char: usize = 4; // TODO
        4 + strlen * bytes_per_char
    }

    fn contents(&mut self) -> Rc<RefCell<Box<dyn ByteBuffer + 'a>>> {
        self.buf.borrow_mut().set_position(0).unwrap();
        self.buf.clone()
    }

    pub(in crate) fn read_from_file(&self, file: Rc<RefCell<File>>) {
        let mut buf = self.buf.borrow_mut();
        let rem = buf.get_limit() - buf.get_position();
        let mut bytes = vec![0u8; rem];
        file.borrow_mut().read(&mut bytes).unwrap();
        buf.put(&bytes).unwrap();
    }

    pub(in crate) fn write_to_file(&self, file: Rc<RefCell<File>>) {
        let mut buf = self.buf.borrow_mut();
        let rem = buf.get_limit() - buf.get_position();
        let mut bytes = vec![0u8; rem];
        buf.get(&mut bytes).unwrap();
        file.borrow_mut().write(&bytes).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_data_set_and_get_i32() {
        let mut p = Page::for_data(8);

        p.set_i32(3, 0x12345678);

        assert_eq!(p.get_i32(3), 0x12345678);
    }

    #[test]
    fn test_for_data_set_and_get_bytes() {
        let mut p = Page::for_data(10);

        let bytes = [0x1, 0x2, 0x3];
        p.set_bytes(3, &bytes);

        assert_eq!(p.get_bytes(3), bytes);
    }

    #[test]
    fn test_for_data_set_and_get_string() {
        let mut p = Page::for_data(40);

        p.set_string(0, "abcd");
        p.set_string(20, "efgh");

        assert_eq!(p.get_string(0), "abcd");
        assert_eq!(p.get_string(20), "efgh");
    }
}
