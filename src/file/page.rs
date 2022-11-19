// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::byte_buffer::{AllocatedBuffer, ByteBuffer, ByteBufferError, WrappedBuffer};
use std::string::FromUtf8Error;
use std::vec;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageError {
    #[error("{0:?}")]
    Byte(#[from] ByteBufferError),

    #[error("{0:?}")]
    InvalidUtf8(#[from] FromUtf8Error),
}

pub type Result<T> = core::result::Result<T, PageError>;

pub struct Page<'a> {
    buf: Box<dyn ByteBuffer + Send + 'a>,
}

impl<'a> Page<'a> {
    pub fn for_data(blocksize: usize) -> Self {
        Page {
            buf: Box::new(AllocatedBuffer::new(blocksize)),
        }
    }

    pub fn for_log(bytes: &'a mut [u8]) -> Self {
        Page {
            buf: Box::new(WrappedBuffer::new(bytes)),
        }
    }

    pub fn set_i32(&mut self, offset: usize, n: i32) -> Result<()> {
        Ok(self.buf.put_i32_to(offset, n)?)
    }

    pub fn get_i32(&self, offset: usize) -> Result<i32> {
        Ok(self.buf.get_i32_from(offset)?)
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        self.buf.set_position(offset)?;
        self.buf.put_i32(bytes.len().try_into().unwrap())?;
        Ok(self.buf.put(bytes)?)
    }

    pub fn get_bytes(&mut self, offset: usize) -> Result<Vec<u8>> {
        self.buf.set_position(offset)?;

        let len = self.buf.get_i32()?.try_into().unwrap();
        let mut res = vec![0u8; len];
        self.buf.get(&mut res)?;
        Ok(res)
    }

    pub fn set_string(&mut self, offset: usize, s: &str) -> Result<()> {
        let bs = s.as_bytes();
        self.set_bytes(offset, bs)
    }

    pub fn get_string(&mut self, offset: usize) -> Result<String> {
        let bs = self.get_bytes(offset)?;
        match String::from_utf8(bs) {
            Ok(str) => Ok(str),
            Err(err) => Err(PageError::InvalidUtf8(err)),
        }
    }

    pub(crate) fn max_length(strlen: usize) -> usize {
        let bytes_per_char: usize = 4; // TODO
        4 + strlen * bytes_per_char
    }

    pub(crate) fn contents(&mut self) -> Result<&mut Box<dyn ByteBuffer + Send + 'a>> {
        self.buf.set_position(0)?;
        Ok(&mut self.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_incomplete() {
        let mut p = Page::for_data(1);

        let result = p.set_i32(0, 0x1234);
        assert_eq!(result.is_err(), true);
        assert_eq!(
            result.unwrap_err().to_string(),
            format!("{:?}", ByteBufferError(byte::Error::Incomplete))
        );
    }

    #[test]
    fn test_for_data_set_and_get_i32() -> Result<()> {
        let mut p = Page::for_data(8);

        p.set_i32(3, 0x12345678)?;

        assert_eq!(p.get_i32(3)?, 0x12345678);
        Ok(())
    }

    #[test]
    fn test_for_data_set_and_get_bytes() -> Result<()> {
        let mut p = Page::for_data(10);

        let bytes = [0x1, 0x2, 0x3];
        p.set_bytes(3, &bytes)?;

        assert_eq!(p.get_bytes(3)?, bytes);
        Ok(())
    }

    #[test]
    fn test_for_data_set_and_get_string() -> Result<()> {
        let mut p = Page::for_data(40);

        p.set_string(0, "abcd")?;
        p.set_string(20, "efgh")?;

        assert_eq!(p.get_string(0)?, "abcd");
        assert_eq!(p.get_string(20)?, "efgh");
        Ok(())
    }
}
