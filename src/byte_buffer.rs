// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use byte::ctx::Bytes;
use byte::*;

pub trait ByteBuffer {
    fn get_limit(&self) -> usize;

    fn get_position(&self) -> usize;
    fn set_position(&mut self, pos: usize) -> byte::Result<()>;

    fn get_i32_from(&self, pos: usize) -> byte::Result<i32>;
    fn put_i32_to(&mut self, pos: usize, n: i32) -> byte::Result<()>;

    fn get_i32(&mut self) -> byte::Result<i32>;
    fn put_i32(&mut self, n: i32) -> byte::Result<()>;
    fn get(&mut self, dst: &mut [u8]) -> byte::Result<()>;
    fn put(&mut self, src: &[u8]) -> byte::Result<()>;
}

pub struct AllocatedBuffer {
    buf: Vec<u8>,
    pos: usize,
}
pub struct WrappedBuffer<'a> {
    buf: &'a mut [u8],
    pos: usize,
}

impl AllocatedBuffer {
    pub fn new(capacity: usize) -> Self {
        AllocatedBuffer {
            buf: vec![0u8; capacity],
            pos: 0,
        }
    }
}

impl ByteBuffer for AllocatedBuffer {
    fn get_limit(&self) -> usize {
        self.buf.len()
    }

    fn get_position(&self) -> usize {
        self.pos
    }

    fn set_position(&mut self, p: usize) -> byte::Result<()> {
        if p > self.buf.len() {
            return Err(byte::Error::BadOffset(p));
        }
        self.pos = p;
        Ok(())
    }

    fn get_i32_from(&self, mut pos: usize) -> byte::Result<i32> {
        check_len(&self.buf, pos + 4)?;
        self.buf.read_with(&mut pos, BE)
    }

    fn put_i32_to(&mut self, mut pos: usize, n: i32) -> byte::Result<()> {
        check_len(&self.buf, pos + 4)?;
        self.buf.write_with(&mut pos, n, BE)
    }

    fn get_i32(&mut self) -> byte::Result<i32> {
        check_len(&self.buf, self.pos + 4)?;
        self.buf.read_with(&mut self.pos, BE)
    }

    fn put_i32(&mut self, n: i32) -> byte::Result<()> {
        check_len(&self.buf, self.pos + 4)?;
        self.buf.write_with(&mut self.pos, n, BE)
    }

    fn get(&mut self, dst: &mut [u8]) -> byte::Result<()> {
        check_len(&self.buf, self.pos + dst.len())?;
        let bs = self
            .buf
            .read_with::<&[u8]>(&mut self.pos, Bytes::Len(dst.len()))?;
        dst.write::<&[u8]>(&mut 0, bs)?;
        Ok(())
    }

    fn put(&mut self, src: &[u8]) -> byte::Result<()> {
        check_len(&self.buf, self.pos + src.len())?;
        self.buf.write::<&[u8]>(&mut self.pos, src)?;
        Ok(())
    }
}

impl<'a> WrappedBuffer<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        WrappedBuffer { buf: bytes, pos: 0 }
    }
}

impl<'a> ByteBuffer for WrappedBuffer<'a> {
    fn get_limit(&self) -> usize {
        self.buf.len()
    }

    fn get_position(&self) -> usize {
        self.pos
    }

    fn set_position(&mut self, p: usize) -> byte::Result<()> {
        if p > self.buf.len() {
            return Err(byte::Error::BadOffset(p));
        }
        self.pos = p;
        Ok(())
    }

    fn get_i32_from(&self, mut pos: usize) -> byte::Result<i32> {
        check_len(&self.buf, pos + 4)?;
        self.buf.read_with(&mut pos, BE)
    }

    fn put_i32_to(&mut self, mut pos: usize, n: i32) -> byte::Result<()> {
        check_len(&self.buf, pos + 4)?;
        self.buf.write_with(&mut pos, n, BE)
    }

    fn get_i32(&mut self) -> byte::Result<i32> {
        check_len(&self.buf, self.pos + 4)?;
        self.buf.read_with(&mut self.pos, BE)
    }

    fn put_i32(&mut self, n: i32) -> byte::Result<()> {
        check_len(&self.buf, self.pos + 4)?;
        self.buf.write_with(&mut self.pos, n, BE)
    }

    fn get(&mut self, dst: &mut [u8]) -> byte::Result<()> {
        check_len(&self.buf, self.pos + dst.len())?;
        let bs = self
            .buf
            .read_with::<&[u8]>(&mut self.pos, Bytes::Len(dst.len()))?;
        dst.write::<&[u8]>(&mut 0, bs)?;
        Ok(())
    }

    fn put(&mut self, src: &[u8]) -> byte::Result<()> {
        check_len(&self.buf, self.pos + src.len())?;
        self.buf.write::<&[u8]>(&mut self.pos, src)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{AllocatedBuffer, ByteBuffer, WrappedBuffer};

    #[test]
    fn test_allocated_buffer_set_position() -> byte::Result<()> {
        let mut b = AllocatedBuffer::new(10);
        b.set_position(5)?;
        assert_eq!(b.get_position(), 5);
        Ok(())
    }

    #[test]
    fn test_allocated_buffer_uses_i32() -> byte::Result<()> {
        let mut b = AllocatedBuffer::new(10);
        test_put_and_get_i32(&mut b)
    }

    #[test]
    fn test_allocated_buffer_uses_bytes() -> byte::Result<()> {
        let mut b = AllocatedBuffer::new(10);
        test_put_and_get_bytes(&mut b)
    }

    #[test]
    fn test_wrapped_buffer_set_position() -> byte::Result<()> {
        let mut buf = [0u8; 10];
        let mut b = WrappedBuffer::new(&mut buf);
        b.set_position(5)?;
        assert_eq!(b.get_position(), 5);
        Ok(())
    }

    #[test]
    fn test_wrapped_buffer_uses_i32() -> byte::Result<()> {
        let mut buf = [0u8; 10];
        {
            let mut b = WrappedBuffer::new(&mut buf);
            test_put_and_get_i32(&mut b)?;
        }
        assert_eq!(buf, [0x12, 0x34, 0x56, 0x12, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0]);
        Ok(())
    }

    #[test]
    fn test_wrapped_buffer_uses_bytes() -> byte::Result<()> {
        let mut buf = [0u8; 10];
        {
            let mut b = WrappedBuffer::new(&mut buf);
            test_put_and_get_bytes(&mut b)?;
        }
        assert_eq!(buf, [0x1, 0x2, 0x3, 0x4, 0x5, 0x0, 0x0, 0x0, 0x0, 0x0]);
        Ok(())
    }

    fn test_put_and_get_i32<'a, B>(b: &mut B) -> byte::Result<()>
    where
        B: ByteBuffer,
    {
        b.put_i32(0x12345678)?;
        assert_eq!(b.get_position(), 4);

        assert_eq!(b.get_i32()?, 0);
        assert_eq!(b.get_position(), 8);

        assert_eq!(b.get_i32_from(0)?, 0x12345678);
        assert_eq!(b.get_position(), 8);

        b.put_i32_to(3, 0x12000000)?;
        assert_eq!(b.get_position(), 8);
        assert_eq!(b.get_i32_from(0)?, 0x12345612);

        Ok(())
    }

    fn test_put_and_get_bytes<'a, B>(b: &mut B) -> byte::Result<()>
    where
        B: ByteBuffer,
    {
        let src: [u8; 5] = [1, 2, 3, 4, 5];
        b.put(&src)?;
        assert_eq!(b.get_position(), src.len());

        b.set_position(2)?;
        assert_eq!(b.get_position(), 2);

        let mut dst = [0u8; 3];
        b.get(&mut dst)?;
        assert_eq!(dst, [3, 4, 5]);
        assert_eq!(b.get_position(), 5);

        Ok(())
    }
}
