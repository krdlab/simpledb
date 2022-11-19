// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::byte_buffer::{ByteBuffer, ByteBufferError};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileError {
    #[error("{0:?}")]
    IO(#[from] std::io::Error),

    #[error("{0:?}")]
    Byte(#[from] ByteBufferError),
}

pub type Result<T> = core::result::Result<T, FileError>;

pub trait RandomAccessFile<'p, 'b> {
    fn read_to(
        &mut self,
        pos: SeekFrom,
        buff: &'p mut Box<dyn ByteBuffer + Send + 'b>,
    ) -> Result<()>;
    fn write_from(
        &mut self,
        pos: SeekFrom,
        buff: &'p mut Box<dyn ByteBuffer + Send + 'b>,
    ) -> Result<()>;
}

impl<'p, 'b> RandomAccessFile<'p, 'b> for File {
    fn read_to(
        &mut self,
        pos: SeekFrom,
        buff: &'p mut Box<dyn ByteBuffer + Send + 'b>,
    ) -> Result<()> {
        self.seek(pos)?;

        let rem = buff.get_limit() - buff.get_position();
        let mut bytes = vec![0u8; rem];
        self.read(&mut bytes)?;

        buff.put(&bytes)?;
        Ok(())
    }

    fn write_from(
        &mut self,
        pos: SeekFrom,
        buf: &'p mut Box<dyn ByteBuffer + Send + 'b>,
    ) -> Result<()> {
        self.seek(pos)?;

        let buff_pos = buf.get_position();
        let rem = buf.get_limit() - buff_pos;
        let mut bytes = vec![0u8; rem];
        buf.get(&mut bytes)?;

        self.write(&bytes)?;

        buf.set_position(buff_pos)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::file::page::Page;

    use super::*;
    use std::{fs::File, io::SeekFrom};
    use tempfile::{tempdir, tempfile};

    #[test]
    fn test_seek() {
        let mut file = tempfile().unwrap();
        {
            let mut buf1 = [1u8, 2u8, 3u8];
            file.write_from(
                SeekFrom::Start(123),
                Page::for_log(&mut buf1).contents().unwrap(),
            )
            .unwrap();

            let mut buf2 = [10u8, 20u8, 30u8];
            file.write_from(
                SeekFrom::Start(123),
                Page::for_log(&mut buf2).contents().unwrap(),
            )
            .unwrap();
        }
        {
            let mut buf = [0u8; 3];
            file.read_to(
                SeekFrom::Start(123),
                Page::for_log(&mut buf).contents().unwrap(),
            )
            .unwrap();
            assert_eq!(buf, [10u8, 20u8, 30u8]);
        }
    }

    #[test]
    fn test_write_block() {
        let dir = tempdir().unwrap();
        {
            let path = dir.path().join("test.db");
            let mut file = File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            {
                // NOTE: init 2 blocks
                let mut b0 = vec![0u8; 400];
                file.write_from(
                    SeekFrom::Start(0),
                    Page::for_log(&mut b0).contents().unwrap(),
                )
                .unwrap();

                let mut b1 = vec![0u8; 400];
                file.write_from(
                    SeekFrom::Start(400),
                    Page::for_log(&mut b1).contents().unwrap(),
                )
                .unwrap();
            }
            {
                // NOTE: overwrite block 1
                let mut b1 = vec![1u8; 400];
                file.write_from(
                    SeekFrom::Start(400),
                    Page::for_log(&mut b1).contents().unwrap(),
                )
                .unwrap();
            }
            assert_eq!(file.metadata().unwrap().len(), 800);

            {
                // NOTE: append block 2
                let mut b1 = vec![0u8; 400];
                file.write_from(
                    SeekFrom::Start(800),
                    Page::for_log(&mut b1).contents().unwrap(),
                )
                .unwrap();
            }
            assert_eq!(file.metadata().unwrap().len(), 1200);
        }
        dir.close().unwrap();
    }
}
