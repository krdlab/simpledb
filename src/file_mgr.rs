// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    byte_buffer::{ByteBuffer, ByteBufferError},
    page::{Page, PageError},
    BlockId,
};
use std::{
    collections::{hash_map::Entry, HashMap},
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::{Path, PathBuf},
    sync::Mutex,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FileMgrError {
    #[error("{0:?}")]
    IO(#[from] std::io::Error),

    #[error("{0:?}")]
    Byte(#[from] ByteBufferError),

    #[error("{0:?}")]
    Page(#[from] PageError),
}

pub type Result<T> = core::result::Result<T, FileMgrError>;

// TODO: rename this to FileExt and move into fileext.rs?
trait FileChannel<'p, 'b> {
    fn read_to(&mut self, buff: &'p mut Box<dyn ByteBuffer + 'b>) -> Result<()>;
    fn write_from(&mut self, buff: &'p mut Box<dyn ByteBuffer + 'b>) -> Result<()>;
}

impl<'p, 'b> FileChannel<'p, 'b> for File {
    fn read_to(&mut self, buff: &'p mut Box<dyn ByteBuffer + 'b>) -> Result<()> {
        let rem = buff.get_limit() - buff.get_position();
        let mut bytes = vec![0u8; rem];
        self.read(&mut bytes)?;

        buff.put(&bytes)?;
        Ok(())
    }

    fn write_from(&mut self, buf: &'p mut Box<dyn ByteBuffer + 'b>) -> Result<()> {
        let pos = buf.get_position();
        let rem = buf.get_limit() - pos;
        let mut bytes = vec![0u8; rem];
        buf.get(&mut bytes)?;

        self.write(&bytes)?;

        buf.set_position(pos)?;
        Ok(())
    }
}

pub struct FileMgr {
    blocksize: usize,
    is_new: bool,
    data: Mutex<FileMgrData>,
}

struct FileMgrData {
    db_dir_path: PathBuf,
    blocksize: usize,
    open_files: HashMap<String, File>,
}

impl FileMgr {
    pub fn new(db_dir_path: &Path, blocksize: usize) -> Self {
        let is_new = !db_dir_path.exists();
        if is_new {
            fs::create_dir_all(db_dir_path).expect("failed to create db directory");
        }
        FileMgr {
            blocksize,
            is_new,
            data: Mutex::new(FileMgrData::new(db_dir_path.to_path_buf(), blocksize)),
        }
    }

    pub fn blocksize(&self) -> usize {
        self.blocksize
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn read(&self, block: &BlockId, page: &mut Page) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.read(block, page)
    }

    pub fn write(&self, block: &BlockId, page: &mut Page) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        data.write(block, page)
    }

    pub fn append(&self, filename: &str) -> Result<BlockId> {
        let mut data = self.data.lock().unwrap();
        data.append(filename)
    }

    pub(in crate) fn length(&self, filename: &str) -> Result<u64> {
        let mut data = self.data.lock().unwrap();
        data.length(filename)
    }
}

impl FileMgrData {
    pub(in crate) fn new(db_dir_path: PathBuf, blocksize: usize) -> Self {
        Self {
            db_dir_path,
            blocksize,
            open_files: HashMap::new(),
        }
    }

    fn open_file(path: &Path) -> Result<File> {
        if path.exists() {
            Ok(File::options().read(true).append(true).open(path)?)
        } else {
            Ok(File::options()
                .read(true)
                .write(true)
                .create(true)
                .open(path)?)
        }
    }

    fn get_file(&mut self, filename: &str) -> Result<&mut File> {
        let file = match self.open_files.entry(filename.to_string()) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let path = self.db_dir_path.join(filename);
                let file = Self::open_file(&path)?;
                v.insert(file)
            }
        };
        Ok(file)
    }

    fn calc_seek_pos(
        blocksize: usize,
        block: &BlockId,
    ) -> std::result::Result<SeekFrom, TryFromIntError> {
        let blocksize = u64::try_from(blocksize)?;
        Ok(SeekFrom::Start(block.number() * blocksize))
    }

    fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let pos = FileMgrData::calc_seek_pos(self.blocksize, block).unwrap();

        let file = self.get_file(block.filename())?;
        file.seek(pos)?;

        file.read_to(page.contents()?)?;
        Ok(())
    }

    fn write(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let pos = FileMgrData::calc_seek_pos(self.blocksize, block).unwrap();

        let f = self.get_file(block.filename())?;
        f.seek(pos)?;

        f.write_from(page.contents()?)?;
        Ok(())
    }

    fn append(&mut self, filename: &str) -> Result<BlockId> {
        let blocksize = self.blocksize;
        let newblocknum = self.length(filename)?;
        let block = BlockId::new(filename, newblocknum);

        let file = self.get_file(filename)?;
        let pos = FileMgrData::calc_seek_pos(blocksize, &block).unwrap();
        file.seek(pos)?;

        let b = vec![0u8; blocksize];
        file.write(&b)?;

        Ok(block)
    }

    fn length(&mut self, filename: &str) -> Result<u64> {
        let blocksize = u64::try_from(self.blocksize).unwrap();

        let file = self.get_file(filename)?;
        Ok(file.metadata()?.len() / blocksize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const TEST_FILE: &str = "test.db";

    // #[test]
    // fn test_write() -> Result<()> {
    //     let dir = tempdir()?;
    //     assert_eq!(dir.path().exists(), true);
    //     let fm = FileMgr::new(dir.path(), 4096);

    //     fm.write(block, page)?;

    //     dir.close()?;
    //     Ok(())
    // }

    #[test]
    fn test_is_new_if_dir_exists() -> Result<()> {
        let dir = tempdir()?;
        assert_eq!(dir.path().exists(), true);

        let fm = FileMgr::new(dir.path(), 4096);
        assert_eq!(fm.is_new(), false);

        dir.close()?;
        Ok(())
    }

    #[test]
    fn test_is_new_if_dir_not_exists() -> Result<()> {
        let tmp_dir = tempdir()?;
        let dir_path = tmp_dir.path().join("new-dir");
        assert_eq!(dir_path.exists(), false);

        let fm = FileMgr::new(&dir_path, 4096);
        assert_eq!(fm.is_new(), true);
        assert_eq!(dir_path.exists(), true);

        tmp_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_append() -> Result<()> {
        let db_dir = tempdir()?;

        let fm = FileMgr::new(db_dir.path(), 4096);
        let block = fm.append(TEST_FILE)?;

        assert_eq!(block.number(), 0);

        db_dir.close()?;
        Ok(())
    }

    #[test]
    fn test_append_double() -> Result<()> {
        let db_dir = tempdir()?;

        let fm = FileMgr::new(db_dir.path(), 4096);
        let _ = fm.append(TEST_FILE)?;
        let block = fm.append(TEST_FILE)?;

        assert_eq!(block.number(), 1);

        db_dir.close()?;
        Ok(())
    }
}
