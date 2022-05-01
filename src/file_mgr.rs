// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File},
    io::{Result, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Mutex,
};

use crate::{page::Page, BlockId};

pub struct FileMgr {
    db_dir_path: PathBuf,
    blocksize: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, Rc<RefCell<File>>>>,
}

impl FileMgr {
    pub fn new(db_dir_path: &Path, blocksize: usize) -> Self {
        let is_new = !db_dir_path.exists();
        if is_new {
            fs::create_dir_all(db_dir_path).unwrap();
        }
        FileMgr {
            db_dir_path: db_dir_path.to_path_buf(),
            blocksize,
            is_new,
            open_files: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    fn get_file(&self, filename: &str) -> Result<Rc<RefCell<File>>> {
        let mut ofs = self
            .open_files
            .lock()
            .expect("failed to lock the map of opened files");
        let file = if let Some(file) = ofs.get_mut(filename) {
            file.clone()
        } else {
            let path = self.db_dir_path.join(filename);
            let file = if path.exists() {
                Rc::new(RefCell::new(File::open(path)?))
            } else {
                Rc::new(RefCell::new(File::create(path)?))
            };
            ofs.insert(filename.to_string(), file.clone());
            file.clone()
        };
        return Ok(file);
    }

    fn calc_seek_pos(&self, block: &BlockId) -> std::result::Result<SeekFrom, TryFromIntError> {
        let blocksize = u64::try_from(self.blocksize)?;
        Ok(SeekFrom::Start(block.number() * blocksize))
    }

    pub fn read(&self, block: &BlockId, page: &Page) -> Result<()> {
        let _ = self.open_files.lock().unwrap();
        let file = self.get_file(block.filename())?;

        let pos = self.calc_seek_pos(block).unwrap();
        file.borrow_mut().seek(pos)?;
        page.read_from_file(file.clone());
        Ok(())
    }

    pub fn write(&self, block: &BlockId, page: &Page) -> Result<()> {
        let _ = self.open_files.lock().unwrap();
        let f = self.get_file(block.filename())?;

        let pos = self.calc_seek_pos(block).unwrap();
        f.borrow_mut().seek(pos)?;
        page.write_to_file(f.clone());
        Ok(())
    }

    pub fn append(&self, filename: &str) -> Result<BlockId> {
        let _ = self.open_files.lock().unwrap();

        let newblocknum = self.length(filename)?;
        let block = BlockId::new(filename, newblocknum);

        let b = vec![0u8; self.blocksize];
        let file = self.get_file(filename)?;
        let blocksize = u64::try_from(self.blocksize).unwrap();
        file.borrow_mut()
            .seek(SeekFrom::Start(block.number() * blocksize))?;
        file.borrow_mut().write(&b)?;

        Ok(block)
    }

    fn length(&self, filename: &str) -> Result<u64> {
        let blocksize = u64::try_from(self.blocksize).unwrap();
        let f = self.get_file(filename)?;
        Ok(f.clone().borrow().metadata()?.len() / blocksize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const TEST_FILE: &str = "test.db";

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
