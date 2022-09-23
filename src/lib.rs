// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::fmt::Display;

mod buffer_mgr;
mod byte_buffer;
mod constants;
mod file_mgr;
mod log_mgr;
mod page;
mod tx;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockId {
    filename: String,
    blknum: u64,
}

impl BlockId {
    pub fn new(filename: &str, blknum: u64) -> Self {
        BlockId {
            filename: filename.to_owned(),
            blknum,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> u64 {
        self.blknum
    }
}

impl Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let result = BlockId::new("test", 1);
        assert_eq!(result.filename(), "test");
        assert_eq!(result.number(), 1);
    }

    #[test]
    fn test_equivalence() {
        {
            let b1 = BlockId::new("test", 1);
            let b2 = BlockId::new("test", 1);
            assert!(b1 == b2);
            assert!(b2 == b1);
        }
        {
            let b1 = BlockId::new("test", 1);
            let b2 = BlockId::new("test", 1);
            let b3 = BlockId::new("test", 1);
            assert!(b1 == b2);
            assert!(b2 == b3);
            assert!(b3 == b1);
        }
    }

    #[test]
    fn test_non_equivalence() {
        {
            let b1 = BlockId::new("test1", 1);
            let b2 = BlockId::new("test2", 1);
            assert!(b1 != b2);
            assert!(b2 != b1);
        }
        {
            let b1 = BlockId::new("test1", 1);
            let b2 = BlockId::new("test1", 2);
            assert!(b1 != b2);
            assert!(b2 != b1);
        }
    }
}
