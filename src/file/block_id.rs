// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    blknum: i64,
}

impl BlockId {
    pub fn new(filename: &str, blknum: i64) -> Self {
        BlockId {
            filename: filename.to_owned(),
            blknum,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> i64 {
        self.blknum
    }

    pub fn number_as_u64(&self) -> u64 {
        self.blknum.try_into().unwrap()
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
