// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

mod byte_buffer;
mod page;

#[derive(Debug, PartialEq, Eq)]
pub struct BlockId {
    filename: String,
    blknum: i32,
}

impl BlockId {
    pub fn new(filename: &str, blknum: i32) -> Self {
        BlockId {
            filename: filename.to_owned(),
            blknum,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> i32 {
        self.blknum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let result = BlockId::new("test", 1);
        assert_eq!(result.filename(), "test");
        assert_eq!(result.number(), 1);
    }
}
