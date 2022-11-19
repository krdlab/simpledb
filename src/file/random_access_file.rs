// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{Read, Seek, SeekFrom, Write},
    };
    use tempfile::{tempdir, tempfile};

    #[test]
    fn test_seek() {
        let mut f = tempfile().unwrap();
        {
            let buf1 = [1u8, 2u8, 3u8];
            f.seek(SeekFrom::Start(123)).unwrap();
            f.write(&buf1).unwrap();

            let buf2 = [10u8, 20u8, 30u8];
            f.seek(SeekFrom::Start(123)).unwrap();
            f.write(&buf2).unwrap();
        }
        {
            let mut buf = [0u8; 3];
            f.seek(SeekFrom::Start(123)).unwrap();
            f.read(&mut buf).unwrap();
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
                file.seek(SeekFrom::Start(0)).unwrap();
                let b0 = vec![0u8; 400];
                file.write(&b0).unwrap();

                file.seek(SeekFrom::Start(400)).unwrap();
                let b1 = vec![0u8; 400];
                file.write(&b1).unwrap();
            }
            {
                // NOTE: overwrite block 1
                file.seek(SeekFrom::Start(400)).unwrap();
                let b1 = vec![1u8; 400];
                file.write(&b1).unwrap();
            }
            assert_eq!(file.metadata().unwrap().len(), 800);

            {
                // NOTE: append block 2
                file.seek(SeekFrom::Start(800)).unwrap();
                let b1 = vec![0u8; 400];
                file.write(&b1).unwrap();
            }
            assert_eq!(file.metadata().unwrap().len(), 1200);
        }
        dir.close().unwrap();
    }
}
