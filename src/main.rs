// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use simpledb::BlockId;

fn main() {
    let b = BlockId::new("", 0);
    println!("{:?}", b);
}
