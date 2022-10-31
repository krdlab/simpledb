// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use simpledb::server::simple_db::SimpleDB;
use std::env;

fn main() {
    let curr_dir = env::current_dir().unwrap();
    let db = SimpleDB::new(&curr_dir.join("db"), 400, 8);
    let mut tx = db.new_tx();
    tx.rollback().unwrap();
    println!("OK");
}
