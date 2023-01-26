// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use simpledb::server::simple_db::SimpleDB;
use std::env;

fn main() {
    let curr_dir = env::current_dir().unwrap();
    let mut db = SimpleDB::new(&curr_dir.join("db"), 400, 8);
    db.init();
    {
        let tx = db.new_tx();
        tx.borrow_mut().rollback().unwrap();
    }
    println!("OK");
}
