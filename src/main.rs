// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use simpledb::record::schema::SqlType;
use simpledb::server::simple_db::SimpleDB;
use std::env;
use std::io;

fn main() {
    let curr_dir = env::current_dir().unwrap();
    let mut db = SimpleDB::new(&curr_dir.join("db"), 400, 8);
    db.init();
    {
        let mut line = String::new();
        while io::stdin().read_line(&mut line).is_ok() {
            line = line.trim_end().into();
            if line == "quit" {
                break;
            } else {
                run_sql(&db, &line);
            }
            line.clear();
        }
    }
    println!("OK");
}

fn run_sql(db: &SimpleDB, line: &String) {
    let planner = db.planner();
    let tx = db.new_tx();
    let count = {
        if let Ok(p) = planner.create_query_plan(line, tx.clone()) {
            let mut count = 0;
            let schema = p.schema();

            let header = schema
                .fields_iter()
                .map(|f| f.into())
                .collect::<Vec<String>>()
                .join(" | ");
            println!("{}", header);

            let mut s = p.open();
            while s.next().unwrap() {
                let record = schema
                    .fields_iter()
                    .map(|f| match schema.field_type(f) {
                        Some(SqlType::Integer) => s.get_i32(f).unwrap().to_string(),
                        Some(SqlType::VarChar) => s.get_string(f).unwrap(),
                        _ => format!("unknown {}", f),
                    })
                    .collect::<Vec<String>>()
                    .join(" | ");
                println!("{}", record);
                count += 1;
            }
            count
        } else {
            planner.execute_update(line, tx.clone()).unwrap()
        }
    };
    tx.borrow_mut().commit().unwrap();
    print_affected(count);
}

fn print_affected(count: i32) {
    println!(
        "Query OK, {} {} affected",
        count,
        (if count < 2 { "row" } else { "rows" })
    );
}
