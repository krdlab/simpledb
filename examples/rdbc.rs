// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use simpledb::rdbc::api;
use simpledb::rdbc::api::Driver;
use simpledb::rdbc::embedded::EmbeddedDriver;

fn main() {
    let driver = EmbeddedDriver {};
    {
        let mut conn = driver.connect("jdbc:simpledb:exampledb").unwrap();
        {
            let mut s = conn.create_statement().unwrap();
            {
                match s.execute_update("create table test (a int, b varchar(10))", &[]) {
                    Err(api::Error::Internal(e))
                        if e.to_string().contains("TableAlreadyExists") =>
                    {
                        s.execute_update("delete from test", &[]).unwrap();
                    }
                    Err(e) => panic!("{:?}", e),
                    Ok(_) => { /* through */ }
                }
                s.execute_update("insert into test (a, b) values (1, 'aaa')", &[])
                    .unwrap();

                let mut rs = s.execute_query("select b from test", &[]).unwrap();
                while rs.next().unwrap() {
                    println!("{:?}", rs.get_string(0).unwrap());
                }
            }
            {
                let mut rs = s.execute_query("select a from test", &[]).unwrap();
                while rs.next().unwrap() {
                    println!("{:?}", rs.get_i32(0).unwrap());
                }
            }
        }
    }
}
