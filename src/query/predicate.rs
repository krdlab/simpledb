// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::scan::Scan;

#[derive(Debug)]
pub enum Constant {
    Int(i32),
    String(String),
}

// TODO: implement Predicate
pub struct Predicate {}
impl Predicate {
    pub fn is_satisfied<S: Scan>(&self, scan: &S) -> bool {
        false // TODO
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}
