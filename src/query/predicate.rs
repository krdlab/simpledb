// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::fmt::Display;

use crate::record::schema::Schema;

use super::scan::Scan;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Constant {
    Int(i32),
    String(String),
}

#[derive(Debug, Clone)]
pub enum Expression {
    Constant(Constant),
    FieldName(String),
}

impl Expression {
    pub fn evaluate<S: Scan>(&self, s: &mut S) -> Constant {
        match self {
            Self::Constant(val) => val.clone(),
            Self::FieldName(fname) => s.get_val(fname.as_str()).unwrap(),
        }
    }

    pub fn is_field_name(&self) -> bool {
        match self {
            Self::Constant(_) => false,
            Self::FieldName(_) => true,
        }
    }

    pub fn applyTo(&self, schema: &Schema) -> bool {
        match self {
            Self::Constant(_) => true,
            Self::FieldName(fname) => schema.has_field(fname),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }

    pub fn is_satisfied<S: Scan>(&self, s: &mut S) -> bool {
        let lval = self.lhs.evaluate(s);
        let rval = self.rhs.evaluate(s);
        lval == rval
    }

    /*
    pub fn reduction_factor(p: Plan) -> i32 {
        // TODO
    }
    */

    // F = c
    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        if let Expression::FieldName(fname) = &self.lhs {
            if fname == field_name {
                if let Expression::Constant(v) = &self.rhs {
                    return Some(v.clone());
                }
            }
        }
        if let Expression::FieldName(fname) = &self.rhs {
            if fname == field_name {
                if let Expression::Constant(v) = &self.lhs {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        if let Expression::FieldName(fname) = &self.lhs {
            if fname == field_name {
                if let Expression::FieldName(v) = &self.rhs {
                    return Some(v.clone());
                }
            }
        }
        if let Expression::FieldName(fname) = &self.rhs {
            if fname == field_name {
                if let Expression::FieldName(v) = &self.lhs {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    pub fn applyTo(&self, schema: &Schema) -> bool {
        self.lhs.applyTo(schema) && self.rhs.applyTo(schema)
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} = {:?}", self.lhs, self.rhs)
    }
}

#[derive(Debug)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn empty() -> Self {
        Self { terms: Vec::new() }
    }

    pub fn new(t: Term) -> Self {
        Self { terms: vec![t] }
    }

    pub fn conjoin_with(&mut self, mut pred: Predicate) {
        self.terms.append(&mut pred.terms);
    }

    pub fn is_satisfied<S: Scan>(&self, scan: &mut S) -> bool {
        for t in self.terms.iter() {
            if !t.is_satisfied(scan) {
                return false;
            }
        }
        true
    }

    /*
    pub fn reduction_factor(&self, p: Plan) -> i32 {
        // TODO
    }
    */

    pub fn select_sub_pred(&self, schema: &Schema) -> Option<Predicate> {
        let mut result = Predicate::empty();
        for t in self.terms.iter() {
            if t.applyTo(schema) {
                result.terms.push(t.clone());
            }
        }
        if result.terms.len() == 0 {
            None
        } else {
            Some(result)
        }
    }

    pub fn join_sub_pred(&self, schema1: &Schema, schema2: &Schema) -> Option<Predicate> {
        let mut new_schema = Schema::new();
        new_schema.add_all(&schema1);
        new_schema.add_all(&schema2);

        let mut result = Predicate::empty();
        for t in self.terms.iter() {
            if !t.applyTo(&schema1) && !t.applyTo(&schema2) && t.applyTo(&new_schema) {
                result.terms.push(t.clone());
            }
        }

        if result.terms.len() == 0 {
            None
        } else {
            Some(result)
        }
    }

    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        for t in self.terms.iter() {
            if let Some(c) = t.equates_with_constant(field_name) {
                return Some(c);
            }
        }
        None
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        for t in self.terms.iter() {
            if let Some(f) = t.equates_with_field(field_name) {
                return Some(f);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_constant_partialeq() {
        use super::Constant::*;
        assert_eq!(Int(1), Int(1));
        assert_ne!(Int(1), Int(2));
        assert_eq!(String("abc".into()), String("abc".into()));
        assert_ne!(String("abd".into()), String("abc".into()));
    }

    #[test]
    fn test_constant_partialord() {
        use super::Constant::*;
        assert!(Int(1) < Int(2));
        assert!(Int(0) > Int(-1));
        assert!(String("abc".into()) < String("abd".into()));
        assert!(String("abd".into()) > String("abc".into()));
    }
}
