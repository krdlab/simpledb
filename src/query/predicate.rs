// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::fmt::Display;

use crate::{plan::plan::Plan, record::schema::Schema};

use super::scan::UpdateScan;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Constant {
    Int(i32),
    String(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Constant(Constant),
    FieldName(String),
}

impl Term {
    pub fn evaluate<'s>(&self, s: &Box<dyn UpdateScan + 's>) -> Constant {
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

    pub fn apply_to(&self, schema: &Schema) -> bool {
        match self {
            Self::Constant(_) => true,
            Self::FieldName(fname) => schema.has_field(fname),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    lhs: Term,
    rhs: Term,
}

impl Expression {
    pub fn new(lhs: Term, rhs: Term) -> Self {
        Self { lhs, rhs }
    }

    pub fn is_satisfied<'s>(&self, s: &Box<dyn UpdateScan + 's>) -> bool {
        let lval = self.lhs.evaluate(s);
        let rval = self.rhs.evaluate(s);
        lval == rval
    }

    pub fn reduction_factor<'p>(&self, p: &Box<dyn Plan + 'p>) -> usize {
        todo!()
    }

    // F = c
    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        if let Term::FieldName(fname) = &self.lhs {
            if fname == field_name {
                if let Term::Constant(v) = &self.rhs {
                    return Some(v.clone());
                }
            }
        }
        if let Term::FieldName(fname) = &self.rhs {
            if fname == field_name {
                if let Term::Constant(v) = &self.lhs {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        if let Term::FieldName(fname) = &self.lhs {
            if fname == field_name {
                if let Term::FieldName(v) = &self.rhs {
                    return Some(v.clone());
                }
            }
        }
        if let Term::FieldName(fname) = &self.rhs {
            if fname == field_name {
                if let Term::FieldName(v) = &self.lhs {
                    return Some(v.clone());
                }
            }
        }
        None
    }

    pub fn apply_to(&self, schema: &Schema) -> bool {
        self.lhs.apply_to(schema) && self.rhs.apply_to(schema)
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} = {:?}", self.lhs, self.rhs)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Predicate {
    exprs: Vec<Expression>,
}

impl Predicate {
    pub fn empty() -> Self {
        Self { exprs: Vec::new() }
    }

    pub fn new(t: Expression) -> Self {
        Self { exprs: vec![t] }
    }

    pub fn is_empty(&self) -> bool {
        self.exprs.is_empty()
    }

    pub fn conjoin_with(&mut self, mut pred: Predicate) {
        self.exprs.append(&mut pred.exprs);
    }

    pub fn is_satisfied<'s>(&self, scan: &Box<dyn UpdateScan + 's>) -> bool {
        for t in self.exprs.iter() {
            if !t.is_satisfied(scan) {
                return false;
            }
        }
        true
    }

    pub fn reduction_factor<'p>(&self, p: &Box<dyn Plan + 'p>) -> usize {
        let mut factor = 1;
        for e in self.exprs.iter() {
            factor *= e.reduction_factor(p);
        }
        factor
    }

    pub fn select_sub_pred(&self, schema: &Schema) -> Option<Predicate> {
        let mut result = Predicate::empty();
        for t in self.exprs.iter() {
            if t.apply_to(schema) {
                result.exprs.push(t.clone());
            }
        }
        if result.exprs.len() == 0 {
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
        for t in self.exprs.iter() {
            if !t.apply_to(&schema1) && !t.apply_to(&schema2) && t.apply_to(&new_schema) {
                result.exprs.push(t.clone());
            }
        }

        if result.exprs.len() == 0 {
            None
        } else {
            Some(result)
        }
    }

    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        for t in self.exprs.iter() {
            if let Some(c) = t.equates_with_constant(field_name) {
                return Some(c);
            }
        }
        None
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        for t in self.exprs.iter() {
            if let Some(f) = t.equates_with_field(field_name) {
                return Some(f);
            }
        }
        None
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        write!(f, "{}", s.join(" and "))
    }
}

#[cfg(test)]
mod tests {
    use super::Term;
    use crate::{query::predicate::Expression, record::schema::Schema};

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

    #[test]
    fn test_term() {
        use super::Constant::*;

        let mut schema = Schema::new();
        schema.add_i32_field("A");

        {
            let t = Term::Constant(Int(1));
            assert!(!t.is_field_name());
            assert!(t.apply_to(&schema));
        }
        {
            let t = Term::FieldName("A".into());
            assert!(t.is_field_name());
            assert!(t.apply_to(&schema));
        }
        {
            let t = Term::FieldName("B".into());
            assert!(!t.apply_to(&schema));
        }
    }

    #[test]
    fn test_expression() {
        use super::Constant::*;
        {
            let t1 = Term::FieldName("A".into());
            let t2 = Term::Constant(Int(1));
            let expr = Expression::new(t1, t2);

            assert_eq!(expr.equates_with_constant("A"), Some(Int(1)));
            assert_eq!(expr.equates_with_field("A"), None);
        }
        {
            let t1 = Term::FieldName("A".into());
            let t2 = Term::FieldName("B".into());
            let expr = Expression::new(t1, t2);

            assert_eq!(expr.equates_with_constant("A"), None);
            assert_eq!(expr.equates_with_field("A"), Some("B".into()));
        }
    }

    #[test]
    fn test_predicate() {
        // NOTE: see: operators::tests
    }
}
