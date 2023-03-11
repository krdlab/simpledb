// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use crate::{
    query::predicate::{Constant, Expression, Predicate, Term},
    record::schema::Schema,
};

use super::{
    data::{QueryData, UpdateCmd},
    lexer::{Lexer, Result},
};

struct PredParser<'s> {
    lex: Lexer<'s>,
}

impl<'s> PredParser<'s> {
    pub fn new(input: &'s str) -> Result<Self> {
        Ok(Self {
            lex: Lexer::new(input)?,
        })
    }

    pub fn field(&mut self) -> Result<String> {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Result<()> {
        if self.lex.match_string_constant() {
            self.lex.eat_string_constant()?;
        } else {
            self.lex.eat_int_constant()?;
        }
        Ok(())
    }

    pub fn term(&mut self) -> Result<()> {
        if self.lex.match_id() {
            self.field()?;
        } else {
            self.constant()?;
        }
        Ok(())
    }

    pub fn expression(&mut self) -> Result<()> {
        self.term()?;
        self.lex.eat_delim('=')?;
        self.term()?;
        Ok(())
    }

    pub fn predicate(&mut self) -> Result<()> {
        self.expression()?;
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            self.predicate()?;
        }
        Ok(())
    }
}

pub struct Parser<'s> {
    lex: Lexer<'s>,
}

impl<'s> Parser<'s> {
    pub fn new(input: &'s str) -> Result<Self> {
        Ok(Self {
            lex: Lexer::new(input)?,
        })
    }

    pub fn field(&mut self) -> Result<String> {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Result<Constant> {
        if self.lex.match_string_constant() {
            Ok(Constant::String(self.lex.eat_string_constant()?))
        } else {
            Ok(Constant::Int(self.lex.eat_int_constant()?))
        }
    }

    pub fn term(&mut self) -> Result<Term> {
        if self.lex.match_id() {
            Ok(Term::FieldName(self.field()?))
        } else {
            Ok(Term::Constant(self.constant()?))
        }
    }

    pub fn expression(&mut self) -> Result<Expression> {
        let lhs = self.term()?;
        self.lex.eat_delim('=')?;
        let rhs = self.term()?;
        Ok(Expression::new(lhs, rhs))
    }

    pub fn predicate(&mut self) -> Result<Predicate> {
        let mut pred = Predicate::new(self.expression()?);
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            pred.conjoin_with(self.predicate()?);
        }
        Ok(pred)
    }

    pub fn query(&mut self) -> Result<QueryData> {
        self.lex.eat_keyword("select")?;
        let fields = self.select_list()?;
        self.lex.eat_keyword("from")?;
        let tables = self.table_list()?;
        let mut pred = Predicate::empty();

        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(QueryData::new(fields, tables, pred))
    }

    pub fn select_list(&mut self) -> Result<Vec<String>> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.field()?);
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.select_list()?);
        }
        Ok(l)
    }

    pub fn table_list(&mut self) -> Result<Vec<String>> {
        let mut l: Vec<String> = Vec::new();
        l.push(self.lex.eat_id()?);
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.table_list()?);
        }
        Ok(l)
    }

    pub fn update_cmd(&mut self) -> Result<UpdateCmd> {
        if self.lex.match_keyword("insert") {
            self.insert()
        } else if self.lex.match_keyword("delete") {
            self.delete()
        } else if self.lex.match_keyword("update") {
            self.modify()
        } else {
            self.create()
        }
    }

    fn create(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("create")?;
        if self.lex.match_keyword("table") {
            self.create_table()
        } else if self.lex.match_keyword("view") {
            self.create_view()
        } else {
            self.create_index()
        }
    }

    pub fn delete(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("delete")?;
        self.lex.eat_keyword("from")?;
        let table_name = self.lex.eat_id()?;
        let mut pred = Predicate::empty();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(UpdateCmd::DeleteData { table_name, pred })
    }

    pub fn insert(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("insert")?;
        self.lex.eat_keyword("into")?;
        let table_name = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let fields = self.field_list()?;
        self.lex.eat_delim(')')?;
        self.lex.eat_keyword("values")?;
        self.lex.eat_delim('(')?;
        let values = self.constant_list()?;
        self.lex.eat_delim(')')?;
        Ok(UpdateCmd::InsertData {
            table_name,
            fields,
            values,
        })
    }

    fn field_list(&mut self) -> Result<Vec<String>> {
        let mut l = Vec::new();
        l.push(self.field()?);
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.field_list()?);
        }
        Ok(l)
    }

    fn constant_list(&mut self) -> Result<Vec<Constant>> {
        let mut l = Vec::new();
        l.push(self.constant()?);
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            l.extend(self.constant_list()?);
        }
        Ok(l)
    }

    pub fn modify(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("update")?;
        let table_name = self.lex.eat_id()?;
        self.lex.eat_keyword("set")?;
        let field = self.field()?;
        self.lex.eat_delim('=')?;
        let value = self.term()?;
        let mut pred = Predicate::empty();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }
        Ok(UpdateCmd::ModifyData {
            table_name,
            field,
            value,
            pred,
        })
    }

    pub fn create_table(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("table")?;
        let table_name = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let schema = self.field_defs()?;
        self.lex.eat_delim(')')?;
        Ok(UpdateCmd::CreateTableData { table_name, schema })
    }

    fn field_defs(&mut self) -> Result<Schema> {
        let mut scheme: Schema = self.field_def()?;
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            let rest: Schema = self.field_defs()?;
            scheme.add_all(&rest);
        }
        Ok(scheme)
    }

    fn field_def(&mut self) -> Result<Schema> {
        let field = self.field()?;
        self.field_type(field)
    }

    fn field_type(&mut self, name: String) -> Result<Schema> {
        let mut schema = Schema::new();
        if self.lex.match_keyword("int") {
            self.lex.eat_keyword("int")?;
            schema.add_i32_field(&name);
        } else {
            self.lex.eat_keyword("varchar")?;
            self.lex.eat_delim('(')?;
            let len = self.lex.eat_int_constant()?;
            self.lex.eat_delim(')')?;
            schema.add_string_field(&name, len.try_into().unwrap());
        }
        Ok(schema)
    }

    pub fn create_view(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("view")?;
        let view_name = self.lex.eat_id()?;
        self.lex.eat_keyword("as")?;
        let query = self.query()?;
        Ok(UpdateCmd::CreateViewData { view_name, query })
    }

    pub fn create_index(&mut self) -> Result<UpdateCmd> {
        self.lex.eat_keyword("index")?;
        let index_name = self.lex.eat_id()?;
        self.lex.eat_keyword("on")?;
        let table_name = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let field = self.lex.eat_id()?;
        self.lex.eat_delim(')')?;
        Ok(UpdateCmd::CreateIndexData {
            index_name,
            table_name,
            field,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Parser, PredParser};
    use crate::{
        parse::{data::UpdateCmd, lexer::LexerError},
        query::predicate::{Constant, Expression, Predicate, Term},
        record::schema::SqlType,
    };

    #[test]
    fn test_predparser() {
        {
            let mut p = PredParser::new("a = 1").unwrap();
            assert!(p.predicate().is_ok());
        }
        {
            let mut p = PredParser::new(" = 1").unwrap();
            assert_eq!(p.predicate().err().unwrap(), LexerError::BadSyntax);
        }
    }

    #[test]
    fn test_parser_when_select() {
        let mut p = Parser::new("select name from users where id = 1").unwrap();
        let query = p.query().unwrap();
        assert_eq!(*query.fields(), vec!["name".to_string()]);
        assert_eq!(*query.tables(), vec!["users".to_string()]);
        assert_eq!(
            *query.pred(),
            Predicate::new(Expression::new(
                Term::FieldName("id".into()),
                Term::Constant(Constant::Int(1))
            ))
        );
    }

    #[test]
    fn test_parser_when_insert() {
        let mut p = Parser::new("insert into users (id, name) values (1, 'krdlab')").unwrap();
        if let UpdateCmd::InsertData {
            table_name,
            fields,
            values,
        } = p.update_cmd().unwrap()
        {
            assert_eq!(table_name, "users");
            assert_eq!(fields, vec!["id", "name"]);
            assert_eq!(
                values,
                vec![Constant::Int(1), Constant::String("krdlab".into())]
            );
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parser_when_delete() {
        let mut p = Parser::new("delete from users where id = 1").unwrap();
        if let UpdateCmd::DeleteData { table_name, pred } = p.update_cmd().unwrap() {
            assert_eq!(table_name, "users");
            assert_eq!(
                pred,
                Predicate::new(Expression::new(
                    Term::FieldName("id".into()),
                    Term::Constant(Constant::Int(1))
                ))
            );
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parser_when_update() {
        let mut p = Parser::new("update users set name = 'krdlab' where id = 1").unwrap();
        if let UpdateCmd::ModifyData {
            table_name,
            field,
            value,
            pred,
        } = p.update_cmd().unwrap()
        {
            assert_eq!(table_name, "users");
            assert_eq!(field, "name");
            assert_eq!(
                value,
                Term::Constant(Constant::String("krdlab".to_string()))
            );
            assert_eq!(
                pred,
                Predicate::new(Expression::new(
                    Term::FieldName("id".to_string()),
                    Term::Constant(Constant::Int(1))
                ))
            );
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parser_when_create_table() {
        let mut p = Parser::new("create table users (id int, name varchar(32))").unwrap();
        if let UpdateCmd::CreateTableData { table_name, schema } = p.update_cmd().unwrap() {
            assert_eq!(table_name, "users");

            let mut f_iter = schema.fields_iter();
            assert_eq!(f_iter.next().unwrap(), "id");
            assert_eq!(schema.field_type("id").unwrap(), SqlType::Integer);

            assert_eq!(f_iter.next().unwrap(), "name");
            assert_eq!(schema.field_type("name").unwrap(), SqlType::VarChar);
            assert_eq!(schema.field_length("name").unwrap(), 32);

            assert!(f_iter.next().is_none());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parser_when_create_view() {
        let mut p = Parser::new("create view test as select name from users").unwrap();
        if let UpdateCmd::CreateViewData { view_name, query } = p.update_cmd().unwrap() {
            assert_eq!(view_name, "test");
            assert_eq!(*query.fields(), vec!["name".to_string()]);
            assert_eq!(*query.tables(), vec!["users".to_string()]);
            assert_eq!(*query.pred(), Predicate::empty());
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parser_when_create_index() {
        let mut p = Parser::new("create index name_idx on users (name)").unwrap();
        if let UpdateCmd::CreateIndexData {
            index_name,
            table_name,
            field,
        } = p.update_cmd().unwrap()
        {
            assert_eq!(index_name, "name_idx");
            assert_eq!(table_name, "users");
            assert_eq!(field, "name");
        } else {
            assert!(false);
        }
    }
}
