// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use super::stream_tokenizer::{StreamTokenizer, TT};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LexerError {
    #[error("Bad syntax")]
    BadSyntax,
}

type Result<T> = core::result::Result<T, LexerError>;

pub(crate) struct Lexer<'s> {
    keywords: Vec<&'static str>,
    tokenizer: StreamTokenizer<'s>,
}

impl<'s> Lexer<'s> {
    pub fn new(input: &'s str) -> Result<Self> {
        let keywords = vec![
            "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
            "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
        ];

        let mut tokenizer = StreamTokenizer::new(input);
        tokenizer.ordinary_char('.');
        tokenizer.word_chars('_', '_');
        tokenizer.lower_case_mode(true);

        let mut s = Self {
            keywords,
            tokenizer,
        };
        s.next_token()?;
        Ok(s)
    }

    pub fn match_delim(&self, d: char) -> bool {
        self.tokenizer
            .ttype()
            .and_then(|t| Some(*t == d))
            .unwrap_or(false)
    }

    pub fn match_int_constant(&self) -> bool {
        self.tokenizer
            .ttype()
            .and_then(|t| Some(*t == TT::Number))
            .unwrap_or(false)
    }

    pub fn match_string_constant(&self) -> bool {
        self.tokenizer
            .ttype()
            .and_then(|t| Some(*t == '\''))
            .unwrap_or(false)
    }

    pub fn match_keyword(&self, w: &str) -> bool {
        self.tokenizer
            .ttype()
            .and_then(|t| Some(*t == TT::Word))
            .unwrap_or(false)
            && self
                .tokenizer
                .sval()
                .and_then(|s| Some(s == w))
                .unwrap_or(false)
    }

    pub fn match_id(&self) -> bool {
        self.tokenizer
            .ttype()
            .and_then(|t| Some(*t == TT::Word))
            .unwrap_or(false)
            && !self
                .tokenizer
                .sval()
                .and_then(|s| Some(self.keywords.contains(&s.as_str())))
                .unwrap_or(false)
    }

    pub fn eat_delim(&mut self, d: char) -> Result<()> {
        if !self.match_delim(d) {
            return Err(LexerError::BadSyntax);
        }
        self.next_token()?;
        Ok(())
    }

    pub fn eat_int_constant(&mut self) -> Result<i32> {
        if !self.match_int_constant() {
            return Err(LexerError::BadSyntax);
        }
        let i = self.tokenizer.nval().ok_or(LexerError::BadSyntax)?;
        self.next_token()?;
        Ok(i.round() as i32) // ! FIXME
    }

    pub fn eat_string_constant(&mut self) -> Result<String> {
        if !self.match_string_constant() {
            return Err(LexerError::BadSyntax);
        }
        let s: String = self.tokenizer.sval().ok_or(LexerError::BadSyntax)?.into();
        self.next_token()?;
        Ok(s)
    }

    pub fn eat_keyword(&mut self, w: &str) -> Result<()> {
        if !self.match_keyword(w) {
            return Err(LexerError::BadSyntax);
        }
        self.next_token()?;
        Ok(())
    }

    pub fn eat_id(&mut self) -> Result<String> {
        if !self.match_id() {
            return Err(LexerError::BadSyntax);
        }
        let s: String = self.tokenizer.sval().ok_or(LexerError::BadSyntax)?.into();
        self.next_token()?;
        Ok(s)
    }

    fn next_token(&mut self) -> Result<()> {
        let _ = self.tokenizer.next_token().ok_or(LexerError::BadSyntax)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Lexer;

    #[test]
    fn test() {
        let input = "select name from users where id = 1";
        let mut l = Lexer::new(input).unwrap();

        assert!(l.match_keyword("select"));
        l.eat_keyword("select").unwrap();

        assert!(l.match_id());
        assert_eq!(l.eat_id().unwrap(), "name");

        assert!(l.match_keyword("from"));
        l.eat_keyword("from").unwrap();

        assert!(l.match_id());
        assert_eq!(l.eat_id().unwrap(), "users");

        assert!(l.match_keyword("where"));
        l.eat_keyword("where").unwrap();

        assert!(l.match_id());
        assert_eq!(l.eat_id().unwrap(), "id");

        assert!(l.match_delim('='));
        l.eat_delim('=').unwrap();

        assert!(l.match_int_constant());
        assert_eq!(l.eat_int_constant().unwrap(), 1);
    }
}
