// Copyright (c) 2023 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use bitflags::bitflags;
use std::str::Chars;

bitflags! {
    #[derive(Default)]
    struct CT: u8 {
        const WHITESPACE = 0b0000_0001;
        const DIGIT = 0b0000_0010;
        const ALPHA = 0b0000_0100;
        const QUOTE = 0b0000_1000;
        const COMMENT = 0b0001_0000;
    }
}

impl CT {
    fn clear(&mut self) {
        self.bits = 0;
    }

    fn has(&self, f: CT) -> bool {
        !(*self & f).is_empty()
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum State {
    NeedChar,
    SkipLF,
    Char(char),
}

impl State {
    fn char(&self) -> Option<char> {
        match self {
            State::Char(c) => Some(*c),
            _ => None,
        }
    }

    fn sub(&self, other: char) -> Option<u32> {
        match self {
            State::Char(c) => {
                if *c >= '0' {
                    Some(*c as u32 - other as u32)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl PartialEq<u32> for State {
    fn eq(&self, other: &u32) -> bool {
        match self {
            State::Char(c) => {
                if let Some(o) = char::from_u32(*other) {
                    *c == o
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl PartialEq<char> for State {
    fn eq(&self, other: &char) -> bool {
        match self {
            State::Char(c) => c == other,
            _ => false,
        }
    }
}

impl PartialEq<State> for char {
    fn eq(&self, other: &State) -> bool {
        match other {
            State::Char(c) => self == c,
            _ => false,
        }
    }
}

impl PartialOrd<u32> for State {
    fn partial_cmp(&self, other: &u32) -> Option<std::cmp::Ordering> {
        match self {
            State::Char(c) => {
                if let Some(o) = char::from_u32(*other) {
                    Some(c.cmp(&o))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl PartialOrd<char> for State {
    fn partial_cmp(&self, other: &char) -> Option<std::cmp::Ordering> {
        match self {
            State::Char(c) => Some(c.cmp(other)),
            _ => None,
        }
    }
}

impl PartialOrd<State> for char {
    fn partial_cmp(&self, other: &State) -> Option<std::cmp::Ordering> {
        match other {
            State::Char(c) => Some(self.cmp(c)),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TT {
    EOF,
    EOL,
    Number,
    Word,
    Any(char),
}

impl From<State> for TT {
    fn from(s: State) -> Self {
        match s {
            State::NeedChar => TT::EOF,
            State::SkipLF => TT::EOL,
            State::Char(ch) => TT::Any(ch),
        }
    }
}

impl PartialEq<TT> for State {
    fn eq(&self, other: &TT) -> bool {
        match self {
            State::Char(c) => match *other {
                TT::Any(a) => *c == a,
                _ => false,
            },
            _ => false,
        }
    }
}

impl PartialEq<Option<TT>> for State {
    fn eq(&self, other: &Option<TT>) -> bool {
        match other {
            Some(tt) => self == tt,
            None => false,
        }
    }
}

pub(crate) struct StreamTokenizer<'s> {
    input: Chars<'s>,
    nval: Option<f64>,
    sval: Option<String>,
    ttype: Option<TT>,

    ctype: [CT; 256],
    force_lower: bool,
    pushed_back: bool,
    peekc: State,
    lineno: u32,
    eol_is_significant: bool,
    slash_slash_comments: bool,
    slash_star_comments: bool,
    buf: Vec<char>,
}

impl<'s> StreamTokenizer<'s> {
    pub fn new(input: &'s str) -> Self {
        let mut s = Self {
            input: input.chars(),
            nval: None,
            sval: None,
            ttype: None,
            ctype: [Default::default(); 256],
            force_lower: false,
            pushed_back: false,
            peekc: State::NeedChar,
            lineno: 1,
            eol_is_significant: false,
            slash_slash_comments: false,
            slash_star_comments: false,
            buf: vec![char::default(); 20],
        };
        s.word_chars('a', 'z');
        s.word_chars('A', 'Z');
        s.word_chars(
            char::from_u32(128 + 32).unwrap(),
            char::from_u32(255).unwrap(),
        );
        s.whitespace_chars(char::from_u32(0).unwrap(), ' ');
        s.comment_char('/');
        s.quote_char('"');
        s.quote_char('\'');
        s.parse_numbers();
        s
    }

    fn word_chars(&mut self, low: char, hi: char) {
        let mut i = low as usize;
        while i <= hi as usize {
            self.ctype[i] |= CT::ALPHA;
            i += 1;
        }
    }

    fn whitespace_chars(&mut self, low: char, hi: char) {
        let mut i = low as usize;
        while i <= hi as usize {
            self.ctype[i] = CT::WHITESPACE;
            i += 1;
        }
    }

    fn comment_char(&mut self, ch: char) {
        let i = ch as usize;
        if 0 <= i && i < self.ctype.len() {
            self.ctype[i] = CT::COMMENT;
        }
    }

    fn quote_char(&mut self, ch: char) {
        let i = ch as usize;
        if i >= 0 && i < self.ctype.len() {
            self.ctype[i] = CT::QUOTE;
        }
    }

    fn parse_numbers(&mut self) {
        for i in '0'..='9' {
            self.ctype[i as usize] = CT::DIGIT;
        }
        self.ctype['.' as usize] = CT::DIGIT;
        self.ctype['-' as usize] = CT::DIGIT;
    }

    pub fn nval(&self) -> Option<f64> {
        self.nval
    }

    pub fn sval(&self) -> Option<&String> {
        self.sval.as_ref()
    }

    pub fn ttype(&self) -> Option<&TT> {
        self.ttype.as_ref()
    }

    pub fn ordinary_char(&mut self, ch: char) {
        let i = ch as usize;
        if i < self.ctype.len() {
            self.ctype[i].clear();
        }
    }

    pub fn lower_case_mode(&mut self, b: bool) {
        self.force_lower = b;
    }

    pub fn set_eol_is_significant(&mut self, flag: bool) {
        self.eol_is_significant = flag;
    }

    fn read(&mut self) -> State {
        use State::*;
        self.input
            .next()
            .and_then(|ch| Some(Char(ch)))
            .unwrap_or(NeedChar)
    }

    fn get_ctype(&self, c: &State) -> CT {
        let ct = self.ctype;
        if let State::Char(ch) = c {
            if *ch <= 255 as char {
                return ct[*ch as usize];
            }
        }
        CT::ALPHA
    }

    fn set_and_get_ttype(&mut self, tt: TT) -> Option<&TT> {
        self.ttype = Some(tt);
        self.ttype.as_ref()
    }

    fn extend_buf(&mut self) {
        let prev = self.buf.clone();
        self.buf = vec![char::default(); prev.len() * 2];
        self.buf[..prev.len()].clone_from_slice(&prev);
    }

    pub fn next_token(&mut self) -> Option<&TT> {
        if self.pushed_back {
            self.pushed_back = false;
            return self.ttype.as_ref();
        }

        // let ct = self.ctype;
        self.sval = None;

        let mut c = self.peekc;
        if c == State::SkipLF {
            c = self.read();
            if c == State::NeedChar {
                return self.set_and_get_ttype(TT::EOF);
            }
            if c == '\n' {
                c = State::NeedChar;
            }
        }
        if c == State::NeedChar {
            c = self.read();
            if c == State::NeedChar {
                return self.set_and_get_ttype(TT::EOF);
            }
        }
        // TODO: self.ttype = c;

        self.peekc = State::NeedChar;

        let mut ctype = self.get_ctype(&c);
        while ctype.has(CT::WHITESPACE) {
            if c == '\r' {
                self.lineno += 1;
                if self.eol_is_significant {
                    self.peekc = State::SkipLF;
                    return self.set_and_get_ttype(TT::EOL);
                }
                c = self.read();
                if c == '\n' {
                    c = self.read();
                }
            } else {
                if c == '\n' {
                    self.lineno += 1;
                    if self.eol_is_significant {
                        return self.set_and_get_ttype(TT::EOL);
                    }
                }
                c = self.read();
            }
            if c == State::NeedChar {
                return self.set_and_get_ttype(TT::EOF);
            }
            ctype = self.get_ctype(&c);
        }

        if ctype.has(CT::DIGIT) {
            let mut neg = false;
            if c == '-' {
                c = self.read();
                if c != '.' && (c < '0' || c > '9') {
                    self.peekc = c;
                    return self.set_and_get_ttype(TT::Any('-'));
                }
                neg = true;
            }
            let mut v = 0.0f64;
            let mut decexp = 0u32;
            let mut seendot = 0u32;
            loop {
                if c == '.' && seendot == 0 {
                    seendot = 1;
                } else if '0' <= c && c <= '9' {
                    let n = c.sub('0').unwrap();
                    v = v * 10.0 + n as f64;
                    decexp += seendot;
                } else {
                    break;
                }
                c = self.read();
            }
            self.peekc = c;
            if decexp != 0 {
                let mut denom = 10f64;
                decexp -= 1;
                while decexp > 0 {
                    denom *= 10f64;
                    decexp -= 1;
                }
                v /= denom;
            }
            self.nval = Some(if neg { -v } else { v });
            return self.set_and_get_ttype(TT::Number);
        }

        if ctype.has(CT::ALPHA) {
            let mut i = 0;
            loop {
                if i >= self.buf.len() {
                    self.extend_buf();
                }
                self.buf[i] = c.char().unwrap();
                i += 1;
                c = self.read();
                ctype = c
                    .char()
                    .and_then(|ch| Some(ch as usize))
                    .and_then(|i| Some(if i <= 255 { self.ctype[i] } else { CT::ALPHA }))
                    .unwrap_or(CT::WHITESPACE);
                if !ctype.has(CT::ALPHA | CT::DIGIT) {
                    break;
                }
            }
            self.peekc = c;
            self.sval = Some(self.buf[0..i].iter().collect());
            if self.force_lower {
                self.sval = self
                    .sval
                    .as_ref()
                    .and_then(|s| Some(s.as_str().to_lowercase()));
            }
            return self.set_and_get_ttype(TT::Word);
        }

        if ctype.has(CT::QUOTE) {
            self.ttype = Some(c.into());
            let mut i = 0;
            let mut d = self.read();
            while d >= 0 && d != self.ttype && d != '\n' && d != '\r' {
                if d == '\\' {
                    c = self.read();
                    let first = c;
                    if c >= '0' && c <= '7' {
                        let mut n = c.sub('0').unwrap();
                        let mut c2 = self.read();
                        if '0' <= c2 && c2 <= '7' {
                            n = (n << 3) + c2.sub('0').unwrap();
                            c2 = self.read();
                            if '0' <= c2 && c2 <= '7' && first <= '3' {
                                n = (n << 3) + c2.sub('0').unwrap();
                                d = self.read();
                            } else {
                                d = c2;
                            }
                        }
                        c = State::Char(char::from_u32(n).unwrap());
                    } else {
                        c = match c {
                            State::Char('a') => State::Char(0x7 as char),
                            State::Char('b') => State::Char(0x8 as char),
                            State::Char('f') => State::Char(0xC as char),
                            State::Char('n') => State::Char('\n'),
                            State::Char('r') => State::Char('\r'),
                            State::Char('t') => State::Char('\t'),
                            State::Char('v') => State::Char(0xB as char),
                            _ => c,
                        };
                        d = self.read();
                    }
                } else {
                    c = d;
                    d = self.read();
                }
                if i >= self.buf.len() {
                    self.extend_buf();
                }
                self.buf[i] = if let State::Char(_c) = c {
                    _c
                } else {
                    panic!("TODO")
                };
                i += 1;
            }

            self.peekc = if d == self.ttype { State::NeedChar } else { d };
            self.sval = Some(self.buf[0..i].iter().collect());
            return self.ttype.as_ref();
        }

        if c == '/' && (self.slash_slash_comments || self.slash_star_comments) {
            c = self.read();
            if c == '*' && self.slash_star_comments {
                let mut prevc = State::NeedChar;
                c = self.read();
                while c != '/' || prevc != '*' {
                    if c == '\r' {
                        self.lineno += 1;
                        c = self.read();
                        if c == '\n' {
                            c = self.read();
                        }
                    } else {
                        if c == '\n' {
                            self.lineno += 1;
                            c = self.read();
                        }
                    }
                    if c == State::NeedChar {
                        self.ttype = Some(TT::EOF);
                        return self.ttype.as_ref();
                    }
                    prevc = c;

                    c = self.read();
                }
                return self.next_token();
            } else if c == '/' && self.slash_slash_comments {
                c = self.read();
                while c != '\n' && c != '\r' && c != State::NeedChar {
                    c = self.read();
                }
                self.peekc = c;
                return self.next_token();
            } else {
                if self.ctype['/' as usize].has(CT::COMMENT) {
                    while c != '\n' && c != '\r' && c != State::NeedChar {
                        c = self.read();
                    }
                    self.peekc = c;
                    return self.next_token();
                } else {
                    self.peekc = c;
                    self.ttype = Some(TT::Any('/'));
                    return self.ttype.as_ref();
                }
            }
        }

        if ctype.has(CT::COMMENT) {
            while c != '\n' && c != '\r' && c != State::NeedChar {
                c = self.read();
            }
            self.peekc = c;
            return self.next_token();
        }

        self.ttype = Some(TT::from(c));
        self.ttype.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::{StreamTokenizer, TT};

    #[test]
    fn test() {
        let s = "select name from test.users where id = 1".into();
        let mut t = StreamTokenizer::new(s);
        t.ordinary_char('.');
        t.lower_case_mode(true);

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "select");

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "name");

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "from");

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "test");

        assert_eq!(*t.next_token().unwrap(), TT::Any('.'));
        assert_eq!(t.sval(), None);

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "users");

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "where");

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "id");

        assert_eq!(*t.next_token().unwrap(), TT::Any('='));
        assert_eq!(t.sval(), None);

        assert_eq!(*t.next_token().unwrap(), TT::Number);
        assert_eq!(t.nval().unwrap(), 1.0f64);

        assert_eq!(*t.next_token().unwrap(), TT::EOF);
    }

    #[test]
    fn test_number() {
        let s = "1".into();
        let mut t = StreamTokenizer::new(s);
        t.ordinary_char('.');
        t.lower_case_mode(true);

        assert_eq!(*t.next_token().unwrap(), TT::Number);
        assert_eq!(t.nval().unwrap(), 1.0);

        assert_eq!(*t.next_token().unwrap(), TT::EOF);
    }

    #[test]
    fn test_word() {
        let s = "a".into();
        let mut t = StreamTokenizer::new(s);
        t.ordinary_char('.');
        t.lower_case_mode(true);

        assert_eq!(*t.next_token().unwrap(), TT::Word);
        assert_eq!(t.sval().unwrap(), "a");

        assert_eq!(*t.next_token().unwrap(), TT::EOF);
    }

    #[test]
    fn test_empty() {
        let s = "".into();
        let mut t = StreamTokenizer::new(s);
        t.ordinary_char('.');
        t.lower_case_mode(true);

        assert_eq!(*t.next_token().unwrap(), TT::EOF);
    }

    // #[test]
    // fn test_escape() {
    //     let s = r"\b\f\n\r\t".into();
    //     let mut t = StreamTokenizer::new(s);
    //     t.ordinary_char('.');
    //     t.lower_case_mode(true);

    //     assert_eq!(*t.next_token().unwrap(), TT::EOF);
    // }
}
