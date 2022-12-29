// Copyright (c) 2022 Sho Kuroda <krdlab@gmail.com>
//
// This software is released under the MIT License.
// https://opensource.org/licenses/MIT

use std::{collections::HashMap, convert::Into};

use crate::{constants::I32_BYTE_SIZE, file::page::Page};

// NOTE: java.sql.Types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Integer = 4,
    VarChar = 12,
}

impl Into<i32> for SqlType {
    fn into(self) -> i32 {
        self as i32
    }
}

struct FieldInfo {
    ftype: SqlType,
    flength: usize,
}

pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn fields_iter(&self) -> std::slice::Iter<String> {
        self.fields.iter()
    }

    pub fn has_field(&self, fname: &str) -> bool {
        self.fields.contains(&fname.into())
    }

    pub fn get_type(&self, fname: &str) -> Option<SqlType> {
        self.info.get(fname).and_then(|fi| Some(fi.ftype))
    }

    fn get_length(&self, fname: &str) -> Option<usize> {
        self.info
            .get(fname)
            .and_then(|fi| Some(fi.flength.try_into().unwrap()))
    }

    pub fn add_field(&mut self, fname: &str, ftype: SqlType, flength: usize) {
        self.fields.push(fname.into());
        self.info.insert(fname.into(), FieldInfo { ftype, flength });
    }

    pub fn add_i32_field(&mut self, fname: &str) {
        self.add_field(fname, SqlType::Integer, 0);
    }

    pub fn add_string_field(&mut self, fname: &str, flength: usize) {
        self.add_field(fname, SqlType::VarChar, flength);
    }

    pub fn add_field_from(&mut self, fname: &str, schema: &Schema) {
        let ft = schema.get_type(fname).unwrap(); // TODO
        let fl = schema.get_length(fname).unwrap(); // TODO
        self.add_field(fname, ft, fl);
    }

    pub fn add_all(&mut self, schema: &Schema) {
        for field in schema.fields.iter() {
            self.add_field_from(field, schema);
        }
    }
}

pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>,
    slotsize: usize,
}

impl Layout {
    pub fn new(schema: Schema) -> Self {
        let mut offsets: HashMap<String, usize> = HashMap::new();
        let mut pos: usize = I32_BYTE_SIZE as usize;
        for fname in schema.fields_iter() {
            offsets.insert(fname.into(), pos);
            pos += Self::length_in_bytes(&schema, fname).unwrap(); // TODO
        }
        let slotsize = pos;

        Self {
            schema,
            offsets,
            slotsize,
        }
    }

    pub fn from_metadata(schema: Schema, offsets: HashMap<String, usize>, slotsize: usize) -> Self {
        Self {
            schema,
            offsets,
            slotsize,
        }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_offset(&self, fname: &str) -> Option<&usize> {
        self.offsets.get(fname)
    }

    pub fn get_slotsize(&self) -> usize {
        self.slotsize
    }

    fn length_in_bytes(schema: &Schema, fname: &str) -> Option<usize> {
        if let Some(ftype) = schema.get_type(fname) {
            if ftype == SqlType::Integer {
                Some(I32_BYTE_SIZE as usize)
            } else {
                Some(Page::max_length(schema.get_length(fname).unwrap()))
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Layout, Schema};

    #[test]
    fn test() {
        let mut schema = Schema::new();
        schema.add_i32_field("A");
        schema.add_string_field("B", 9);

        let layout = Layout::new(schema);
        assert_eq!(layout.get_offset("A"), Some(&4)); // NOTE: 0 to 3 is a flag area
        assert_eq!(layout.get_offset("B"), Some(&8));
        assert_eq!(layout.get_slotsize(), 48); // NOTE: 4 + 4 + 4 (area of string bytes length) + (9 (field length) * 4 (bytes/char))
    }
}
