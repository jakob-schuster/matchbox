use std::fmt::Formatter;

use std::fmt::Display;

/// A field of a record.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RecField<T> {
    pub name: String,
    pub data: T,
}

impl<T> RecField<T> {
    pub fn new(name: String, data: T) -> RecField<T> {
        RecField { name, data }
    }
}

impl<T: Display> Display for RecField<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("{} = {}", self.name, self.data).fmt(f)
    }
}

/// A field of a core record.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CoreRecField<T> {
    pub name: Vec<u8>,
    pub data: T,
}

impl<'a, T> CoreRecField<T> {
    pub fn new(name: &'a [u8], data: T) -> CoreRecField<T> {
        CoreRecField {
            name: name.to_vec(),
            data,
        }
    }
}

impl<'a, T: Display> Display for CoreRecField<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!(
            "{} = {}",
            String::from_utf8(self.name.to_vec()).unwrap(),
            self.data
        )
        .fmt(f)
    }
}
