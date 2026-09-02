use core::fmt;
use std::fmt::Formatter;

use std::fmt::Display;

/// A range, from a start element to an end.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Ran<T> {
    pub start: T,
    pub end: T,
}

impl<T: fmt::Display> fmt::Display for Ran<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({}:{})", self.start, self.end)
    }
}

impl<T> Ran<T> {
    pub fn new(start: T, end: T) -> Self {
        Ran { start, end }
    }

    pub fn map<B>(&self, f: impl Fn(&T) -> B) -> Ran<B> {
        Ran::new(f(&self.start), f(&self.end))
    }

    pub fn to_tuple(&self) -> (&T, &T) {
        (&self.start, &self.end)
    }
}

impl<T: Ord> Ran<T> {
    /// Checks if two Rans are disjoint.
    pub fn disjoint(&self, other: &Self) -> bool {
        other.start >= self.end || self.start >= other.end
    }
}
