use core::fmt;
use std::fmt::Formatter;

use std::fmt::Display;

/// A cache of elements.
#[derive(Clone, Debug)]
pub struct Cache<A> {
    pub(crate) vec: Vec<A>,
}

impl<A: Clone + Display> Cache<A> {
    /// Push an element to the top of the cache.
    /// Returns a new cache, and the index at which the value is found.
    pub fn push(&self, a: A) -> (Cache<A>, usize) {
        let index = self.vec.len();
        let mut vec = self.vec.clone();
        vec.push(a);

        (Cache { vec }, index)
    }

    /// Get an item at an index in the cache.
    pub fn get<'a>(&self, index: usize) -> &A {
        self.vec.get(index).expect("Bad index in cache!")
    }
}

impl<A: Display> Display for Cache<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        format!(
            "[{}]",
            self.vec
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
        .fmt(f)
    }
}

impl<A> Default for Cache<A> {
    fn default() -> Self {
        Cache {
            vec: Vec::default(),
        }
    }
}
