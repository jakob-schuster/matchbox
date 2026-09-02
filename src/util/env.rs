use std::fmt::Formatter;

use std::fmt::Display;

/// An environment, which contains a number of elements.
#[derive(Clone, Debug)]
pub struct Env<A> {
    pub(crate) vec: Vec<A>,
}

impl<A> Env<A> {
    pub fn iter(&self) -> std::slice::Iter<'_, A> {
        self.vec.iter()
    }

    /// Get element at a De Bruijn index.
    pub fn get_level(&self, i: usize) -> &A {
        match self.vec.get(i) {
            Some(a) => a,
            None => panic!("Bad index in env!"),
        }
    }

    /// Get element at a De Bruijn levl.
    pub fn get_index(&self, i: usize) -> &A {
        match self.vec.get(self.vec.len() - 1 - i) {
            Some(a) => a,
            None => panic!("Bad index in env!"),
        }
    }

    pub fn from_vec(vec: Vec<A>) -> Env<A> {
        Env { vec }
    }
}

impl<A: Display> Display for Env<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!(
            "{{ {} }}",
            self.iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        )
        .fmt(f)
    }
}

impl<A: Eq> Env<A> {
    pub fn find_first(&self, a: &A) -> Option<usize> {
        for (i, b) in self.vec.iter().enumerate() {
            if b.eq(a) {
                return Some(i);
            }
        }
        None
    }

    pub fn find_last(&self, a: &A) -> Option<usize> {
        for (i, b) in self.vec.iter().enumerate().rev() {
            if b.eq(a) {
                return Some(i);
            }
        }
        None
    }
}

impl<A: Clone> Env<A> {
    pub fn with(&self, a: A) -> Env<A> {
        let mut vec = self.vec.clone();
        vec.push(a);

        Env { vec }
    }

    pub fn without(&self, index: usize) -> Env<A> {
        let mut vec = self.vec.clone();
        vec.remove(index);

        Env { vec }
    }
}

impl<A> Default for Env<A> {
    fn default() -> Self {
        Env { vec: vec![] }
    }
}
