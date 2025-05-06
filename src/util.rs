use std::fmt::{self, Display, Formatter};

use crate::core::InternalError;

pub type Arena = bumpalo::Bump;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Location {
    pub start: usize,
    pub end: usize,
}

impl Location {
    pub fn new(start: usize, end: usize) -> Location {
        Location { start, end }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Located<T> {
    pub location: Location,
    pub data: T,
}

impl<T> Located<T> {
    pub fn new(location: Location, data: T) -> Located<T> {
        Located { location, data }
    }
}

impl<T: Display> Display for Located<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.fmt(f)
    }
}

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

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CoreRecField<'a, T> {
    pub name: &'a [u8],
    pub data: T,
}

impl<'a, T> CoreRecField<'a, T> {
    pub fn new(name: &'a [u8], data: T) -> CoreRecField<'a, T> {
        CoreRecField { name, data }
    }
}

impl<'a, T: Display> Display for CoreRecField<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!(
            "{} = {}",
            String::from_utf8(self.name.to_vec()).unwrap(),
            self.data
        )
        .fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct Env<A> {
    vec: Vec<A>,
}

impl<A> Env<A> {
    pub fn iter(&self) -> std::slice::Iter<'_, A> {
        self.vec.iter()
    }

    pub fn get_level(&self, i: usize) -> &A {
        match self.vec.get(i) {
            Some(a) => a,
            None => panic!("Bad index in env!"),
        }
    }

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

#[derive(Clone, Debug)]
pub struct Cache<A> {
    vec: Vec<A>,
}

impl<A: Clone + Display> Cache<A> {
    pub fn push(&self, a: A) -> (Cache<A>, usize) {
        let index = self.vec.len();
        let mut vec = self.vec.clone();
        vec.push(a);

        (Cache { vec }, index)
    }

    pub fn get(&self, index: usize) -> &A {
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

pub fn bytes_to_string(bytes: &[u8]) -> Result<String, InternalError> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| InternalError::new("couldn't convert bytes to string!?"))
}

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
    pub fn disjoint(&self, other: &Self) -> bool {
        other.start >= self.end || self.start >= other.end
    }
}

/// Translates a sequence into protein.
pub fn translate(seq: &[u8]) -> String {
    fn translate_codon(codon: &[u8]) -> char {
        match codon {
            b"TTT" => 'F',
            b"TTC" => 'F',
            b"TTA" => 'L',
            b"TTG" => 'L',
            b"TCT" => 'S',
            b"TCC" => 'S',
            b"TCA" => 'S',
            b"TCG" => 'S',
            b"TAT" => 'Y',
            b"TAC" => 'Y',
            b"TAA" => '-',
            b"TAG" => '-',
            b"TGT" => 'C',
            b"TGC" => 'C',
            b"TGA" => '-',
            b"TGG" => 'W',

            b"CTT" => 'L',
            b"CTC" => 'L',
            b"CTA" => 'L',
            b"CTG" => 'L',
            b"CCT" => 'P',
            b"CCC" => 'P',
            b"CCA" => 'P',
            b"CCG" => 'P',
            b"CAT" => 'H',
            b"CAC" => 'H',
            b"CAA" => 'Q',
            b"CAG" => 'Q',
            b"CGT" => 'R',
            b"CGC" => 'R',
            b"CGA" => 'R',
            b"CGG" => 'R',

            b"ATT" => 'I',
            b"ATC" => 'I',
            b"ATA" => 'I',
            b"ATG" => 'M',
            b"ACT" => 'T',
            b"ACC" => 'T',
            b"ACA" => 'T',
            b"ACG" => 'T',
            b"AAT" => 'N',
            b"AAC" => 'N',
            b"AAA" => 'K',
            b"AAG" => 'K',
            b"AGT" => 'S',
            b"AGC" => 'S',
            b"AGA" => 'R',
            b"AGG" => 'R',

            b"GTT" => 'V',
            b"GTC" => 'V',
            b"GTA" => 'V',
            b"GTG" => 'V',
            b"GCT" => 'A',
            b"GCC" => 'A',
            b"GCA" => 'A',
            b"GCG" => 'A',
            b"GAT" => 'D',
            b"GAC" => 'D',
            b"GAA" => 'E',
            b"GAG" => 'E',
            b"GGT" => 'G',
            b"GGC" => 'G',
            b"GGA" => 'G',
            b"GGG" => 'G',

            _ => '?',
        }
    }

    if seq.len() < 3 {
        // terminate on the end of sequences
        String::new()
    } else {
        // or, recursively call
        format!("{}{}", translate_codon(&seq[..3]), translate(&seq[3..]))
    }
}

pub fn rev_comp(seq: &[u8]) -> Vec<u8> {
    bio::alphabets::dna::revcomp(seq)
}
