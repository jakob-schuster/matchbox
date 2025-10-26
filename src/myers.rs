//! Match sequences with error tolerance using [Myers' dynamic programming algorithm](https://dl.acm.org/doi/10.1145/316542.316550), with the Rust Bio implementation.

use bio::pattern_matching::myers::{long, Myers};
use itertools::Itertools;

/// Myers (from the Rust Bio crate) can either be short or long, with different implementation.
/// Wrap both `Myers<u64>` and `long::Myers<u64>`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VarMyers {
    Short(Myers<u64>),
    Long(long::Myers<u64>),
}

impl VarMyers {
    /// Create a new VarMyers, selecting which enum branch based on the length of the sequence.
    pub fn new(seq: &[u8]) -> Self {
        if seq.len() <= 64 {
            VarMyers::Short(Myers::<u64>::new(seq))
        } else {
            VarMyers::Long(long::Myers::<u64>::new(seq))
        }
    }

    /// Find all matches within a target sequence, below a given edit distance.
    pub fn find_all(&self, seq: &[u8], edit_dist: usize) -> Vec<(usize, usize, usize)> {
        match self {
            VarMyers::Short(s) => s
                .clone()
                .find_all(seq, edit_dist as u8)
                .map(|(s, e, d)| (s, e, d as usize))
                .collect_vec(),
            VarMyers::Long(l) => l.clone().find_all(seq, edit_dist as usize).collect_vec(),
        }
    }

    /// Find all disjoint matches within a target sequence, below a given edit distance.
    pub fn find_all_disjoint(&self, seq: &[u8], edit_dist: usize) -> Vec<(usize, usize, usize)> {
        let mut matches = self.find_all(seq, edit_dist);

        matches.sort_by_key(|(_, _, dist)| *dist);

        fn disjoint(m1: &(usize, usize, usize), m2: &(usize, usize, usize)) -> bool {
            let (start1, end1, _) = *m1;
            let (start2, end2, _) = *m2;

            start2 >= end1 || start1 >= end2
        }

        let mut best: Vec<(usize, usize, usize)> = vec![];
        for m @ (_, _, _) in matches {
            let dis = best
                .clone()
                .into_iter()
                .map(|n| disjoint(&m, &n))
                .all(|b| b);

            if dis {
                best.push(m.to_owned());
            }
        }

        best
    }
}
