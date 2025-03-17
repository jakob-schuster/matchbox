use bio::pattern_matching::myers::{long, Myers};
use itertools::Itertools;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VarMyers {
    Short(Myers<u64>),
    Long(long::Myers<u64>),
}

impl VarMyers {
    pub fn new(seq: &[u8]) -> Self {
        if seq.len() <= 64 {
            VarMyers::Short(Myers::<u64>::new(seq))
        } else {
            VarMyers::Long(long::Myers::<u64>::new(seq))
        }
    }

    pub fn find_all(&self, seq: &[u8], edit_dist: u8) -> Vec<(usize, usize, usize)> {
        match self {
            VarMyers::Short(s) => s
                .clone()
                .find_all(seq, edit_dist)
                .map(|(s, e, d)| (s, e, d as usize))
                .collect_vec(),
            VarMyers::Long(l) => l.clone().find_all(seq, edit_dist as usize).collect_vec(),
        }
    }

    pub fn find_all_disjoint(&self, seq: &[u8], edit_dist: u8) -> Vec<(usize, usize, usize)> {
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
