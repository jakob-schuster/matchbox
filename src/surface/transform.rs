use crate::surface::{BinOp, Num, Pattern, Region, RegionData, Tm, TmData};

impl Pattern {
    /// Performs a program transformation (surface -> surface),
    /// identifying pattern terms which are binary minus terms
    /// and replacing them with two separate regions, one of which
    /// is a unary minus term.
    pub fn transform_binary_minus(regs: &[Region]) -> Vec<Region> {
        fn transform_binary_minus_tm(tm: &Tm) -> Vec<Tm> {
            match &tm.data {
                TmData::BinOp {
                    tm0,
                    tm1,
                    op: BinOp::Minus,
                } => transform_binary_minus_tm(tm0)
                    .into_iter()
                    // check both recursive cases; don't assume associativity
                    .chain(transform_binary_minus_tm(tm1))
                    .collect(),
                _ => vec![tm.clone()],
            }
        }

        fn transform_tm_to_regs(tms: &[Tm], error: &Tm) -> Vec<Region> {
            match &tms[..] {
                [] => vec![],
                [tm] => vec![Region::new(
                    tm.location.clone(),
                    RegionData::Term {
                        tm: tm.clone(),
                        error: error.clone(),
                    },
                )],
                [tm, rest @ ..] => vec![Region::new(
                    tm.location.clone(),
                    RegionData::Term {
                        tm: tm.clone(),
                        // the first term was given no error rate
                        error: Tm::new(tm.location.clone(), TmData::NumLit { n: Num::Int(0) }),
                    },
                )]
                .into_iter()
                .chain(transform_tm_to_regs(rest, error))
                .collect(),
            }
        }

        match regs {
            [] => vec![],
            [reg, rest @ ..] => {
                let regs1 = match &reg.data {
                    RegionData::Hole => vec![reg.clone()],
                    RegionData::Term { tm, error } => {
                        transform_tm_to_regs(&transform_binary_minus_tm(tm), error)
                    }
                    RegionData::Named { name, regs } => vec![Region::new(
                        reg.location.clone(),
                        RegionData::Named {
                            name: name.clone(),
                            regs: Pattern::transform_binary_minus(regs),
                        },
                    )],
                    RegionData::Sized { tm, regs } => vec![Region::new(
                        reg.location.clone(),
                        RegionData::Sized {
                            tm: tm.clone(),
                            regs: Pattern::transform_binary_minus(regs),
                        },
                    )],
                };

                regs1
                    .into_iter()
                    .chain(Pattern::transform_binary_minus(rest))
                    .collect()
            }
        }
    }
}
