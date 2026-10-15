use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::{FlattenOk, Itertools};

use crate::{
    core::{
        self,
        eval2::env,
        matcher::{
            read_matcher::Seq,
            read_matcher2::eval2::match_context::{
                bind_context::BindContext, location_context::LocationContext,
            },
        },
        InternalError, ListVal, Val,
    },
    util::{location::Location, ran::Ran},
};

pub mod eval2;

#[derive(Clone)]
pub enum MatchMode {
    All,
    One,
    Unique,
}

impl Display for MatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchMode::All => "all",
            MatchMode::One => "one",
            MatchMode::Unique => "unique",
        }
        .fmt(f)
    }
}

pub struct ReadMatcher<'p> {
    /// The search operations to perform
    pub ops: Vec<Op<'p>>,
    /// The location ranges to bind at the end in each world
    pub binds: Vec<Ran<usize>>,
    /// The location to label to the end
    pub end: usize,
    /// Whether the matcher will return all possible matches, or just one
    pub mode: MatchMode,
    /// Additionally, stay tagged with a location
    pub location: Location,
}

impl<'p> Display for ReadMatcher<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub enum Op<'p> {
    Let {
        // the location to assign to
        loc: usize,
        // the operation to calculate the new position
        op: LocOp,
        // the closest known locations to check bounds against
        flanking: Ran<usize>,
    },
    Restrict {
        // location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
        // partially computed list of binds to make
        a: A<'p>,
    },
}

#[derive(Clone)]
enum A<'p> {
    // fully unevaluated form
    Unevaluated { tm: core::Tm, error: core::Tm },

    // partially evaluated; either the term or the error has been computed
    PartialA { tm: core::Tm, error: f64 },
    PartialB { tm: ListVal<'p>, error: core::Tm },

    // fully evaluated form
    Evaluated { ctxs: Vec<(BindContext<'p>, Seq)> },
}

impl<'p> A<'p> {
    fn get_priority(&self) -> usize {
        match self {
            A::Unevaluated { tm, error } => todo!(),

            // we don't know enough about this form
            A::PartialA { tm, error } => todo!(),

            // computed region results with more options to search for
            // are lower priority
            A::PartialB { tm, error } => tm.get_reference().len(),
            A::Evaluated { ctxs } => ctxs.len(),
        }
    }

    fn reference<'r>(&'r self) -> A<'r> {
        match self {
            A::Unevaluated { .. } | A::PartialA { .. } => self.clone(),
            A::PartialB { tm, error } => A::PartialB {
                tm: tm.reference(),
                error: error.clone(),
            },
            A::Evaluated { ctxs } => A::Evaluated {
                ctxs: ctxs
                    .iter()
                    .map(|(ctx, seq)| (ctx.reference(), seq.clone()))
                    .collect(),
            },
        }
    }
}

pub enum LocOp {
    Var {
        loc: usize,
    },
    Offset {
        loc_op: Arc<LocOp>,
        offset: core::Tm,
        direction: Direction,
    },
}

enum Direction {
    Plus,
    Minus,
}

impl Direction {
    fn get(&self) -> i32 {
        match self {
            Direction::Plus => 1,
            Direction::Minus => -1,
        }
    }
}

pub mod elab2 {
    use itertools::Itertools;

    use crate::{
        core::{
            self,
            matcher::read_matcher2::{Op, ReadMatcher, A},
        },
        surface::{
            self, elab::ElabError, elab2::context::Context, MatchMode, ReadParameter, Region,
            RegionData,
        },
        util::ran::Ran,
    };

    #[derive(Clone)]
    enum FlatRegion {
        Hole,
        Tm {
            // the ids that appear in the term that are also in the param binds
            ids: Vec<String>,
            // the term
            tm: surface::Tm,
            // the term representing the error rate
            error_tm: surface::Tm,
        },
    }

    enum CoreFlatRegion<'p> {
        Hole,
        Tm { a: A<'p> },
    }

    struct CoreReadParameter {
        pub name: String,
        pub tm: core::Tm,
    }

    impl<'p> ReadMatcher<'p> {
        pub fn infer(
            c0: &Context<'p>,
            regions: &[Region],
            params: &[ReadParameter],
            mode: &MatchMode,
        ) -> (ReadMatcher<'p>, Context<'p>) {
            // first, separate all the regions out into
            // - a vec of hole/tm
            // - the list of named, sized, etc with location ranges

            let param_names = params.iter();

            // flatten regions into:
            // - a list of sizes and their spans within the pattern
            // - a list of names and their spans within the pattern
            // - a list of Regs, which are either holes or as-yet-unresolved terms
            let (sized, named, regs): (
                Vec<Ranged<surface::Tm>>,
                Vec<Ranged<String>>,
                Vec<Ranged<FlatRegion>>,
            ) = todo!();

            // go through the params, and typecheck them,
            // building a context with all params in
            let (params, c1): (Vec<CoreReadParameter>, Context<'p>) = todo!();

            // go through the sized, and typecheck them,
            // making sure that each is a number,
            // and evaluating only where possible
            let (sized_ctm, c2): (Vec<Ranged<core::Tm>>, Context<'p>) = todo!();

            // go through the FlatRegions and typecheck,
            // making sure that each is a string,
            // and evaluating only where possible
            // regs.iter()
            //     .fold((c2, vec![]), |(cx, v0), reg| match reg.data {
            //         FlatRegion::Hole => (cx.reference(), v0),
            //         FlatRegion::Tm { ids, tm, error_tm } => {
            //             todo!()
            //         }
            //     });

            let core_regs: Vec<Ranged<CoreFlatRegion<'p>>> = todo!();
            let bindable_regs: Vec<Ranged<A<'p>>> = core_regs
                .iter()
                .flat_map(|ranged| match &ranged.data {
                    CoreFlatRegion::Hole => None,
                    CoreFlatRegion::Tm { a } => Some(Ranged {
                        data: a.reference(),
                        ran: ranged.ran.clone(),
                    }),
                })
                // sort by priority!
                // NOTE this is where we could, if we wanted to, generate a whole bunch of priority orderings,
                // and compare them against each other...
                .sorted_by_key(|ranged| ranged.data.get_priority())
                .collect();

            let mut known = vec![0, core_regs.len()];
            let mut ops: Vec<Op<'p>> = vec![];

            //

            // now, we go through

            todo!()
        }
    }

    #[derive(Clone)]
    struct Ranged<T> {
        data: T,
        ran: Ran<usize>,
    }

    fn flatten(regs: &[Region]) -> () {
        #[derive(Clone, Default)]
        struct Accumulator {
            i: usize,
            sized: Vec<Ranged<surface::Tm>>,
            named: Vec<Ranged<String>>,
            regs: Vec<Ranged<FlatRegion>>,
        }

        impl Accumulator {
            fn with_hole(&self, loc: usize) -> Accumulator {
                todo!()
            }

            fn with_named(&self, name: &str, loc: usize, length: usize) -> Accumulator {
                todo!()
            }

            fn with_sized(&self, size: surface::Tm, loc: usize, length: usize) -> Accumulator {
                todo!()
            }

            fn with_term(&self, tm: surface::Tm, error: surface::Tm, loc: usize) -> Accumulator {
                todo!()
            }
        }

        fn flatten_recursive(regs: &[Region], acc: &Accumulator) -> Accumulator {
            match &regs[..] {
                [] => acc.clone(),
                [first, rest @ ..] => {
                    let first_acc =
                        match &first.data {
                            RegionData::Hole => acc.with_hole(acc.i),
                            RegionData::Term { tm, error } => {
                                acc.with_term(tm.clone(), error.clone(), acc.i)
                            }
                            RegionData::Named { name, regs } => flatten_recursive(regs, acc)
                                .with_named(name, acc.i, get_length(regs)),
                            RegionData::Sized { tm, regs } => flatten_recursive(regs, acc)
                                .with_sized(tm.clone(), acc.i, get_length(regs)),
                        };

                    // then, thread the accumulator through
                    flatten_recursive(rest, &first_acc)
                }
            }
        }

        let final_acc = flatten_recursive(regs, &Accumulator::default());

        todo!()
    }

    fn get_length(regs: &[Region]) -> usize {
        fn get_length_recursive(regs: &[Region], i: usize) -> usize {
            match &regs[..] {
                // on empty list, just return out
                [] => i,
                [first, rest @ ..] => {
                    let first_len = match &first.data {
                        RegionData::Hole | RegionData::Term { tm: _, error: _ } => 1,
                        RegionData::Named { name: _, regs } | RegionData::Sized { tm: _, regs } => {
                            get_length(&regs[..])
                        }
                    };

                    // where we're up to, plus the length of the contents of this region, plus the length of the rest of the regions
                    i + first_len + get_length(rest)
                }
            }
        }

        get_length_recursive(regs, 0)
    }
}
