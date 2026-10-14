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
        InternalError, Val,
    },
    util::{location::Location, ran::Ran},
};

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

enum A<'p> {
    A { tm: core::Tm, error: core::Tm },
    Aa { tm: core::Tm, error: f64 },
    Ab { tm: Val<'p>, error: core::Tm },
    B { ctxs: Vec<(BindContext<'p>, Seq)> },
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

pub mod eval2;

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
    use crate::{
        core::matcher::read_matcher2::ReadMatcher,
        surface::{elab::ElabError, elab2::context::Context, MatchMode, Region},
    };

    impl<'p> ReadMatcher<'p> {
        pub fn infer(
            c0: &Context<'p>,
            regs: &[Region],
            params: Vec<()>,
            mode: &MatchMode,
        ) -> (ReadMatcher<'p>, Context<'p>) {
            todo!()
        }
    }
}
