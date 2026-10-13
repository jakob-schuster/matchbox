use std::{collections::HashMap, sync::Arc};

use crate::{
    core::{self, eval2::env, matcher::read_matcher::Seq, Val},
    util::ran::Ran,
};

pub struct ReadMatcher {}

pub enum Op<'p> {
    Let {
        loc: usize,
        op: LocOp,
    },
    Restrict {
        ran: Ran<usize>,
        fixed: Ran<bool>,
        save: Vec<Ran<usize>>,
        a: A<'p>,
    },
}

enum A<'p> {
    A {
        tm: core::Tm,
        error: core::Tm,
    },
    Aa {
        tm: core::Tm,
        error: f64,
    },
    Ab {
        tm: Val<'p>,
        error: core::Tm,
    },
    B {
        ctxs: Vec<(HashMap<usize, Val<'p>>, Seq)>,
    },
}

impl<'p> A<'p> {
    fn eval<'r: 'a, 'a>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
    ) -> &Vec<(HashMap<usize, Val<'p>>, Seq)> {
        match self {
            A::A { tm, error } => todo!(),
            A::Aa { tm, error } => todo!(),
            A::Ab { tm, error } => todo!(),
            A::B { ctxs } => ctxs,
        }
    }
}

impl<'p> Op<'p> {
    fn exec<'a, 'r>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        seq: &'r [u8],
        // loc_ctx:
    ) where
        'p: 'r,
        'r: 'a,
    {
        match self {
            Op::Let { loc, op } => todo!(),
            Op::Restrict {
                ran,
                fixed,
                save,
                a,
            } => {
                // we must be able to concretise A at this point; otherwise, we're in trouble
                let ctxs = a.eval(global, local);

                todo!()
            }
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
