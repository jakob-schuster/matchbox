use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::{Either, FlattenOk, Itertools};

use crate::{
    core::{
        self,
        eval2::env,
        matcher::{
            read_matcher::Seq,
            read_matcher2::{
                elab2::{CoreReadParameter, CoreReadParameterData},
                eval2::match_context::{
                    bind_context::BindContext, location_context::LocationContext,
                },
            },
        },
        InternalError, ListVal, StrVal, Val,
    },
    surface::{self, elab2::context::Context},
    util::{location::Location, ran::Ran},
};

pub mod elab2;
pub mod eval2;
pub mod seq;

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

#[derive(Clone)]
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
        data: OpData<'p>,
    },
}

impl<'p> Op<'p> {
    fn reference<'r>(&'r self) -> Op<'r> {
        match self {
            Op::Let { loc, op, flanking } => Op::Let {
                loc: *loc,
                op: op.clone(),
                flanking: flanking.clone(),
            },
            Op::Restrict {
                ran,
                fixed,
                save,
                data,
            } => Op::Restrict {
                ran: ran.clone(),
                fixed: fixed.clone(),
                save: save.clone(),
                data: data.reference(),
            },
        }
    }
}

#[derive(Clone)]
enum OpData<'p> {
    Unevaluated {
        params: Vec<ParamData<'p>>,
        tm: core::Tm,
        error_tm: core::Tm,
    },

    PartialA {
        binds: Vec<BindContext<'p>>,
        tm: core::Tm,
        error_tm: core::Tm,
    },
    PartialB {
        binds: Vec<(BindContext<'p>, StrVal<'p>)>,
        error_tm: core::Tm,
    },
    PartialC {
        binds: Vec<(BindContext<'p>, f64)>,
        tm: core::Tm,
    },

    FullyEvaluated {
        binds: Vec<(BindContext<'p>, Seq)>,
    },
}

impl<'p> OpData<'p> {
    fn new_unevaluated(params: &[ParamData<'p>], tm: core::Tm, error_tm: core::Tm) -> OpData<'p> {
        OpData::Unevaluated {
            params: params.to_vec(),
            tm,
            error_tm,
        }
    }

    fn new_partial_a(binds: &[BindContext<'p>], tm: core::Tm, error_tm: core::Tm) -> OpData<'p> {
        OpData::PartialA {
            binds: binds.to_vec(),
            tm,
            error_tm,
        }
    }

    fn new_partial_b(binds: &[(BindContext<'p>, StrVal<'p>)], error_tm: core::Tm) -> OpData<'p> {
        OpData::PartialB {
            binds: binds.to_vec(),
            error_tm,
        }
    }

    fn new_partial_c(binds: &[(BindContext<'p>, f64)], tm: core::Tm) -> OpData<'p> {
        OpData::PartialC {
            binds: binds.to_vec(),
            tm,
        }
    }

    fn new_evaluated(binds: &[(BindContext<'p>, Seq)]) -> OpData<'p> {
        OpData::FullyEvaluated {
            binds: binds.to_vec(),
        }
    }

    fn reference<'r>(&'r self) -> OpData<'r> {
        todo!()
    }

    fn get_priority(&self) -> usize {
        todo!()
    }
}

#[derive(Clone)]
struct ParamContext<'p> {
    map: HashMap<String, ParamData<'p>>,
}

impl<'p> ParamContext<'p> {
    fn new() -> ParamContext<'p> {
        todo!()
    }

    fn with(&self, name: &str, param_data: ParamData<'p>) -> ParamContext<'p> {
        todo!()
    }

    fn get(&self, name: &str) -> Option<ParamData<'p>> {
        todo!()
    }
}

#[derive(Clone)]
enum ParamData<'p> {
    Unevaluated {
        index: usize,
        tm: core::Tm,
        ty: core::Val<'p>,
    },
    Evaluated {
        index: usize,
        vals: ListVal<'p>,
        ty: core::Val<'p>,
    },
}

impl<'p> ParamData<'p> {
    fn new_unevaluated(index: usize, tm: core::Tm, ty: core::Val<'p>) -> ParamData<'p> {
        ParamData::Unevaluated { index, tm, ty }
    }

    fn new_evaluated(index: usize, vals: ListVal<'p>, ty: core::Val<'p>) -> ParamData<'p> {
        ParamData::Evaluated { index, vals, ty }
    }

    fn get_ty(&self) -> &Val<'p> {
        match self {
            ParamData::Unevaluated { index, tm, ty } => ty,
            ParamData::Evaluated { index, vals, ty } => ty,
        }
    }

    fn get_index(&self) -> usize {
        match self {
            ParamData::Unevaluated { index, tm, ty } => *index,
            ParamData::Evaluated { index, vals, ty } => *index,
        }
    }
}

struct Bind<'p> {
    index: usize,
    val: Val<'p>,
}

struct ATm<'p> {
    params: Vec<CoreReadParameterData<'p>>,
    tm: core::Tm,
    error: core::Tm,
}

#[derive(Clone)]
enum A<'p> {
    // fully unevaluated form
    Unevaluated {
        params: Vec<Either<&'p core::Tm, ListVal<'p>>>,
        tm: &'p core::Tm,
        error: &'p core::Tm,
    },

    // partially evaluated; the params have been evaluated
    PartialParams {
        ctxs: Vec<env::Env<Val<'p>>>,
        tm: &'p core::Tm,
        error: &'p core::Tm,
    },

    // partially evaluated; either the term or the error rate has additionally been evaluated
    PartialTm {
        ctxs: Vec<(env::Env<Val<'p>>, StrVal<'p>)>,
        error: &'p core::Tm,
    },
    PartialError {
        ctxs: Vec<(env::Env<Val<'p>>, f64)>,
        tm: &'p core::Tm,
    },

    // fully evaluated form
    // contains one world per possible set of input binds
    Evaluated {
        ctxs: Vec<(env::Env<Val<'p>>, Seq)>,
    },
}

#[derive(Clone)]
struct A2<'p> {
    ctxs: Either<
        (
            Vec<(usize, Either<core::Tm, ListVal<'p>>)>,
            core::Tm,
            core::Tm,
        ),
        Vec<(
            Vec<(usize, Val<'p>)>,
            Either<(Either<core::Tm, StrVal<'p>>, Either<core::Tm, f64>), Seq>,
        )>,
    >,
}

impl<'p> A<'p> {
    fn new(params: &'p [core::Tm], tm: &'p core::Tm, error: &'p core::Tm) -> A<'p> {
        A::Unevaluated {
            // WARN could be costly clone
            params: params.iter().map(|param| Either::Left(param)).collect(),
            tm,
            error,
        }
    }

    fn get_priority(&self) -> usize {
        match self {
            // we don't know enough about this form
            A::Unevaluated { tm, .. } => todo!(),

            // computed region results with more options to search for
            // are lower priority
            A::PartialParams { ctxs, .. } => ctxs.len(),
            A::PartialTm { ctxs, .. } => ctxs.len(),
            A::PartialError { ctxs, .. } => ctxs.len(),
            A::Evaluated { ctxs, .. } => ctxs.len(),
        }
    }

    fn reference<'r>(&'r self) -> A<'r> {
        match self {
            A::Unevaluated { params, tm, error } => A::Unevaluated {
                params: params.to_vec(),
                tm: tm.clone(),
                error: error.clone(),
            },
            A::PartialParams { ctxs, tm, error } => A::PartialParams {
                ctxs: ctxs.iter().map(env::Env::reference).collect(),
                tm: tm.clone(),
                error: error.clone(),
            },
            A::PartialTm { ctxs, error } => A::PartialTm {
                ctxs: ctxs
                    .iter()
                    .map(|(bind_context, s)| (bind_context.reference(), s.reference()))
                    .collect(),
                error: error.clone(),
            },
            A::PartialError { ctxs, tm } => A::PartialError {
                ctxs: ctxs.iter().map(env::Env::reference).collect(),
                tm: tm.clone(),
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

#[derive(Clone)]
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

#[derive(Clone)]
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
