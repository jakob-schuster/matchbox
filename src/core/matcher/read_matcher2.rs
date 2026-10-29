use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::{Either, FlattenOk, Itertools};

use crate::{
    core::{
        self,
        eval2::env,
        matcher::read_matcher2::{
            elab2::{CoreReadParameter, CoreReadParameterData},
            eval2::match_context::{bind_context::BindContext, location_context::LocationContext},
            seq::Seq,
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

    fn concretise<'q>(&self) -> Op<'q> {
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
                data: data.concretise(),
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

    fn concretise<'q>(&self) -> OpData<'q> {
        match self {
            OpData::Unevaluated {
                params,
                tm,
                error_tm,
            } => OpData::Unevaluated {
                params: params.iter().map(|param| param.concretise()).collect(),
                tm: tm.clone(),
                error_tm: error_tm.clone(),
            },
            OpData::PartialA {
                binds,
                tm,
                error_tm,
            } => OpData::PartialA {
                binds: binds.iter().map(|bind_ctx| bind_ctx.concretise()).collect(),
                tm: tm.clone(),
                error_tm: error_tm.clone(),
            },
            OpData::PartialB { binds, error_tm } => OpData::PartialB {
                binds: binds
                    .iter()
                    .map(|(bind_ctx, str_val)| (bind_ctx.concretise(), str_val.concretise()))
                    .collect(),
                error_tm: error_tm.clone(),
            },
            OpData::PartialC { binds, tm } => OpData::PartialC {
                binds: binds
                    .iter()
                    .map(|(bind_ctx, error_val)| (bind_ctx.concretise(), *error_val))
                    .collect(),
                tm: tm.clone(),
            },
            OpData::FullyEvaluated { binds } => OpData::FullyEvaluated {
                binds: binds
                    .iter()
                    .map(|(bind_ctx, seq)| (bind_ctx.concretise(), seq.clone()))
                    .collect(),
            },
        }
    }

    fn get_priority(&self) -> usize {
        match self {
            // if unevaluated, we can just roughly estimate by how many parameters are involved
            OpData::Unevaluated { params, .. } => params.len(),

            // otherwise, based on the number of different bind contexts that must be considered
            OpData::PartialA { binds, .. } => binds.len(),
            OpData::PartialB { binds, .. } => binds.len(),
            OpData::PartialC { binds, .. } => binds.len(),
            OpData::FullyEvaluated { binds } => binds.len(),
        }
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

    fn reference<'r>(&'r self) -> ParamData<'r> {
        match self {
            ParamData::Unevaluated { index, tm, ty } => ParamData::Unevaluated {
                index: *index,
                tm: tm.clone(),
                ty: ty.reference(),
            },
            ParamData::Evaluated { index, vals, ty } => ParamData::Evaluated {
                index: *index,
                vals: vals.reference(),
                ty: ty.reference(),
            },
        }
    }

    fn concretise<'q>(&self) -> ParamData<'q> {
        match self {
            ParamData::Unevaluated { index, tm, ty } => ParamData::Unevaluated {
                index: *index,
                tm: tm.clone(),
                ty: ty.concretise(),
            },
            ParamData::Evaluated { index, vals, ty } => ParamData::Evaluated {
                index: *index,
                vals: vals.concretise(),
                ty: ty.concretise(),
            },
        }
    }
}

struct Bind<'p> {
    index: usize,
    val: Val<'p>,
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
