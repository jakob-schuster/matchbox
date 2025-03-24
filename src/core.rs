use std::{collections::HashMap, fmt::Display, sync::Arc};

use bio::stats::probs;
use itertools::Itertools;
use matcher::Matcher;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rec::{ConcreteRec, FastaRead, Rec};

use crate::util::{self, Arena, CoreRecField, Env, Located, Location, RecField};

pub mod library;
pub mod matcher;
pub mod rec;

#[derive(Clone, Debug)]
pub struct EvalError {
    pub location: Location,
    pub message: String,
}

impl EvalError {
    fn new(location: &Location, message: &str) -> EvalError {
        EvalError {
            location: location.clone(),
            message: message.to_string(),
        }
    }

    fn from_internal(e: InternalError, location: Location) -> EvalError {
        EvalError {
            location,
            message: e.message,
        }
    }
}

pub type Prog<'p: 'a, 'a> = Located<ProgData<'p, 'a>>;
#[derive(Clone)]
pub struct ProgData<'p: 'a, 'a> {
    pub stmt: Stmt<'p, 'a>,
}

impl<'a, 'p: 'a> Prog<'p, 'a> {
    pub fn eval(&self, read: &'a Val<'p, 'a>, arena: &'a Arena) -> Result<Vec<Effect>, EvalError> {
        let new_env: Env<&'a Val<'p, 'a>> = todo!();
        eval_stmt(arena, &new_env.with(read), &self.data.stmt)
    }
}

pub type Stmt<'p: 'a, 'a> = Located<StmtData<'p, 'a>>;
#[derive(Clone)]
pub enum StmtData<'p: 'a, 'a> {
    Let {
        tm: Tm<'p, 'a>,
        next: Arc<Stmt<'p, 'a>>,
    },
    Out {
        tm0: Tm<'p, 'a>,
        tm1: Tm<'p, 'a>,
        next: Arc<Stmt<'p, 'a>>,
    },
    If {
        branches: Vec<Branch<'p, 'a>>,
        next: Arc<Stmt<'p, 'a>>,
    },
    End,
}

pub type Branch<'p: 'a, 'a> = Located<BranchData<'p, 'a>>;
#[derive(Clone)]
pub enum BranchData<'p: 'a, 'a> {
    Bool {
        tm: Tm<'p, 'a>,
        stmt: Stmt<'p, 'a>,
    },

    Is {
        tm: Tm<'p, 'a>,
        branches: Vec<PatternBranch<'p, 'a>>,
    },
}

pub type PatternBranch<'p: 'a, 'a> = Located<PatternBranchData<'p, 'a>>;
#[derive(Clone)]
pub struct PatternBranchData<'p: 'a, 'a> {
    pub matcher: Arc<dyn Matcher<'p, 'a> + 'a>,
    pub stmt: Stmt<'p, 'a>,
}

pub fn eval_prog<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'p, 'a>>,
    prog: &Prog<'p, 'a>,
) -> Result<Vec<Effect>, EvalError> {
    eval_stmt(arena, env, &prog.data.stmt)
}

fn eval_stmt<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'p, 'a>>,
    stmt: &Stmt<'p, 'a>,
) -> Result<Vec<Effect>, EvalError> {
    match &stmt.data {
        StmtData::Let { tm, next } => {
            // evaluate the term
            let val = eval(arena, env, tm)?;
            // and then evaluate the statement with that binding
            eval_stmt(arena, &env.with(val), next)
        }
        StmtData::Out { tm0, tm1, next } => {
            let val0 = eval(arena, env, tm0)?;
            let val1 = eval(arena, env, tm1)?;

            // export the values to be portable
            Ok([Effect {
                val: make_portable(arena, val0),
                handler: make_portable(arena, val1),
            }]
            .into_iter()
            .chain(eval_stmt(arena, env, next)?)
            .collect::<Vec<_>>())
        }
        StmtData::If { branches, next } => {
            let vec: Vec<Effect> = {
                for branch in branches {
                    if let Some(vec) = eval_branch(arena, env, branch)? {
                        return Ok(vec);
                    }
                }

                Ok(vec![])
            }?;

            Ok(vec
                .into_iter()
                .chain(eval_stmt(arena, env, next)?)
                .collect::<Vec<_>>())
        }
        StmtData::End => Ok(vec![]),
    }
}

fn eval_branch<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'p, 'a>>,
    branch: &Branch<'p, 'a>,
) -> Result<Option<Vec<Effect>>, EvalError> {
    match &branch.data {
        BranchData::Bool { tm, stmt } => match eval(arena, env, tm)? {
            Val::Bool { b } => match b {
                true => Ok(Some(eval_stmt(arena, env, stmt)?)),
                false => Ok(None),
            },
            _ => Err(EvalError::from_internal(
                InternalError {
                    message: "expected bool in branch?!".to_string(),
                },
                tm.location.clone(),
            )),
        },
        BranchData::Is { tm, branches } => {
            let val = eval(arena, env, tm)?;

            for branch in branches {
                if let Some(vec) = eval_pattern_branch(arena, env, branch, val)? {
                    return Ok(Some(vec));
                }
            }

            // if no branch in the is has matched, continue on with the branches
            Ok(None)
        }
    }
}

fn eval_pattern_branch<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'p, 'a>>,
    branch: &PatternBranch<'p, 'a>,
    val: &'a Val<'p, 'a>,
) -> Result<Option<Vec<Effect>>, EvalError> {
    match &branch.data.matcher.evaluate(arena, env, val)?[..] {
        [] => Ok(None),
        bind_options => Ok(Some(
            bind_options
                .iter()
                .map(|binds| {
                    eval_stmt(
                        arena,
                        &binds.iter().fold(env.clone(), |env0, bind| env0.with(bind)),
                        &branch.data.stmt,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
        )),
    }
}

pub type Tm<'p: 'a, 'a> = Located<TmData<'p, 'a>>;
#[derive(Clone)]
pub enum TmData<'p: 'a, 'a> {
    Var {
        index: usize,
    },

    Univ,
    AnyTy,

    BoolTy,
    BoolLit {
        b: bool,
    },

    NumTy,
    NumLit {
        n: f32,
    },

    StrTy,
    StrLit {
        s: &'a [u8],
    },

    // list types
    ListTy {
        ty: Arc<Tm<'p, 'a>>,
    },
    ListLit {
        tms: Vec<Tm<'p, 'a>>,
    },

    FunTy {
        args: Vec<Tm<'p, 'a>>,
        body: Arc<Tm<'p, 'a>>,
    },
    // can't remember why we need args still...
    FunLit {
        args: Vec<Tm<'p, 'a>>,
        body: Arc<Tm<'p, 'a>>,
    },
    FunForeignLit {
        args: Vec<Tm<'p, 'a>>,
        body: Arc<
            dyn Fn(&'a Arena, &Location, &[&'a Val<'p, 'a>]) -> Result<&'a Val<'p, 'a>, EvalError>
                + Send
                + Sync,
        >,
    },
    FunApp {
        head: Arc<Tm<'p, 'a>>,
        args: Vec<Tm<'p, 'a>>,
    },

    RecTy {
        fields: Vec<CoreRecField<'a, Tm<'p, 'a>>>,
    },
    RecWithTy {
        fields: Vec<CoreRecField<'a, &'a Tm<'p, 'a>>>,
    },
    RecLit {
        fields: Vec<CoreRecField<'a, Tm<'p, 'a>>>,
    },
    RecProj {
        tm: Arc<Tm<'p, 'a>>,
        name: &'a [u8],
    },
}

pub fn eval<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'p, 'a>>,
    tm: &Tm<'p, 'a>,
) -> Result<&'a Val<'p, 'a>, EvalError> {
    match &tm.data {
        // look up the variable in the environment
        TmData::Var { index } => Ok(env.get_index(*index)),

        TmData::Univ => Ok(arena.alloc(Val::Univ)),
        TmData::AnyTy => Ok(arena.alloc(Val::AnyTy)),

        TmData::BoolTy => Ok(arena.alloc(Val::BoolTy)),
        TmData::BoolLit { b } => Ok(arena.alloc(Val::Bool { b: *b })),
        TmData::NumTy => Ok(arena.alloc(Val::NumTy)),
        TmData::NumLit { n } => Ok(arena.alloc(Val::Num { n: n.clone() })),
        TmData::StrTy => Ok(arena.alloc(Val::StrTy)),
        TmData::StrLit { s } => Ok(arena.alloc(Val::Str { s: s })),

        TmData::FunTy { args, body } => Ok(arena.alloc(Val::FunTy {
            args: args
                .iter()
                .map(|arg| eval(arena, env, arg))
                .collect::<Result<Vec<_>, EvalError>>()?,
            body: eval(arena, env, body)?,
        })),
        TmData::FunLit { args, body } => Ok(arena.alloc(Val::Fun {
            data: FunData {
                env: env.clone(),
                body: body.as_ref().clone(),
            },
        })),
        TmData::FunForeignLit { args, body } => {
            Ok(arena.alloc(Val::FunForeign { f: body.clone() }))
        }
        TmData::FunApp { head, args } => app(
            arena,
            &tm.location,
            eval(arena, env, head)?,
            args.iter()
                .map(|arg| eval(arena, env, arg))
                .collect::<Result<Vec<_>, _>>()?,
        ),

        TmData::ListTy { ty } => Ok(arena.alloc(Val::ListTy {
            ty: eval(arena, env, ty)?,
        })),
        TmData::ListLit { tms } => Ok(arena.alloc(Val::List {
            v: tms
                .iter()
                .map(|tm| eval(arena, env, tm))
                .collect::<Result<Vec<_>, _>>()?,
        })),

        TmData::RecTy { fields } => Ok(arena.alloc(Val::RecTy {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(CoreRecField::new(
                        field.name,
                        eval(arena, env, &field.data)?,
                    ))
                })
                .collect::<Result<Vec<_>, EvalError>>()?,
        })),
        TmData::RecWithTy { fields } => Ok(arena.alloc(Val::RecWithTy {
            fields: fields
                .iter()
                .map(|field| {
                    Ok(CoreRecField::new(
                        field.name,
                        eval(arena, env, &field.data)?,
                    ))
                })
                .collect::<Result<Vec<_>, EvalError>>()?,
        })),

        // allocate a RecLit as a ConcreteRec
        TmData::RecLit { fields } => Ok(arena.alloc(Val::Rec(
            arena.alloc(ConcreteRec {
                map: fields
                    .iter()
                    .map(|field| {
                        Ok((
                            field.name,
                            arena.alloc(eval(arena, env, &field.data)?) as &Val<'p, 'a>,
                        ))
                    })
                    .collect::<Result<HashMap<_, _>, EvalError>>()?,
            }),
        ))),
        TmData::RecProj { tm: head_tm, name } => match eval(arena, env, head_tm)? {
            Val::Rec(r) => {
                let e = r
                    .get(name, arena)
                    .map_err(|e| EvalError::from_internal(e, tm.location.clone()))?;

                Ok(e)
            }
            _ => Err(EvalError::from_internal(
                InternalError {
                    message: "trying to access field of non-record value?!".to_string(),
                },
                tm.location.clone(),
            )),
        },
    }
}

/// Function type that explicitly captures its environment.
#[derive(Clone)]
pub struct FunData<'p: 'a, 'a> {
    pub env: Env<&'a Val<'p, 'a>>,
    pub body: Tm<'p, 'a>,
}

impl<'p: 'a, 'a> FunData<'p, 'a> {
    pub fn app(
        &self,
        arena: &'a Arena,
        args: &[&'a Val<'p, 'a>],
    ) -> Result<&'a Val<'p, 'a>, EvalError> {
        let new_env = args
            .iter()
            .fold(self.env.clone(), |env0, arg| env0.with(arg));

        eval(arena, &new_env, &self.body)
    }
}

#[derive(Clone)]
pub enum Val<'p: 'a, 'a> {
    /// Atomic values
    Univ,
    AnyTy,

    BoolTy,
    Bool {
        b: bool,
    },

    NumTy,
    Num {
        n: f32,
    },

    StrTy,
    Str {
        s: &'a [u8],
    },

    ListTy {
        ty: &'a Val<'p, 'a>,
    },
    List {
        v: Vec<&'a Val<'p, 'a>>,
    },

    /// Record values; can have any backend that implements the trait.
    /// This allows us to have some Record values that merely wrap the
    /// structures provided by readers.
    RecTy {
        fields: Vec<CoreRecField<'a, &'a Val<'p, 'a>>>,
    },
    // A record type which requires certain fields;
    // other fields can also be harmlessly present
    RecWithTy {
        fields: Vec<CoreRecField<'a, &'a Val<'p, 'a>>>,
    },
    Rec(&'a dyn Rec<'p, 'a>),

    /// Function value; defunctionalised, carries the context it needs
    /// and the Rust function it will execute, which takes this carried
    /// context and any arguments.
    FunTy {
        args: Vec<&'a Val<'p, 'a>>,
        body: &'a Val<'p, 'a>,
    },
    Fun {
        data: FunData<'p, 'a>,
    },
    FunForeign {
        f: Arc<
            dyn Fn(&'a Arena, &Location, &[&'a Val<'p, 'a>]) -> Result<&'a Val<'p, 'a>, EvalError>
                + Send
                + Sync,
        >,
    },
    /// Represents a dependent return type of a function,
    /// which can only be elaborated when the function is actually applied.
    /// Should probably come back and clean all of this up conceptually.
    FunReturnTyAwaiting {
        data: FunData<'p, 'a>, // expected_ty: Arc<Val<'a>>,
    },

    Neutral {
        neutral: Neutral<'p, 'a>,
    },
}

impl<'p: 'a, 'arena: 'a, 'a> Val<'p, 'a> {
    pub fn equiv(&self, other: &Val<'p, 'a>) -> bool {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<&Val<'p, 'a>>], name: &[u8]| -> Option<Val<'p, 'a>> {
            let mut out = None;
            for field in fields {
                if field.name.eq(name) {
                    out = Some(field.data.clone())
                }
            }
            out
        };

        match (self, other) {
            // Neutrals we just let through; what else can we do?
            (Val::Neutral { .. }, _) | (_, Val::Neutral { .. }) => true,
            // Similar with awaiting
            (Val::FunReturnTyAwaiting { .. }, _) | (_, Val::FunReturnTyAwaiting { .. }) => true,

            // Any is equivalent to everything
            (Val::AnyTy, _) | (_, Val::AnyTy) => true,

            // trivially equivalent
            (Val::Univ, Val::Univ)
            | (Val::BoolTy, Val::BoolTy)
            | (Val::NumTy, Val::NumTy)
            | (Val::StrTy, Val::StrTy) => true,

            // equivalent if argument types are the same
            // and return types are the same
            (
                Val::FunTy {
                    args: args1,
                    body: body1,
                },
                Val::FunTy {
                    args: args2,
                    body: body2,
                },
            ) => {
                args1.len().eq(&args2.len())
                    && args1.iter().zip(args2).all(|(arg1, arg2)| arg1.equiv(arg2))
                    && body1.equiv(body2)
            }

            // equivalent if all fields are the same
            (Val::RecTy { fields: fields1 }, Val::RecTy { fields: fields2 }) => {
                let mut names = fields1
                    .iter()
                    .chain(fields2)
                    .map(|a| a.name.clone())
                    .unique();

                fields1.len().eq(&fields2.len())
                    && names.all(|name| match (get(&fields1, &name), get(&fields2, &name)) {
                        // both recs must contain all fields, and the types must be equivalent
                        (Some(vty1), Some(vty2)) => vty1.equiv(&vty2),
                        // any fields missing is a type error
                        _ => false,
                    })
            }

            // if one record type contains equivalent fields to each one
            // required by a record type containing, they are equivalent
            (
                Val::RecTy { fields },
                Val::RecWithTy {
                    fields: with_fields,
                },
            )
            | (
                Val::RecWithTy {
                    fields: with_fields,
                },
                Val::RecTy { fields },
            ) => {
                let names = fields.iter().map(|CoreRecField { name, .. }| name);
                let with_names = fields.iter().map(|CoreRecField { name, .. }| name);

                // all the names in the RecWith should be present in the Rec
                names.clone().all(|name| with_names.clone().contains(name))
                    && fields
                        .iter()
                        .all(|field| match get(with_fields, &field.name) {
                            // if it contains it, they must be equiv
                            Some(ty) => field.data.equiv(&ty),
                            // if it doesn't contain it, that's fine
                            None => true,
                        })
            }

            // two RecTypeContaining are equivalent if their shared fields are equivalent
            // (any missing field could always be in the margin of flexibility)
            (Val::RecWithTy { fields: fields1 }, Val::RecWithTy { fields: fields2 }) => {
                let names1 = fields1.iter().map(|CoreRecField { name, .. }| name);
                let names2 = fields2.iter().map(|CoreRecField { name, .. }| name);
                let shared_names = names1.filter(|name| names2.clone().contains(name));

                shared_names
                    .clone()
                    .all(|name| match (get(fields1, name), get(fields2, name)) {
                        // if they both contain it, the types must be equivalent
                        (Some(ty1), Some(ty2)) => ty1.equiv(&ty2),
                        // if just one of them has it, it doesn't matter what the type is
                        (None, Some(_)) | (Some(_), None) => true,
                        // should never happen
                        (None, None) => panic!("neither contains the name?!"),
                    })
            }

            _ => false,
        }
    }

    pub fn eq(&self, arena: &'arena Arena, other: &Val<'p, 'a>) -> bool {
        match (self, other) {
            (Val::Univ, Val::Univ)
            | (Val::AnyTy, Val::AnyTy)
            | (Val::BoolTy, Val::BoolTy)
            | (Val::NumTy, Val::NumTy)
            | (Val::StrTy, Val::StrTy) => true,

            (Val::Bool { b: b1 }, Val::Bool { b: b2 }) => b1.eq(b2),
            (Val::Num { n: n1 }, Val::Num { n: n2 }) => n1.eq(n2),
            (Val::Str { s: s1 }, Val::Str { s: s2 }) => s1.eq(s2),

            // check that all the fields of r1 and all the fields of r2 are the same
            (Val::Rec(r1), Val::Rec(r2)) => {
                let fields1 = r1.all(arena);
                let fields2 = r2.all(arena);
                let mut names = fields1.iter().chain(fields2.iter()).map(|(name, _)| name);

                fields1.len().eq(&fields2.len())
                    && names.all(|name| match (fields1.get(name), fields2.get(name)) {
                        // both recs must contain all fields, and the values must be equal
                        (Some(val1), Some(val2)) => val1.eq(arena, val2),
                        // any fields missing is unequal
                        _ => false,
                    })
            }

            // really not sure how to check equivalence between functions, so false for now
            _ => false,
        }
    }

    pub fn most_precise(&self, arena: &'a Arena, other: &Val<'p, 'a>) -> Val<'p, 'a> {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get =
            |fields: &[CoreRecField<&'a Val<'p, 'a>>], name: &[u8]| -> Option<&'a Val<'p, 'a>> {
                let mut out = None;
                for field in fields {
                    if field.name.eq(name) {
                        out = Some(field.data)
                    }
                }
                out
            };

        match (self, other) {
            // if either type is Any, the other type is assumed more precise
            (Val::AnyTy, ty) | (ty, Val::AnyTy) => ty.clone(),

            // if both are Rec, take the most precise version of each field
            (Val::RecTy { fields: fields1 }, Val::RecTy { fields: fields2 }) => {
                Val::RecTy {
                    fields: fields1
                        .iter()
                        .chain(fields2)
                        .map(|CoreRecField { name, .. }| name)
                        .unique()
                        .map(|name| match (get(fields1, name), get(fields2, name)) {
                            // when both Recs contain the field name, take the most precise definition
                            (Some(ty1), Some(ty2)) => CoreRecField::new(
                                name,
                                arena.alloc(ty1.most_precise(arena, &ty2)) as &Val<'p, 'a>,
                            ),
                            // this should never happen
                            _ => panic!(
                                "a record was missing a field?! both should have all fields?!"
                            ),
                        })
                        .collect::<Vec<_>>(),
                }
            }

            // all atomic combinations are equally precise (assuming they are equiv!)
            _ => self.clone(),
        }
    }
}

#[derive(Clone)]
pub enum Neutral<'p: 'a, 'a> {
    Var {
        level: usize,
    },
    FunApp {
        head: Arc<Val<'p, 'a>>,
        args: Vec<Val<'p, 'a>>,
    },
    RecProj {
        tm: Arc<Val<'p, 'a>>,
        name: String,
    },
}

pub fn app<'p: 'a, 'a>(
    arena: &'a Arena,
    location: &Location,
    head: &'a Val<'p, 'a>,
    args: Vec<&'a Val<'p, 'a>>,
) -> Result<&'a Val<'p, 'a>, EvalError> {
    match head {
        Val::Fun { data } => {
            data.app(arena, &args)

            // eval(
            //     arena,
            //     // expensive?
            //     &args.iter().fold(env.clone(), |env0, arg| env0.with(arg)),
            //     body,
            // )
        }
        Val::FunForeign { f } => f(arena, location, &args),
        _ => Err(EvalError::from_internal(
            InternalError {
                message: "invalid function application".to_string(),
            },
            location.clone(),
        )),
        Val::FunReturnTyAwaiting { data } => data.app(arena, &args),
    }
}

pub fn make_portable<'p: 'a, 'a>(arena: &'a Arena, val: &'a Val<'p, 'a>) -> PortableVal {
    match val {
        Val::Univ => PortableVal::Univ,

        Val::BoolTy => PortableVal::BoolTy,
        Val::Bool { b } => PortableVal::Bool { b: *b },

        Val::NumTy => PortableVal::NumTy,
        Val::Num { n } => PortableVal::Num { n: n.clone() },

        Val::StrTy => PortableVal::StrTy,
        Val::Str { s } => PortableVal::Str { s: s.to_vec() },

        Val::ListTy { ty } => PortableVal::ListTy {
            ty: Arc::new(make_portable(arena, ty)),
        },
        Val::List { v } => PortableVal::List {
            v: v.iter().map(|val| make_portable(arena, val)).collect(),
        },

        Val::RecTy { fields } => PortableVal::RecTy {
            fields: fields
                .iter()
                .map(|field| (field.name.to_vec(), make_portable(arena, field.data)))
                .collect(),
        },
        Val::RecWithTy { fields } => PortableVal::RecWithTy {
            fields: fields
                .iter()
                .map(|field| (field.name.to_vec(), make_portable(arena, field.data)))
                .collect(),
        },
        Val::Rec(rec) => PortableVal::Rec {
            fields: rec
                .all(arena)
                .iter()
                .map(|(name, val)| (name.to_vec(), make_portable(arena, val)))
                .collect(),
        },

        Val::FunTy { args, body } => PortableVal::FunTy {
            args: args.iter().map(|arg| make_portable(arena, arg)).collect(),
            body: Arc::new(make_portable(arena, body)),
        },
        Val::Fun { .. } => PortableVal::Fun {
            s: "#fun".to_string(),
        },
        Val::FunForeign { f } => PortableVal::Fun {
            s: "#fun-foreign".to_string(),
        },

        // not sure what to do with this.. just spit out a string?
        Val::FunReturnTyAwaiting { .. } => todo!(),
        Val::Neutral { .. } => todo!(),
        Val::AnyTy => todo!(),
    }
}

/// Represents an error INTERNAL to matchbox; expected to never appear to the user.
#[derive(Clone, Debug)]
pub struct InternalError {
    pub message: String,
}

impl InternalError {
    pub fn new(message: &str) -> InternalError {
        InternalError {
            message: message.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PortableVal {
    /// Atomic values
    Univ,
    Any,

    BoolTy,
    Bool {
        b: bool,
    },

    NumTy,
    Num {
        n: f32,
    },

    StrTy,
    Str {
        s: Vec<u8>,
    },

    /// Functions just become description strings
    FunTy {
        args: Vec<PortableVal>,
        body: Arc<PortableVal>,
    },
    Fun {
        s: String,
    },

    ListTy {
        ty: Arc<PortableVal>,
    },
    List {
        v: Vec<PortableVal>,
    },

    RecTy {
        fields: HashMap<Vec<u8>, PortableVal>,
    },
    RecWithTy {
        fields: HashMap<Vec<u8>, PortableVal>,
    },
    Rec {
        fields: HashMap<Vec<u8>, PortableVal>,
    },
}

#[derive(Clone)]
pub struct Effect {
    pub val: PortableVal,
    pub handler: PortableVal,
}

impl<'p: 'a, 'a> Display for Prog<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.stmt.fmt(f)
    }
}

impl<'p: 'a, 'a> Display for StmtData<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StmtData::Let { tm, next } => format!("push {}; {}", tm, next).fmt(f),
            StmtData::Out { tm0, tm1, next } => format!("{} |> {}; {}", tm0, tm1, next).fmt(f),
            StmtData::If { branches, next } => format!(
                "if {}; {}",
                branches.iter().map(|a| a.to_string()).join(", "),
                next
            )
            .fmt(f),
            StmtData::End => "".fmt(f),
        }
    }
}

impl<'p: 'a, 'a> Display for BranchData<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchData::Bool { tm, stmt } => format!("{} => {}", tm, stmt).fmt(f),
            BranchData::Is { tm, branches } => format!(
                "{} is {}",
                tm,
                branches.iter().map(|b| b.to_string()).join(", ")
            )
            .fmt(f),
        }
    }
}

impl<'p: 'a, 'a> Display for PatternBranchData<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("#matcher => {}", self.stmt).fmt(f)
    }
}

impl<'p: 'a, 'a> Display for TmData<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TmData::Var { index } => format!("#[{}]", index).fmt(f),
            TmData::Univ => "Univ".fmt(f),
            TmData::AnyTy => "Any".fmt(f),
            TmData::BoolTy => "Bool".fmt(f),
            TmData::BoolLit { b } => b.fmt(f),
            TmData::NumTy => "Num".fmt(f),
            TmData::NumLit { n } => n.fmt(f),
            TmData::StrTy => "Str".fmt(f),
            TmData::StrLit { s } => format!("'{}'", util::bytes_to_string(s).unwrap()).fmt(f),
            TmData::FunTy { args, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            TmData::FunLit { args, body } => format!(
                "({}) => {}",
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            TmData::FunForeignLit { args, body } => format!(
                "({}) => #foreign",
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            )
            .fmt(f),
            TmData::FunApp { head, args } => format!(
                "({})({})",
                head,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::ListLit { tms } => format!("[{}]", tms.into_iter().join(", ")).fmt(f),
            TmData::ListTy { ty } => format!("[{}]", ty).fmt(f),
            TmData::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecLit { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecProj { tm, name } => {
                format!("{}.{}", tm, util::bytes_to_string(name).unwrap()).fmt(f)
            }
        }
    }
}

impl<'p: 'a, 'a> Display for Val<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Univ => "Type".fmt(f),
            Val::AnyTy => "Any".fmt(f),
            Val::BoolTy => "Bool".fmt(f),
            Val::Bool { b } => b.fmt(f),
            Val::NumTy => "Num".fmt(f),
            Val::Num { n } => n.fmt(f),
            Val::StrTy => "Str".fmt(f),
            Val::Str { s } => util::bytes_to_string(s).unwrap().fmt(f),

            Val::ListTy { ty } => format!("[{}]", ty).fmt(f),
            Val::List { v } => format!("[{}]", v.into_iter().join(", ")).fmt(f),

            Val::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Val::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),

            Val::Rec(rec) => rec.fmt(f),
            Val::FunTy { args, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            Val::Fun { data } => format!("#func({})", data).fmt(f),
            Val::FunForeign { .. } => "#func(foreign)".fmt(f),
            Val::FunReturnTyAwaiting { data } => format!("#awaiting({})", data).fmt(f),
            Val::Neutral { neutral } => "#neutral".fmt(f),
        }
    }
}

impl<'p: 'a, 'a> Display for FunData<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("{} :: {}", self.body, self.env).fmt(f)
    }
}

impl<'a> Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("{} |> {}", self.val, self.handler).fmt(f)
    }
}

impl<'a> Display for PortableVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortableVal::Univ => "Type".fmt(f),
            PortableVal::Any => "Any".fmt(f),
            PortableVal::BoolTy => "Bool".fmt(f),
            PortableVal::Bool { b } => b.fmt(f),
            PortableVal::NumTy => "Num".fmt(f),
            PortableVal::Num { n } => n.fmt(f),
            PortableVal::StrTy => "Str".fmt(f),
            PortableVal::Str { s } => util::bytes_to_string(s).unwrap().fmt(f),
            PortableVal::ListTy { ty } => format!("[{}]", ty).fmt(f),
            PortableVal::List { v } => format!("[{}]", v.into_iter().join(", ")).fmt(f),
            PortableVal::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .map(|(name, val)| format!(
                        "{} : {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .map(|(name, val)| format!(
                        "{} : {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::Rec { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .map(|(name, val)| format!(
                        "{} = {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::FunTy { args, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            PortableVal::Fun { s } => format!("#func({})", s).fmt(f),
        }
    }
}
