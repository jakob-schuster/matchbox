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

pub type Prog<'p> = Located<ProgData<'p>>;
#[derive(Clone)]
pub struct ProgData<'p> {
    pub stmt: Stmt<'p>,
}

pub type Stmt<'p> = Located<StmtData<'p>>;
#[derive(Clone)]
pub enum StmtData<'p> {
    Let {
        tm: Tm<'p>,
        next: Arc<Stmt<'p>>,
    },
    Out {
        tm0: Tm<'p>,
        tm1: Tm<'p>,
        next: Arc<Stmt<'p>>,
    },
    If {
        branches: Vec<Branch<'p>>,
        next: Arc<Stmt<'p>>,
    },
    End,
}

pub type Branch<'p> = Located<BranchData<'p>>;
#[derive(Clone)]
pub enum BranchData<'p> {
    Bool {
        tm: Tm<'p>,
        stmt: Stmt<'p>,
    },

    Is {
        tm: Tm<'p>,
        branches: Vec<PatternBranch<'p>>,
    },
}

pub type PatternBranch<'p> = Located<PatternBranchData<'p>>;
#[derive(Clone)]
pub struct PatternBranchData<'p> {
    pub matcher: Arc<dyn Matcher<'p> + 'p>,
    pub stmt: Stmt<'p>,
}

impl<'p> Prog<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<&'p Val<'p>>,
        read: &'a Val<'a>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'a,
    {
        self.data.stmt.eval(arena, env, &Env::default().with(read))
    }
}

impl<'p> Stmt<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<&'p Val<'p>>,
        env: &Env<&'a Val<'a>>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            StmtData::Let { tm, next } => {
                // evaluate the term
                let val = tm.eval(arena, global_env, env)?;
                // and then evaluate the statement with that binding
                next.eval(arena, global_env, &env.with(val))
            }
            StmtData::Out { tm0, tm1, next } => {
                let val0 = tm0.eval(arena, global_env, env)?;
                let val1 = tm1.eval(arena, global_env, env)?;

                // export the values to be portable
                Ok([Effect {
                    val: make_portable(arena, val0),
                    handler: make_portable(arena, val1),
                }]
                .into_iter()
                .chain(next.eval(arena, global_env, env)?)
                .collect::<Vec<_>>())
            }
            StmtData::If { branches, next } => {
                // return the results of the first successful branch
                let get_first_branch_results = || {
                    for branch in branches {
                        if let Some(vec) = branch.eval(arena, global_env, env)? {
                            return Ok(vec);
                        }
                    }

                    Ok(vec![])
                };

                // and then chain on the rest of the results after this statement
                Ok(get_first_branch_results()?
                    .into_iter()
                    .chain(next.eval(arena, global_env, env)?)
                    .collect::<Vec<_>>())
            }
            StmtData::End => Ok(vec![]),
        }
    }
}

impl<'p> Branch<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<&'p Val<'p>>,
        env: &Env<&'a Val<'a>>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            BranchData::Bool { tm, stmt } => match tm.eval(arena, global_env, env)? {
                Val::Bool { b } => match b {
                    true => Ok(Some(stmt.eval(arena, global_env, env)?)),
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
                let val = tm.eval(arena, global_env, env)?;

                for branch in branches {
                    if let Some(vec) = branch.eval(arena, global_env, env, val)? {
                        return Ok(Some(vec));
                    }
                }

                // if no branch in the is has matched, continue on with the branches
                Ok(None)
            }
        }
    }
}

impl<'p> PatternBranch<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<&'p Val<'p>>,
        env: &Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'a,
    {
        match &self.data.matcher.evaluate(arena, env, val)?[..] {
            [] => Ok(None),
            bind_options => Ok(Some(
                bind_options
                    .iter()
                    .map(|binds| {
                        self.data.stmt.eval(
                            arena,
                            global_env,
                            &binds.iter().fold(env.clone(), |env0, bind| env0.with(bind)),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>(),
            )),
        }
    }
}

pub type Tm<'a> = Located<TmData<'a>>;
#[derive(Clone)]
pub enum TmData<'a> {
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
        ty: Arc<Tm<'a>>,
    },
    ListLit {
        tms: Vec<Tm<'a>>,
    },

    FunTy {
        args: Vec<Tm<'a>>,
        body: Arc<Tm<'a>>,
    },
    // can't remember why we need args still...
    FunLit {
        args: Vec<Tm<'a>>,
        body: Arc<Tm<'a>>,
    },
    FunForeignLit {
        args: Vec<Tm<'a>>,
        body_ty: Arc<Tm<'a>>,
        body: Arc<
            dyn for<'b> Fn(&'b Arena, &Location, &[&'b Val<'b>]) -> Result<&'b Val<'b>, EvalError>
                + Send
                + Sync,
        >,
    },
    FunApp {
        head: Arc<Tm<'a>>,
        args: Vec<Tm<'a>>,
    },

    RecTy {
        fields: Vec<CoreRecField<'a, Tm<'a>>>,
    },
    RecWithTy {
        fields: Vec<CoreRecField<'a, Tm<'a>>>,
    },
    RecLit {
        fields: Vec<CoreRecField<'a, Tm<'a>>>,
    },
    RecProj {
        tm: Arc<Tm<'a>>,
        name: &'a [u8],
    },
}

impl<'p> Tm<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<&'p Val<'p>>,
        env: &Env<&'a Val<'a>>,
    ) -> Result<&'a Val<'a>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            // look up the variable in the environment
            TmData::Var { index } => {
                Ok(if *index < env.iter().len() {
                    env.get_index(*index)
                } else {
                    // WARN clumsy and wasteful coercion
                    arena.alloc(
                        global_env
                            .get_index(*index - env.iter().len())
                            .coerce(arena),
                    )
                })
            }

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
                    .map(|arg| arg.eval(arena, global_env, env))
                    .collect::<Result<Vec<_>, EvalError>>()?,
                body: body.eval(arena, global_env, env)?,
            })),
            TmData::FunLit { args, body } => Ok(arena.alloc(Val::Fun {
                data: FunData {
                    env: env.clone(),
                    body: body.as_ref().clone(),
                },
            })),
            TmData::FunForeignLit {
                args,
                body_ty,
                body,
            } => {
                let args = args
                    .iter()
                    .map(|arg| arg.eval(arena, global_env, env))
                    .collect::<Result<Vec<_>, _>>()?;

                let smaller_env = args.iter().fold(env.clone(), |env0, _| {
                    env0.with(arena.alloc(Val::Neutral {
                        neutral: Neutral::Var {
                            level: env0.iter().len(),
                        },
                    }))
                });

                Ok(arena.alloc(Val::FunForeign {
                    args,
                    body_ty: body_ty.eval(arena, global_env, &smaller_env)?,
                    body: body.clone(),
                }))
            }
            TmData::FunApp { head, args } => app(
                arena,
                &self.location,
                head.eval(arena, global_env, env)?,
                args.iter()
                    .map(|arg| arg.eval(arena, global_env, env))
                    .collect::<Result<Vec<_>, _>>()?,
            ),

            TmData::ListTy { ty } => Ok(arena.alloc(Val::ListTy {
                ty: ty.eval(arena, global_env, env)?,
            })),
            TmData::ListLit { tms } => Ok(arena.alloc(Val::List {
                v: tms
                    .iter()
                    .map(|tm| tm.eval(arena, global_env, env))
                    .collect::<Result<Vec<_>, _>>()?,
            })),

            TmData::RecTy { fields } => Ok(arena.alloc(Val::RecTy {
                fields: fields
                    .iter()
                    .map(|field| {
                        Ok(CoreRecField::new(
                            field.name,
                            field.data.eval(arena, global_env, env)?,
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
                            field.data.eval(arena, global_env, env)?,
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
                                arena.alloc(field.data.eval(arena, global_env, env)?) as &Val<'a>,
                            ))
                        })
                        .collect::<Result<HashMap<_, _>, EvalError>>()?,
                }),
            ))),
            TmData::RecProj { tm: head_tm, name } => match head_tm.eval(arena, global_env, env)? {
                Val::Rec(r) => {
                    let e = r
                        .get(name, arena)
                        .map_err(|e| EvalError::from_internal(e, self.location.clone()))?;

                    Ok(e)
                }
                _ => Err(EvalError::from_internal(
                    InternalError {
                        message: "trying to access field of non-record value?!".to_string(),
                    },
                    self.location.clone(),
                )),
            },
        }
    }
}

/// Function type that explicitly captures its environment.
#[derive(Clone)]
pub struct FunData<'a> {
    pub env: Env<&'a Val<'a>>,
    pub body: Tm<'a>,
}

impl<'a> FunData<'a> {
    pub fn app(&self, arena: &'a Arena, args: &[&'a Val<'a>]) -> Result<&'a Val<'a>, EvalError> {
        let new_env = args
            .iter()
            .fold(self.env.clone(), |env0, arg| env0.with(arg));

        self.body.eval(arena, &Env::default(), &new_env)
    }
}

#[derive(Clone)]
pub enum Val<'a> {
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
        ty: &'a Val<'a>,
    },
    List {
        v: Vec<&'a Val<'a>>,
    },

    /// Record values; can have any backend that implements the trait.
    /// This allows us to have some Record values that merely wrap the
    /// structures provided by readers.
    RecTy {
        fields: Vec<CoreRecField<'a, &'a Val<'a>>>,
    },
    // A record type which requires certain fields;
    // other fields can also be harmlessly present
    RecWithTy {
        fields: Vec<CoreRecField<'a, &'a Val<'a>>>,
    },
    Rec(&'a dyn Rec<'a>),

    /// Function value; defunctionalised, carries the context it needs
    /// and the Rust function it will execute, which takes this carried
    /// context and any arguments.
    FunTy {
        args: Vec<&'a Val<'a>>,
        body: &'a Val<'a>,
    },
    Fun {
        data: FunData<'a>,
    },
    FunForeign {
        args: Vec<&'a Val<'a>>,
        body_ty: &'a Val<'a>,
        body: Arc<
            dyn for<'b> Fn(&'b Arena, &Location, &[&'b Val<'b>]) -> Result<&'b Val<'b>, EvalError>
                + Send
                + Sync,
        >,
    },
    /// Represents a dependent return type of a function,
    /// which can only be elaborated when the function is actually applied.
    /// Should probably come back and clean all of this up conceptually.
    FunReturnTyAwaiting {
        data: FunData<'a>, // expected_ty: Arc<Val<'a>>,
    },

    Neutral {
        neutral: Neutral<'a>,
    },
}

impl<'a> Val<'a> {
    pub fn equiv(&self, other: &Val<'a>) -> bool {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<&Val<'a>>], name: &[u8]| -> Option<Val<'a>> {
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

    pub fn eq<'b, 'c>(&self, arena: &'b Arena, other: &Val<'c>) -> bool {
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

    pub fn most_precise(&self, arena: &'a Arena, other: &Val<'a>) -> Val<'a> {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<&'a Val<'a>>], name: &[u8]| -> Option<&'a Val<'a>> {
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
                                arena.alloc(ty1.most_precise(arena, &ty2)) as &Val<'a>,
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

    /// A clumsy function to move a value from one arena to another,
    /// necessitated by ConcreteRec, which has its values from a static arena and then needs to produce values in the dynamic arena.
    /// Probably shockingly inefficient and could be removed with better design.
    fn coerce<'b>(&self, arena: &'b Arena) -> Val<'b>
    where
        'a: 'b,
    {
        match self {
            Val::Univ => Val::Univ,
            Val::AnyTy => Val::AnyTy,
            Val::BoolTy => Val::BoolTy,
            Val::Bool { b } => Val::Bool { b: *b },
            Val::NumTy => Val::NumTy,
            Val::Num { n } => Val::Num { n: *n },
            Val::StrTy => Val::StrTy,
            Val::Str { s } => Val::Str { s: s as &'b [u8] },
            Val::ListTy { ty } => Val::ListTy {
                ty: arena.alloc(ty.coerce(arena)),
            },
            Val::List { v } => Val::List {
                v: v.iter()
                    .map(|v| arena.alloc(v.coerce(arena)) as &Val<'b>)
                    .collect::<Vec<_>>(),
            },
            Val::RecTy { fields } => Val::RecTy {
                fields: fields
                    .iter()
                    .map(|a| {
                        CoreRecField::new(a.name, arena.alloc(a.data.coerce(arena)) as &Val<'b>)
                    })
                    .collect(),
            },
            Val::RecWithTy { fields } => Val::RecWithTy {
                fields: fields
                    .iter()
                    .map(|a| {
                        CoreRecField::new(a.name, arena.alloc(a.data.coerce(arena)) as &Val<'b>)
                    })
                    .collect(),
            },
            Val::Rec(rec) => Val::Rec(rec.coerce(arena)),
            Val::FunTy { args, body } => Val::FunTy {
                args: args
                    .iter()
                    .map(|arg| arena.alloc(arg.coerce(arena)) as &Val<'b>)
                    .collect(),
                body: arena.alloc(body.coerce(arena)),
            },

            // WARN these really need to be implemented
            Val::Fun { data } => todo!(),
            Val::FunForeign {
                body: f,
                args,
                body_ty,
            } => Val::FunForeign {
                body: f.clone(),
                args: args
                    .iter()
                    .map(|val| arena.alloc(val.coerce(arena)) as &Val<'b>)
                    .collect::<Vec<_>>(),
                body_ty: arena.alloc(body_ty.coerce(arena)),
            },
            Val::FunReturnTyAwaiting { data } => todo!(),
            Val::Neutral { neutral } => Val::Neutral {
                neutral: neutral.coerce(arena),
            },
        }
    }

    pub fn is_neutral(&self) -> bool {
        match self {
            Val::Neutral { neutral } => true,

            Val::ListTy { ty } => ty.is_neutral(),
            Val::List { v } => v.iter().any(|val| val.is_neutral()),
            Val::RecTy { fields } => fields.iter().any(|field| field.data.is_neutral()),
            Val::RecWithTy { fields } => fields.iter().any(|field| field.data.is_neutral()),
            Val::Rec(rec) => rec.is_neutral(),
            Val::FunTy { args, body } => {
                args.iter().any(|arg| arg.is_neutral()) || body.is_neutral()
            }
            Val::FunForeign {
                args,
                body_ty,
                body,
            } => args.iter().any(|arg| arg.is_neutral()) || body_ty.is_neutral(),
            _ => false,
        }
    }
}

#[derive(Clone)]
pub enum Neutral<'a> {
    Var {
        level: usize,
    },
    FunApp {
        head: Arc<Val<'a>>,
        args: Vec<Val<'a>>,
    },
    RecProj {
        tm: Arc<Val<'a>>,
        name: String,
    },
}

impl<'a> Neutral<'a> {
    fn coerce<'b>(&self, arena: &'b Arena) -> Neutral<'b>
    where
        'a: 'b,
    {
        match self {
            Neutral::Var { level } => Neutral::Var { level: *level },
            Neutral::FunApp { head, args } => Neutral::FunApp {
                head: Arc::new(head.coerce(arena)),
                args: args.iter().map(|arg| arg.coerce(arena)).collect(),
            },
            Neutral::RecProj { tm, name } => Neutral::RecProj {
                tm: Arc::new(tm.coerce(arena)),
                name: name.clone(),
            },
        }
    }
}

pub fn app<'a>(
    arena: &'a Arena,
    location: &Location,
    head: &'a Val<'a>,
    args: Vec<&'a Val<'a>>,
) -> Result<&'a Val<'a>, EvalError> {
    fn is_neutral(v: &Val) -> bool {
        matches!(v, Val::Neutral { .. })
    }

    // catch if anything's neutral
    if is_neutral(head) || args.iter().any(|arg| is_neutral(arg)) {
        Ok(arena.alloc(Val::Neutral {
            neutral: Neutral::FunApp {
                head: Arc::new(head.clone()),
                args: args.into_iter().cloned().collect(),
            },
        }) as &Val<'a>)
    } else {
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
            Val::FunForeign { body: f, .. } => f(arena, location, &args),
            Val::FunReturnTyAwaiting { data } => data.app(arena, &args),
            _ => Err(EvalError::from_internal(
                InternalError {
                    message: format!("trying to apply '{}' as a function?!", head),
                },
                location.clone(),
            )),
        }
    }
}

pub fn make_portable<'a>(arena: &'a Arena, val: &'a Val<'a>) -> PortableVal {
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
        Val::FunForeign { body: f, .. } => PortableVal::Fun {
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

impl<'a> Display for Prog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.stmt.fmt(f)
    }
}

impl<'a> Display for StmtData<'a> {
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

impl<'a> Display for BranchData<'a> {
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

impl<'a> Display for PatternBranchData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("#matcher => {}", self.stmt).fmt(f)
    }
}

impl<'a> Display for TmData<'a> {
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
            TmData::FunForeignLit {
                args,
                body,
                body_ty,
            } => format!(
                "({}): {} => #foreign",
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                body_ty
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

impl<'a> std::fmt::Debug for Val<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.to_string(), f)
    }
}

impl<'a> Display for Val<'a> {
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
            Val::Neutral { neutral } => format!("#neutral({})", neutral).fmt(f),
        }
    }
}

impl<'a> Display for Neutral<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Neutral::Var { level } => format!("#[{}]", level).fmt(f),
            Neutral::FunApp { head, args } => format!(
                "({})({})",
                head,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecProj { tm, name } => format!("{}.{}", tm, name).fmt(f),
        }
    }
}

impl<'a> Display for FunData<'a> {
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
