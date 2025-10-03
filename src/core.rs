use std::{collections::HashMap, fmt::Display, sync::Arc};

use bio::stats::probs;
use itertools::Itertools;
use matcher::Matcher;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use rec::{ConcreteRec, FastaRead, FullyConcreteRec, Rec};

use crate::util::{
    self, bytes_to_string, Arena, Cache, CoreRecField, Env, Located, Location, RecField,
};

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

impl<'p> Prog<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        read: Val<'a>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'a,
    {
        self.data
            .stmt
            .eval(arena, env, cache, &Env::default().with(read.clone()))
    }

    pub fn cache<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
    ) -> Result<(Prog<'p>, Cache<Val<'a>>), EvalError>
    where
        'p: 'a,
    {
        // start with an empty cache and local env
        let (stmt, cache) =
            self.data
                .stmt
                .cache(arena, global_env, &Cache::default(), &Env::default())?;

        Ok((Prog::new(self.location.clone(), ProgData { stmt }), cache))
    }
}

pub type Stmt<'p> = Located<StmtData<'p>>;
#[derive(Clone)]
pub enum StmtData<'p> {
    Let {
        tm: Tm<'p>,
        next: Arc<Stmt<'p>>,
    },
    Tm {
        tm: Tm<'p>,
        next: Arc<Stmt<'p>>,
    },
    If {
        branches: Vec<Branch<'p>>,
        next: Arc<Stmt<'p>>,
    },
    End,
}

impl<'p> Stmt<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            StmtData::Let { tm, next } => {
                // evaluate the term
                let val = tm.eval(arena, global_env, cache, env)?;
                // and then evaluate the statement with that binding
                next.eval(arena, global_env, cache, &env.with(val))
            }
            StmtData::Tm { tm, next } => {
                let val = tm.eval(arena, global_env, cache, env)?;

                match val {
                    Val::Effect { val, handler } => {
                        // export the values to be portable
                        Ok([Effect {
                            val: val.clone(),
                            handler: handler.clone(),
                        }]
                        .into_iter()
                        .chain(next.eval(arena, global_env, cache, env)?)
                        .collect::<Vec<_>>())
                    }
                    _ => panic!("type error in statement-level effect, found {}?!", val),
                }
            }
            StmtData::If { branches, next } => {
                // return the results of the first successful branch
                let get_first_branch_results = || {
                    for branch in branches {
                        if let Some(vec) = branch.eval(arena, global_env, cache, env)? {
                            return Ok(vec);
                        }
                    }

                    Ok(vec![])
                };

                // and then chain on the rest of the results after this statement
                Ok(get_first_branch_results()?
                    .into_iter()
                    .chain(next.eval(arena, global_env, cache, env)?)
                    .collect::<Vec<_>>())
            }
            StmtData::End => Ok(vec![]),
        }
    }

    fn cache<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'a>>,
        env: &Env<Val<'a>>,
    ) -> Result<(Stmt<'p>, Cache<Val<'a>>), EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            StmtData::Let { tm, next } => {
                // cache the term
                let (tm_cached, cache) = tm.cache(arena, global_env, cache, env)?;

                // evaluate the term (with no cache, as this is before caching anyway)
                let (stmt, cache) = match tm.eval(arena, global_env, &Cache::default(), env) {
                    // then evaluate the statement with that binding
                    Ok(val) => next.cache(arena, global_env, &cache, &env.with(val))?,
                    // otherwise, evaluate the statement with no new binding
                    _ => next.cache(arena, global_env, &cache, env)?,
                };

                Ok((
                    Stmt::new(
                        self.location.clone(),
                        StmtData::Let {
                            tm: tm_cached,
                            next: Arc::new(stmt),
                        },
                    ),
                    cache,
                ))
            }
            StmtData::Tm { tm, next } => {
                let (tm, cache) = tm.cache(arena, global_env, cache, env)?;
                let (stmt, cache) = next.cache(arena, global_env, &cache, env)?;

                Ok((
                    Stmt::new(
                        self.location.clone(),
                        StmtData::Tm {
                            tm,
                            next: Arc::new(stmt),
                        },
                    ),
                    cache,
                ))
            }
            StmtData::If { branches, next } => {
                let (branches, cache) = branches.iter().try_fold(
                    (vec![], cache.clone()),
                    |(branches0, cache0), branch| {
                        let (branch, cache) = branch.cache(arena, global_env, &cache0, env)?;

                        Ok((
                            branches0
                                .into_iter()
                                .chain([branch].iter().cloned())
                                .collect::<Vec<_>>(),
                            cache,
                        ))
                    },
                )?;

                let (stmt, cache) = next.cache(arena, global_env, &cache, env)?;

                Ok((
                    Stmt::new(
                        self.location.clone(),
                        StmtData::If {
                            branches,
                            next: Arc::new(stmt),
                        },
                    ),
                    cache,
                ))
            }
            StmtData::End => Ok((
                Stmt::new(self.location.clone(), StmtData::End),
                cache.clone(),
            )),
        }
    }
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

impl<'p> Branch<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            BranchData::Bool { tm, stmt } => match tm.eval(arena, global_env, cache, env)? {
                Val::Bool { b } => match b {
                    true => Ok(Some(stmt.eval(arena, global_env, cache, env)?)),
                    false => Ok(None),
                },
                v => Err(EvalError::from_internal(
                    InternalError {
                        message: format!("expected bool in branch, found {}?!", v),
                    },
                    tm.location.clone(),
                )),
            },
            BranchData::Is { tm, branches } => {
                let val = tm.eval(arena, global_env, cache, env)?;

                for branch in branches {
                    if let Some(vec) = branch.eval(arena, global_env, cache, env, &val)? {
                        return Ok(Some(vec));
                    }
                }

                // if no branch in the is has matched, continue on with the branches
                Ok(None)
            }
        }
    }

    fn cache<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'a>>,
        env: &Env<Val<'a>>,
    ) -> Result<(Branch<'p>, Cache<Val<'a>>), EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            BranchData::Bool { tm, stmt } => {
                let (tm, cache) = tm.cache(arena, global_env, cache, env)?;
                let (stmt, cache) = stmt.cache(arena, global_env, &cache, env)?;

                Ok((
                    Branch::new(self.location.clone(), BranchData::Bool { tm, stmt }),
                    cache,
                ))
            }
            BranchData::Is { tm, branches } => {
                let (tm, cache) = tm.cache(arena, global_env, cache, env)?;
                let (branches, cache) = branches.iter().try_fold(
                    (vec![], cache.clone()),
                    |(branches0, cache0), branch| {
                        let (branch, cache) = branch.cache(arena, global_env, &cache0, env)?;

                        Ok((
                            branches0
                                .into_iter()
                                .chain([branch].iter().cloned())
                                .collect::<Vec<_>>(),
                            cache,
                        ))
                    },
                )?;

                Ok((
                    Branch::new(self.location.clone(), BranchData::Is { tm, branches }),
                    cache,
                ))
            }
        }
    }
}

pub type PatternBranch<'p> = Located<PatternBranchData<'p>>;
#[derive(Clone)]
pub struct PatternBranchData<'p> {
    pub matcher: Arc<dyn Matcher<'p> + 'p>,
    pub stmt: Stmt<'p>,
}

impl<'p> PatternBranch<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'a,
    {
        match &self.data.matcher.eval(arena, global_env, cache, env, val)?[..] {
            [] => Ok(None),
            bind_options => Ok(Some(
                bind_options
                    .into_iter()
                    .map(|binds| {
                        self.data.stmt.eval(
                            arena,
                            global_env,
                            cache,
                            &binds
                                .iter()
                                .fold(env.clone(), |env0, bind| env0.with(bind.clone())),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>(),
            )),
        }
    }

    fn cache<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'a>>,
        env: &Env<Val<'a>>,
    ) -> Result<(PatternBranch<'p>, Cache<Val<'a>>), EvalError>
    where
        'p: 'a,
    {
        // matcher doesn't need to be cached, since it carries all its values
        // and expects them to be statically evaluable

        // WARN for now, don't go deeper - anything inside will not be cached
        return Ok((self.clone(), cache.clone()));

        let (stmt, cache) = self.data.stmt.cache(arena, global_env, cache, env)?;

        Ok((
            PatternBranch::new(
                self.location.clone(),
                PatternBranchData {
                    matcher: self.data.matcher.clone(),
                    stmt,
                },
            ),
            cache,
        ))
    }
}

pub type Tm<'a> = Located<TmData<'a>>;
#[derive(Clone)]
pub enum TmData<'a> {
    Cached {
        index: usize,
    },
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
        opts: Vec<(&'a [u8], Tm<'a>, Tm<'a>)>,
        body: Arc<Tm<'a>>,
    },
    // can't remember why we need args still...
    FunLit {
        body: Arc<Tm<'a>>,
    },
    FunForeignLit {
        body: Arc<
            dyn for<'b> Fn(&'b Arena, &Location, &[Val<'b>]) -> Result<Val<'b>, EvalError>
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

    // the return type of effectful functions
    EffectTy,
}

#[derive(Clone)]
struct FunSig<'a> {
    args: Vec<Tm<'a>>,
    opts: Vec<(&'a [u8], Tm<'a>, Tm<'a>)>,
    body: Arc<Tm<'a>>,
}

impl<'p> Tm<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
    ) -> Result<Val<'a>, EvalError>
    where
        'p: 'a,
    {
        match &self.data {
            // WARN clumsy and wasteful coercion
            TmData::Cached { index } => Ok(cache.get(*index).coerce()),

            // look up the variable in the environment
            TmData::Var { index } => {
                Ok(if *index < env.iter().len() {
                    (*env.get_index(*index)).clone()
                } else {
                    // WARN clumsy and wasteful coercion
                    global_env.get_index(*index - env.iter().len()).coerce()
                })
            }

            TmData::Univ => Ok(Val::Univ),
            TmData::AnyTy => Ok(Val::AnyTy),

            TmData::BoolTy => Ok(Val::BoolTy),
            TmData::BoolLit { b } => Ok(Val::Bool { b: *b }),
            TmData::NumTy => Ok(Val::NumTy),
            TmData::NumLit { n } => Ok(Val::Num { n: *n }),
            TmData::StrTy => Ok(Val::StrTy),
            TmData::StrLit { s } => Ok(Val::Str { s }),

            TmData::FunTy { args, opts, body } => {
                let args = args
                    .iter()
                    .map(|arg| arg.eval(arena, global_env, cache, env))
                    .collect::<Result<Vec<_>, EvalError>>()?;

                let opts = opts
                    .iter()
                    .map(|(name, ty, val)| {
                        Ok((
                            *name,
                            ty.eval(arena, global_env, cache, env)?,
                            // val.eval(arena, global_env, cache, env)?,
                            val.clone(),
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let body = body.eval(arena, global_env, cache, env)?;

                if args.iter().any(|arg| arg.is_neutral())
                    || opts.iter().any(|(_, ty, val)| ty.is_neutral())
                    || body.is_neutral()
                {
                    Ok(Val::Neutral {
                        neutral: Neutral::FunTy {
                            args,
                            opts,
                            body: Arc::new(body),
                        },
                    })
                } else {
                    Ok(Val::FunTy {
                        args,
                        opts,
                        body: Arc::new(body),
                    })
                }
            }
            // WARN we aren't giving an option for this to be neutral yet. is this a problem?
            TmData::FunLit { body } => Ok(Val::Fun {
                data: FunData {
                    env: env.clone() as Env<Val<'a>>,
                    body: body.coerce(),
                },
            }),
            TmData::FunForeignLit {
                // args,
                // body_ty,
                body,
            } => {
                // let args = args
                //     .iter()
                //     .map(|arg| arg.eval(arena, global_env, cache, env))
                //     .collect::<Result<Vec<_>, _>>()?;

                // let smaller_env = args.iter().fold(env.clone(), |env0, _| {
                //     env0.with(Val::Neutral {
                //         neutral: Neutral::Var {
                //             level: env0.iter().len(),
                //         },
                //     })
                // });

                // let body_ty = body_ty.eval(arena, global_env, cache, &smaller_env)?;

                // we DON'T care if the body type is neutral
                // if args.iter().any(|arg| arg.is_neutral()) {
                //     Ok(Val::Neutral {
                //         neutral: Neutral::FunForeignLit {
                //             // args,
                //             // body_ty: Arc::new(body_ty),
                //             body: body.clone(),
                //         },
                //     })
                // } else {
                Ok(Val::FunForeign {
                    // args,
                    // body_ty: Arc::new(body_ty),
                    body: body.clone(),
                })
                // }
            }
            TmData::FunApp { head, args } => app(
                arena,
                &self.location,
                head.eval(arena, global_env, cache, env)?,
                args.iter()
                    .map(|arg| arg.eval(arena, global_env, cache, env))
                    .collect::<Result<Vec<_>, _>>()?,
            ),

            TmData::ListTy { ty } => {
                let val = ty.eval(arena, global_env, cache, env)?;
                if val.is_neutral() {
                    Ok(Val::Neutral {
                        neutral: Neutral::ListTy { ty: Arc::new(val) },
                    })
                } else {
                    Ok(Val::ListTy {
                        ty: Arc::new(ty.eval(arena, global_env, cache, env)?),
                    })
                }
            }
            TmData::ListLit { tms } => {
                let v = tms
                    .iter()
                    .map(|tm| tm.eval(arena, global_env, cache, env))
                    .collect::<Result<Vec<_>, _>>()?;

                if v.iter().any(|val| val.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::ListLit { tms: v },
                    })
                } else {
                    Ok(Val::List { v })
                }
            }

            TmData::RecTy { fields } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(CoreRecField::new(
                            field.name,
                            field.data.eval(arena, global_env, cache, env)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, EvalError>>()?;
                if fields.iter().any(|field| field.data.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::RecTy { fields },
                    })
                } else {
                    Ok(Val::RecTy { fields })
                }
            }
            TmData::RecWithTy { fields } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(CoreRecField::new(
                            field.name,
                            field.data.eval(arena, global_env, cache, env)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, EvalError>>()?;
                if fields.iter().any(|field| field.data.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::RecWithTy { fields },
                    })
                } else {
                    Ok(Val::RecWithTy { fields })
                }
            }

            // allocate a RecLit as a ConcreteRec
            TmData::RecLit { fields } => {
                let new_fields = fields
                    .iter()
                    .map(|field| {
                        Ok((
                            field.name.to_vec(),
                            field.data.eval(arena, global_env, cache, env)? as Val<'a>,
                        ))
                    })
                    .collect::<Result<HashMap<_, _>, EvalError>>()?;

                if new_fields.iter().any(|(_, val)| val.is_neutral()) {
                    // if any value is neutral, the whole record is neutral
                    Ok(Val::Neutral {
                        neutral: Neutral::RecLit {
                            fields: fields
                                .iter()
                                .map(|field| {
                                    Ok(CoreRecField::new(
                                        field.name,
                                        field.data.eval(arena, global_env, cache, env)? as Val<'a>,
                                    ))
                                })
                                .collect::<Result<Vec<_>, EvalError>>()?,
                        },
                    })
                } else {
                    Ok(Val::Rec {
                        rec: Arc::new(FullyConcreteRec { map: new_fields })
                            as Arc<dyn Rec<'a> + 'a>,
                    })
                }
            }
            TmData::RecProj { tm: head_tm, name } => {
                match head_tm.eval(arena, global_env, cache, env)? {
                    Val::Rec { rec: r } => {
                        let e = r
                            .get(name)
                            .map_err(|e| EvalError::from_internal(e, self.location.clone()))?
                            .clone();

                        Ok(e)
                    }
                    Val::Neutral { neutral } => Ok(Val::Neutral {
                        neutral: Neutral::RecProj {
                            tm: Arc::new(Val::Neutral {
                                neutral: neutral.clone(),
                            }),
                            name: *name,
                        },
                    }),
                    val @ _ => Err(EvalError::from_internal(
                        InternalError {
                            message: format!(
                                "trying to access field of non-record value {}?!",
                                val
                            ),
                        },
                        self.location.clone(),
                    )),
                }
            }

            TmData::EffectTy => Ok(Val::EffectTy),
        }
    }

    fn cache<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'a>>,
        env: &Env<Val<'a>>,
    ) -> Result<(Tm<'p>, Cache<Val<'a>>), EvalError>
    where
        'p: 'a,
    {
        // this is still pre-caching
        if let Ok(val) = self.eval(arena, global_env, &Cache::default(), env) {
            match val {
                // for now, shallow caching
                Val::Neutral { .. } => Ok((self.clone(), cache.clone())),

                // otherwise, cache the value and change the tm
                // (this is the ENTIRE bit of program logic for this caching operation.
                // could we do all of this with a fold somehow?)
                _ => {
                    let (cache, index) = cache.push(val);

                    Ok((
                        Tm::new(self.location.clone(), TmData::Cached { index }),
                        cache,
                    ))
                }
            }
        } else {
            // if the evaluation had an error, don't worry about caching
            Ok((self.clone(), cache.clone()))
        }
    }

    fn coerce<'a>(&self) -> Tm<'a>
    where
        'p: 'a,
    {
        let tm_data = match &self.data {
            TmData::Cached { index } => TmData::Cached { index: *index },
            TmData::Var { index } => TmData::Var { index: *index },
            TmData::Univ => TmData::Univ,
            TmData::AnyTy => TmData::AnyTy,
            TmData::BoolTy => TmData::BoolTy,
            TmData::BoolLit { b } => TmData::BoolLit { b: *b },
            TmData::NumTy => TmData::NumTy,
            TmData::NumLit { n } => TmData::NumLit { n: *n },
            TmData::StrTy => TmData::StrTy,
            TmData::StrLit { s } => TmData::StrLit { s },
            TmData::ListTy { ty } => TmData::ListTy {
                ty: Arc::new(ty.coerce()),
            },
            TmData::ListLit { tms } => TmData::ListLit {
                tms: tms.iter().map(|tm| tm.coerce()).collect(),
            },
            TmData::FunTy { args, opts, body } => TmData::FunTy {
                args: args.iter().map(|tm| tm.coerce()).collect(),
                opts: opts
                    .iter()
                    .map(|(name, ty, val)| (*name as &'a [u8], ty.coerce(), val.coerce()))
                    .collect(),
                body: Arc::new(body.coerce()),
            },
            TmData::FunLit { body } => TmData::FunLit {
                body: Arc::new(body.coerce()),
            },
            TmData::FunForeignLit { body } => TmData::FunForeignLit { body: body.clone() },
            TmData::FunApp { head, args } => TmData::FunApp {
                head: Arc::new(head.coerce()),
                args: args.iter().map(|arg| arg.coerce()).collect(),
            },
            TmData::RecTy { fields } => TmData::RecTy {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
            TmData::RecWithTy { fields } => TmData::RecWithTy {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
            TmData::RecLit { fields } => TmData::RecLit {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
            TmData::RecProj { tm, name } => TmData::RecProj {
                tm: Arc::new(tm.coerce()),
                name,
            },
            TmData::EffectTy => TmData::EffectTy,
        };

        Tm::new(self.location.clone(), tm_data)
    }
}

/// Function type that explicitly captures its environment.
#[derive(Clone)]
pub struct FunData<'a> {
    pub env: Env<Val<'a>>,
    pub body: Tm<'a>,
}

impl<'a> FunData<'a> {
    pub fn app(&self, arena: &'a Arena, args: Vec<Val<'a>>) -> Result<Val<'a>, EvalError> {
        let new_env = args
            .into_iter()
            .fold(self.env.clone(), |env0, arg| env0.with(arg));

        self.body
            .eval(arena, &Env::default(), &Cache::default(), &new_env)
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
        ty: Arc<Val<'a>>,
    },
    List {
        v: Vec<Val<'a>>,
    },

    /// Record values; can have any backend that implements the trait.
    /// This allows us to have some Record values that merely wrap the
    /// structures provided by readers.
    RecTy {
        fields: Vec<CoreRecField<'a, Val<'a>>>,
    },
    // A record type which requires certain fields;
    // other fields can also be harmlessly present
    RecWithTy {
        fields: Vec<CoreRecField<'a, Val<'a>>>,
    },
    Rec {
        rec: Arc<dyn Rec<'a> + 'a>,
    },

    /// Function value; defunctionalised, carries the context it needs
    /// and the Rust function it will execute, which takes this carried
    /// context and any arguments.
    FunTy {
        args: Vec<Val<'a>>,
        opts: Vec<(&'a [u8], Val<'a>, Tm<'a>)>,
        body: Arc<Val<'a>>,
    },
    Fun {
        data: FunData<'a>,
    },
    FunForeign {
        body: Arc<
            dyn for<'b> Fn(&'b Arena, &Location, &[Val<'b>]) -> Result<Val<'b>, EvalError>
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

    EffectTy,
    // by the time it's an effect, the values have already been made portable
    Effect {
        val: PortableVal,
        handler: PortableVal,
    },
}

struct OptionalArgs<'a> {
    v: Vec<(&'a [u8], Val<'a>)>,
}

impl<'a> OptionalArgs<'a> {
    fn get(&self, name: &[u8]) -> Result<&Val<'a>, InternalError> {
        for (arg_name, arg_val) in &self.v {
            if (*arg_name).eq(name) {
                return Ok(arg_val);
            }
        }

        Err(InternalError::new(&format!(
            "couldn't find argument '{}' in function type!?",
            String::from_utf8(name.to_vec()).unwrap()
        )))
    }
}

impl<'a> Val<'a> {
    pub fn equiv(&self, other: &Val<'a>) -> bool {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<Val<'a>>], name: &[u8]| -> Option<Val<'a>> {
            let mut out = None;
            for field in fields {
                if field.name.eq(name) {
                    out = Some(field.data.clone())
                }
            }
            out
        };
        let get_opt =
            |fields: &[(&[u8], Val<'a>, Tm<'a>)], name: &[u8]| -> Option<(Val<'a>, Tm<'a>)> {
                let mut out = None;
                for (field_name, ty, val) in fields {
                    if field_name.eq(&name) {
                        out = Some((ty.clone(), val.clone()))
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
            | (Val::StrTy, Val::StrTy)
            | (Val::EffectTy, Val::EffectTy) => true,

            // list types must have equivalent inner types
            (Val::ListTy { ty: ty1 }, Val::ListTy { ty: ty2 }) => ty1.equiv(ty2),

            // equivalent if argument types are the same
            // and return types are the same
            (
                Val::FunTy {
                    args: args1,
                    opts: opts1,
                    body: body1,
                },
                Val::FunTy {
                    args: args2,
                    opts: opts2,
                    body: body2,
                },
            ) => {
                let mut names = opts1
                    .iter()
                    .chain(opts2)
                    .map(|(name, _, _)| name.clone())
                    .unique();

                args1.len().eq(&args2.len())
                    && args1.iter().zip(args2).all(|(arg1, arg2)| arg1.equiv(arg2))
                    && names.all(|name| match (get_opt(opts1, name), get_opt(opts2, name)) {
                        // WARN for now do not verify that the values themselves are the same
                        // just check the types and the names of all the fields
                        (Some((ty1, val1)), Some((ty2, val2))) => ty1.equiv(&ty2),
                        // any optional arguments missing is a type error
                        _ => false,
                    })
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
                let with_names = with_fields.iter().map(|CoreRecField { name, .. }| name);

                // all the names in the RecWith should be present in the Rec
                with_names.clone().all(|name| names.clone().contains(name))
                    && fields
                        .iter()
                        .all(|field| match get(with_fields, field.name) {
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

    pub fn eq<'b, 'c>(&self, other: &Val<'c>) -> bool {
        match (self, other) {
            (Val::Univ, Val::Univ)
            | (Val::AnyTy, Val::AnyTy)
            | (Val::BoolTy, Val::BoolTy)
            | (Val::NumTy, Val::NumTy)
            | (Val::StrTy, Val::StrTy)
            | (Val::EffectTy, Val::EffectTy) => true,

            (Val::Bool { b: b1 }, Val::Bool { b: b2 }) => b1.eq(b2),
            (Val::Num { n: n1 }, Val::Num { n: n2 }) => n1.eq(n2),
            (Val::Str { s: s1 }, Val::Str { s: s2 }) => s1.eq(s2),

            // check that all the fields of r1 and all the fields of r2 are the same
            (Val::Rec { rec: r1 }, Val::Rec { rec: r2 }) => {
                let fields1 = r1.all();
                let fields2 = r2.all();
                let mut names = fields1.iter().chain(fields2.iter()).map(|(name, _)| name);

                fields1.len().eq(&fields2.len())
                    && names.all(|name| match (fields1.get(name), fields2.get(name)) {
                        // both recs must contain all fields, and the values must be equal
                        (Some(val1), Some(val2)) => val1.eq(val2),
                        // any fields missing is unequal
                        _ => false,
                    })
            }

            (
                Val::Effect {
                    val: val1,
                    handler: handler1,
                },
                Val::Effect {
                    val: val2,
                    handler: handler2,
                },
            ) => val1.eq(val2) && handler1.eq(handler2),

            // really not sure how to check equivalence between functions, so false for now
            _ => false,
        }
    }

    pub fn most_precise(&self, arena: &'a Arena, other: &Val<'a>) -> Val<'a> {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<Val<'a>>], name: &[u8]| -> Option<Val<'a>> {
            let mut out = None;
            for field in fields {
                if field.name.eq(name) {
                    out = Some(field.data.clone())
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
                            (Some(ty1), Some(ty2)) => {
                                CoreRecField::new(name, ty1.most_precise(arena, &ty2))
                            }

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
    pub fn coerce<'b>(&self) -> Val<'b>
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
                ty: Arc::new(ty.coerce()),
            },
            Val::List { v } => Val::List {
                v: v.iter().map(|v| v.coerce() as Val<'b>).collect::<Vec<_>>(),
            },
            Val::RecTy { fields } => Val::RecTy {
                fields: fields
                    .iter()
                    .map(|a| CoreRecField::new(a.name, a.data.coerce() as Val<'b>))
                    .collect(),
            },
            Val::RecWithTy { fields } => Val::RecWithTy {
                fields: fields
                    .iter()
                    .map(|a| CoreRecField::new(a.name, a.data.coerce() as Val<'b>))
                    .collect(),
            },
            Val::Rec { rec } => Val::Rec { rec: rec.coerce() },
            Val::FunTy { args, opts, body } => Val::FunTy {
                args: args.iter().map(|arg| arg.coerce() as Val<'b>).collect(),
                body: Arc::new(body.coerce()),
                opts: opts
                    .iter()
                    .map(|(a, b, c)| (a as &'a [u8], b.coerce(), c.coerce()))
                    .collect(),
            },
            Val::Fun { data } => Val::Fun {
                data: FunData {
                    env: Env::from_vec(data.env.iter().map(|val| val.coerce()).collect_vec()),
                    body: data.body.coerce(),
                },
            },
            Val::FunForeign { body: f } => Val::FunForeign { body: f.clone() },
            Val::FunReturnTyAwaiting { data } => todo!(),
            Val::Neutral { neutral } => Val::Neutral {
                neutral: neutral.coerce(),
            },
            Val::EffectTy => Val::EffectTy,
            Val::Effect { val, handler } => Val::Effect {
                val: val.clone(),
                handler: handler.clone(),
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
            Val::Rec { rec } => rec.is_neutral(),
            Val::FunTy { args, opts, body } => {
                args.iter().any(|arg| arg.is_neutral()) || body.is_neutral()
            }

            _ => false,
        }
    }
}

#[derive(Clone)]
pub enum Neutral<'a> {
    Var {
        level: usize,
    },
    //
    ListTy {
        ty: Arc<Val<'a>>,
    },
    ListLit {
        tms: Vec<Val<'a>>,
    },
    //
    FunTy {
        args: Vec<Val<'a>>,
        opts: Vec<(&'a [u8], Val<'a>, Tm<'a>)>,
        body: Arc<Val<'a>>,
    },
    FunLit {
        body: Arc<Val<'a>>,
    },
    FunForeignLit {
        body: Arc<
            dyn for<'b> Fn(&'b Arena, &Location, &[Val<'b>]) -> Result<Val<'b>, EvalError>
                + Send
                + Sync,
        >,
    },
    FunApp {
        head: Arc<Val<'a>>,
        args: Vec<Val<'a>>,
    },
    //
    RecTy {
        fields: Vec<CoreRecField<'a, Val<'a>>>,
    },
    RecWithTy {
        fields: Vec<CoreRecField<'a, Val<'a>>>,
    },
    RecLit {
        fields: Vec<CoreRecField<'a, Val<'a>>>,
    },
    RecProj {
        tm: Arc<Val<'a>>,
        name: &'a [u8],
    },
}

impl<'a> Neutral<'a> {
    fn coerce<'b>(&self) -> Neutral<'b>
    where
        'a: 'b,
    {
        match self {
            Neutral::Var { level } => Neutral::Var { level: *level },
            Neutral::FunApp { head, args } => Neutral::FunApp {
                head: Arc::new(head.coerce()),
                args: args.iter().map(|arg| arg.coerce()).collect(),
            },
            Neutral::RecProj { tm, name } => Neutral::RecProj {
                tm: Arc::new(tm.coerce()),
                name: name.clone(),
            },
            Neutral::ListTy { ty } => Neutral::ListTy {
                ty: Arc::new(ty.coerce()),
            },
            Neutral::ListLit { tms } => Neutral::ListLit {
                tms: tms.iter().map(|tm| tm.coerce()).collect(),
            },
            Neutral::FunTy { args, opts, body } => Neutral::FunTy {
                args: args.iter().map(|arg| arg.coerce()).collect(),
                opts: opts
                    .iter()
                    .map(|(a, b, c)| (a as &'a [u8], b.coerce(), c.coerce()))
                    .collect(),
                body: Arc::new(body.coerce()),
            },
            Neutral::FunLit { body } => Neutral::FunLit {
                body: Arc::new(body.coerce()),
            },
            Neutral::FunForeignLit { body } => Neutral::FunForeignLit { body: body.clone() },
            Neutral::RecTy { fields } => Neutral::RecTy {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
            Neutral::RecWithTy { fields } => Neutral::RecWithTy {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
            Neutral::RecLit { fields } => Neutral::RecLit {
                fields: fields
                    .iter()
                    .map(|field| CoreRecField::new(field.name, field.data.coerce()))
                    .collect(),
            },
        }
    }
}

pub fn app<'a>(
    arena: &'a Arena,
    location: &Location,
    head: Val<'a>,
    args: Vec<Val<'a>>,
) -> Result<Val<'a>, EvalError> {
    // catch if anything's neutral
    if head.is_neutral() || args.iter().any(|arg| arg.is_neutral()) {
        Ok(Val::Neutral {
            neutral: Neutral::FunApp {
                head: Arc::new(head.clone()),
                args,
            },
        } as Val<'a>)
    } else {
        match head {
            Val::Fun { data } => {
                data.app(arena, args)

                // eval(
                //     arena,
                //     // expensive?
                //     &args.iter().fold(env.clone(), |env0, arg| env0.with(arg)),
                //     body,
                // )
            }
            Val::FunForeign { body: f, .. } => f(arena, location, &args),
            Val::FunReturnTyAwaiting { data } => data.app(arena, args),
            _ => Err(EvalError::from_internal(
                InternalError {
                    message: format!("trying to apply '{}' as a function?!", head),
                },
                location.clone(),
            )),
        }
    }
}

pub fn make_portable<'a>(arena: &'a Arena, val: &Val<'a>) -> PortableVal {
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
                .map(|field| (field.name.to_vec(), make_portable(arena, &field.data)))
                .collect(),
        },
        Val::RecWithTy { fields } => PortableVal::RecWithTy {
            fields: fields
                .iter()
                .map(|field| (field.name.to_vec(), make_portable(arena, &field.data)))
                .collect(),
        },
        Val::Rec { rec } => PortableVal::Rec {
            fields: rec
                .all()
                .iter()
                .map(|(name, val)| (name.to_vec(), make_portable(arena, val)))
                .collect(),
        },

        Val::FunTy { args, opts, body } => PortableVal::FunTy {
            args: args.iter().map(|arg| make_portable(arena, arg)).collect(),
            opts: opts
                .iter()
                // WARN to make tms portable, for now just serialise them as strings
                .map(|(name, ty, tm)| (name.to_vec(), make_portable(arena, ty), tm.to_string()))
                .collect(),
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

        Val::EffectTy => PortableVal::EffectTy,
        Val::Effect { val, handler } => PortableVal::Effect {
            val: Arc::new(val.clone()),
            handler: Arc::new(handler.clone()),
        },
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
        // WARN here, the term has been serialised as a string
        // because I have no idea what else to do
        opts: Vec<(Vec<u8>, PortableVal, String)>,
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

    EffectTy,
    Effect {
        val: Arc<PortableVal>,
        handler: Arc<PortableVal>,
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
            StmtData::Tm { tm, next } => format!("{}; {}", tm, next).fmt(f),
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
        format!("#matcher({}) => {}", self.matcher, self.stmt).fmt(f)
    }
}

impl<'a> Display for TmData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TmData::Cached { index } => format!("#cached[{}]", index).fmt(f),
            TmData::Var { index } => format!("#[{}]", index).fmt(f),
            TmData::Univ => "Univ".fmt(f),
            TmData::AnyTy => "Any".fmt(f),
            TmData::BoolTy => "Bool".fmt(f),
            TmData::BoolLit { b } => b.fmt(f),
            TmData::NumTy => "Num".fmt(f),
            TmData::NumLit { n } => n.fmt(f),
            TmData::StrTy => "Str".fmt(f),
            TmData::StrLit { s } => format!("'{}'", util::bytes_to_string(s).unwrap()).fmt(f),
            TmData::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|a| a.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            TmData::FunLit { body } => format!("#fun({})", body).fmt(f),
            TmData::FunForeignLit { body } => "#fun-foreign".fmt(f),
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
            TmData::EffectTy => "Effect".fmt(f),
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
                    .sorted_by_key(|field| field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Val::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|field| field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Val::Rec { rec } => rec.fmt(f),
            Val::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            Val::Fun { data } => format!("#fun({})", data).fmt(f),
            Val::FunForeign { body } => format!("#fun-foreign",).fmt(f),
            Val::FunReturnTyAwaiting { data } => format!("#awaiting({})", data).fmt(f),
            Val::Neutral { neutral } => format!("#neutral({})", neutral).fmt(f),
            Val::EffectTy => "Effect".fmt(f),
            Val::Effect { val, handler } => format!("#effect({}, {})", val, handler).fmt(f),
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
            Neutral::RecProj { tm, name } => {
                format!("{}.{}", tm, bytes_to_string(name).unwrap()).fmt(f)
            }
            Neutral::ListTy { ty } => format!("listty[{}]", ty).fmt(f),
            Neutral::ListLit { tms } => format!("[{}]", tms.into_iter().join(", ")).fmt(f),
            Neutral::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|field| field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecLit { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),

            Neutral::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|a| a.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            Neutral::FunLit { body } => format!("#fun({})", body).fmt(f),
            Neutral::FunForeignLit { body } => format!("#fun-foreign",).fmt(f),
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
                    .sorted_by_key(|(name, _)| *name)
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
                    .sorted_by_key(|(name, _)| *name)
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
                    .sorted_by_key(|(name, _)| *name)
                    .map(|(name, val)| format!(
                        "{} = {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            PortableVal::Fun { s } => format!("#func({})", s).fmt(f),
            PortableVal::EffectTy => "Effect".fmt(f),
            PortableVal::Effect { val, handler } => format!("#effect({}, {})", val, handler).fmt(f),
        }
    }
}
