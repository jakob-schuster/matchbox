use std::{collections::HashMap, sync::Arc};

use crate::{
    core::{
        app,
        rec::{FullyConcreteRec, Rec},
        Branch, BranchData, Effect, EvalError, FunData, InternalError, Neutral, PatternBranch,
        PatternBranchData, Prog, ProgData, Stmt, StmtData, Tm, TmData, Val,
    },
    util::{cache::Cache, env::Env, recfield::CoreRecField, Arena},
};

impl<'p> Prog<'p> {
    /// In the context of an arena, an environment of values,
    /// the cache of pre-computed values, and a single read,
    /// evaluate a program into a list of effects.
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
}

impl<'p> Stmt<'p> {
    /// In the context of an arena, a global environment of values from the standard library,
    /// the cache of pre-computed values, and a local environment,
    /// evaluate a statement into a list of effects.
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
                    Val::Effect { effect } => {
                        // export the values to be portable
                        Ok([effect]
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
}

impl<'p> Branch<'p> {
    /// In the context of an arena, a global environment of values from the standard library,
    /// the cache of pre-computed values, and a local environment,
    /// evaluate a branch into a list of effects.
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
                v @ _ => Err(EvalError::from_internal(
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
}

impl<'p> PatternBranch<'p> {
    /// In the context of an arena, a global environment of values from the standard library,
    /// the cache of pre-computed values, and a local environment,
    /// evaluate a pattern branch into a list of effects.
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
        match &self.data.matcher.evaluate(arena, env, val)?[..] {
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
}

impl<'p> Tm<'p> {
    /// In the context of an arena, a global environment of values from the standard library,
    /// the cache of pre-computed values, and a local environment,
    /// evaluate a term into a value.
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
}
