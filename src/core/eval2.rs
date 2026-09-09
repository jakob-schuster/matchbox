use std::{collections::HashMap, sync::Arc};

use crate::{
    core::{
        rec::{ConcreteRec, FullyConcreteRec, Rec2},
        Branch, BranchData, Effect, EvalError, FunData, InternalError, ListVal, Neutral,
        PatternBranch, Prog, Stmt, StmtData, StrVal, Tm, TmData, Val,
    },
    util::recfield::CoreRecField,
};

pub mod env {
    use std::fmt::{self, Display, Formatter};

    #[derive(Clone)]
    pub(crate) struct Env<A> {
        pub(crate) vec: Vec<A>,
    }

    impl<A: Clone + Display> Env<A> {
        /// Push an element to the top of the environment.
        /// Returns the index at which the value is found.
        pub fn push(&mut self, a: A) -> usize {
            let index = self.vec.len();
            self.vec.push(a);

            index
        }

        /// Get an item at an index in the cache.
        pub fn get<'a>(&'a self, index: usize) -> &'a A {
            self.vec.get(index).expect("Bad index in cache!")
        }
    }

    impl<A: Display> Display for Env<A> {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            format!(
                "[{}]",
                self.vec
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f)
        }
    }

    impl<A> Default for Env<A> {
        fn default() -> Self {
            Env {
                vec: Vec::default(),
            }
        }
    }
}

impl<'p> Prog<'p> {
    pub fn eval2<'r, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        read: Val<'a>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        // add the read to the local environment
        let mut local2 = local.clone();
        local2.push(read);

        self.data.stmt.eval2(global, &local2)
    }
}

impl<'p> Stmt<'p> {
    pub fn eval2<'r, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
    ) -> Result<Vec<Effect>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match &self.data {
            StmtData::Let { tm, next } => {
                // evaluate the term
                let val = tm.eval2(global, local)?;
                // bind the result in the environment
                let mut local2 = local.clone();
                local2.push(val);
                // and then evaluate the statement with that binding
                next.eval2(global, &local2)
            }
            StmtData::Tm { tm, next } => {
                let val = tm.eval2(global, local)?;

                match val {
                    Val::Effect { effect } => {
                        // export the values to be portable
                        Ok([effect]
                            .into_iter()
                            .chain(next.eval2(global, local)?)
                            .collect::<Vec<_>>())
                    }
                    _ => panic!("type error in statement-level effect, found {}?!", val),
                }
            }
            StmtData::If { branches, next } => {
                // return the results of the first successful branch
                let get_first_branch_results = || {
                    for branch in branches {
                        if let Some(vec) = branch.eval2(global, local)? {
                            return Ok(vec);
                        }
                    }

                    Ok(vec![])
                };

                // and then chain on the rest of the results after this statement
                Ok(get_first_branch_results()?
                    .into_iter()
                    .chain(next.eval2(global, local)?)
                    .collect::<Vec<_>>())
            }
            StmtData::End => Ok(vec![]),
        }
    }
}

impl<'p> Branch<'p> {
    pub fn eval2<'r, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match &self.data {
            BranchData::Bool { tm, stmt } => match tm.eval2(global, local)? {
                Val::Bool { b } => {
                    if b {
                        Ok(Some(stmt.eval2(global, local)?))
                    } else {
                        Ok(None)
                    }
                }
                v => Err(EvalError::from_internal(
                    InternalError {
                        message: format!("expected bool in branch, found {}?!", v),
                    },
                    tm.location.clone(),
                )),
            },
            BranchData::Is { tm, branches } => {
                let val = tm.eval2(global, local)?;

                for branch in branches {
                    if let Some(vec) = branch.eval2(global, local, &val)? {
                        return Ok(Some(vec));
                    }
                }

                // if no branch has matched, continue on with the branches
                Ok(None)
            }
        }
    }
}

impl<'p> PatternBranch<'p> {
    pub fn eval2<'r, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Option<Vec<Effect>>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match &self.data.matcher.eval2(global, local, val)?[..] {
            [] => Ok(None),
            bind_options => Ok(Some(
                bind_options
                    .iter()
                    .map(|binds| {
                        let mut new_local = local.clone();
                        for bind in binds {
                            new_local.push(bind.reference());
                        }

                        self.data.stmt.eval2(global, &new_local)
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>(),
            )),
        }
    }
}

impl Tm {
    pub fn eval2<'p: 'r, 'r: 'a, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
    ) -> Result<Val<'r>, EvalError> {
        match &self.data {
            TmData::Global { index } => Ok(global.get(*index).reference()),
            TmData::Local { index } => Ok(local.get(*index).clone()),
            TmData::Univ => Ok(Val::Univ),
            TmData::AnyTy => Ok(Val::AnyTy),
            TmData::BoolTy => Ok(Val::BoolTy),
            TmData::BoolLit { b } => Ok(Val::Bool { b: *b }),
            TmData::NumTy => Ok(Val::NumTy),
            TmData::NumLit { n } => Ok(Val::Num { n: *n }),
            TmData::StrTy => Ok(Val::StrTy),
            TmData::StrLit { s } => Ok(Val::Str {
                s: StrVal::Reference { s },
            }),
            TmData::ListTy { ty } => Ok(Val::ListTy {
                ty: Arc::new(ty.eval2(global, local)?),
            }),
            TmData::ListLit { tms } => Ok(Val::List {
                v: ListVal::Concrete {
                    v: tms
                        .iter()
                        .map(|tm| tm.eval2(global, local))
                        .collect::<Result<Vec<Val<'r>>, EvalError>>()?,
                },
            }),
            TmData::FunTy { args, opts, body } => {
                // first, evaluate the args
                let args = args
                    .iter()
                    .map(|arg| arg.eval2(global, local))
                    .collect::<Result<Vec<_>, EvalError>>()?;

                // then, evaluate the opts
                let opts = opts
                    .iter()
                    .map(|(name, ty, val)| Ok((&name[..], ty.eval2(global, local)?, val.clone())))
                    .collect::<Result<Vec<_>, EvalError>>()?;

                // then, evaluate the body
                let body = body.eval2(global, local)?;

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
            TmData::FunLit { body } => Ok(Val::Fun {
                data: FunData {
                    global: global.clone(),
                    local: local.clone(),
                    body: *body.clone(),
                },
            }),
            TmData::FunForeignLit { body } => Ok(Val::FunForeign { body: body.clone() }),
            TmData::FunApp { head, args } => {
                let head_val = head.eval2(global, local)?;
                let args_vals = args
                    .iter()
                    .map(|arg| arg.eval2(global, local))
                    .collect::<Result<Vec<_>, _>>()?;

                if head_val.is_neutral() || args_vals.iter().any(|arg| arg.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::FunApp {
                            head: Arc::new(head_val),
                            args: args_vals,
                        },
                    })
                } else {
                    match head_val {
                        Val::Fun { data } => data.app(args_vals),
                        Val::FunForeign { body } => body(&self.location, &args_vals),
                        Val::FunReturnTyAwaiting { data } => data.app(args_vals),
                        _ => Err(EvalError::from_internal(
                            InternalError::new(&format!(
                                "trying to apply '{}' as a function?!",
                                head_val
                            )),
                            self.location,
                        )),
                    }
                }
            }
            TmData::RecTy { fields } => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        Ok(CoreRecField::new(
                            &field.name,
                            field.data.eval2(global, local)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

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
                            &field.name,
                            field.data.eval2(global, local)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if fields.iter().any(|field| field.data.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::RecTy { fields },
                    })
                } else {
                    Ok(Val::RecWithTy { fields })
                }
            }
            TmData::RecLit { fields } => {
                let new_fields = fields
                    .iter()
                    .map(|field| Ok((&field.name[..], field.data.eval2(global, local)?)))
                    .collect::<Result<HashMap<_, _>, _>>()?;

                if new_fields.iter().any(|(_, val)| val.is_neutral()) {
                    Ok(Val::Neutral {
                        neutral: Neutral::RecLit {
                            fields: fields
                                .iter()
                                .map(|field| {
                                    Ok(CoreRecField::new(
                                        &field.name,
                                        field.data.eval2(global, local)?,
                                    ))
                                })
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    })
                } else {
                    Ok(Val::Rec {
                        rec: Rec2::Concrete {
                            r: ConcreteRec { map: new_fields },
                        },
                    })
                }
            }
            TmData::RecProj { tm, name } => match tm.eval2(global, local)? {
                Val::Rec { rec } => {
                    let val = rec
                        .get(name)
                        .map_err(|e| EvalError::from_internal(e, self.location.clone()))?
                        .clone();

                    Ok(val)
                }
                Val::Neutral { neutral } => Ok(Val::Neutral {
                    neutral: Neutral::RecProj {
                        tm: Arc::new(Val::Neutral {
                            neutral: neutral.clone(),
                        }),
                        name,
                    },
                }),
                val => Err(EvalError::from_internal(
                    InternalError::new(&format!(
                        "trying to access field of non-record value {}?!",
                        val
                    )),
                    self.location.clone(),
                )),
            },
            TmData::EffectTy => Ok(Val::EffectTy),
        }
    }
}
