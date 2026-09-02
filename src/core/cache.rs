use std::sync::Arc;

use crate::{
    core::{
        Branch, BranchData, EvalError, PatternBranch, PatternBranchData, Prog, ProgData, Stmt,
        StmtData, Tm, TmData, Val,
    },
    util::{cache::Cache, env::Env, Arena},
};

impl<'p> Prog<'p> {
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

impl<'p> Stmt<'p> {
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

impl<'p> Branch<'p> {
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

impl<'p> PatternBranch<'p> {
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

impl<'p> Tm<'p> {
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
}
