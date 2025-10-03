use bio::data_structures::qgram_index::Match;
use itertools::Itertools;
pub mod read_matcher;

use crate::{
    myers::VarMyers,
    surface::Context,
    util::{Arena, Cache, Env, Ran},
};

use super::{EvalError, Val};
use std::{collections::HashMap, fmt::Display, sync::Arc};

pub trait Matcher<'p>: Send + Sync + Display {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a;
}

pub struct Chain<'p> {
    pub m1: Arc<dyn Matcher<'p> + 'p>,
    pub m2: Arc<dyn Matcher<'p> + 'p>,
}
impl<'p> Matcher<'p> for Chain<'p> {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        let r1 = self.m1.eval(arena, global_env, cache, env, val)?;
        let r2 = self.m2.eval(arena, global_env, cache, env, val)?;

        todo!()
        // Ok(r1.into_iter().chain(r2).collect::<Vec<_>>())
    }
}

impl<'p> Display for Chain<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("chain({},{})", self.m1.to_string(), self.m2.to_string()).fmt(f)
    }
}

pub struct FieldAccess<'p> {
    pub name: String,
    pub inner: Arc<dyn Matcher<'p> + 'p>,
}
impl<'p> Matcher<'p> for FieldAccess<'p> {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        match val {
            Val::Rec { rec } => match rec.get(self.name.as_bytes()) {
                Ok(field) => self.inner.eval(arena, global_env, cache, env, &field),
                // such errors are actually OK when pattern matching,
                // just means the pattern didn't match!
                Err(_) => Ok(vec![]),
            },
            _ => Ok(vec![]),
        }
    }
}
impl<'p> Display for FieldAccess<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("field_access({},{})", self.name, self.inner).fmt(f)
    }
}

pub struct Succeed {}
impl<'p> Matcher<'p> for Succeed {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        // one world, with no binds
        Ok(vec![vec![]])
    }
}

impl Display for Succeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "succeed()".fmt(f)
    }
}

pub struct Bind {}
impl<'p> Matcher<'p> for Bind {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        Ok(vec![vec![val.clone()]])
    }
}

impl Display for Bind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "bind()".fmt(f)
    }
}

pub struct Equal<'p> {
    pub val: &'p Val<'p>,
}

impl<'p> Equal<'p> {
    pub fn new(val: &'p Val<'p>) -> Equal<'p> {
        Equal { val }
    }
}

impl<'p> Matcher<'p> for Equal<'p> {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        global_env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        if val.eq(&self.val) {
            // one world, with no binds
            Ok(vec![vec![]])
        } else {
            Ok(vec![])
        }
    }
}

impl<'p> Display for Equal<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("equal({})", self.val).fmt(f)
    }
}
