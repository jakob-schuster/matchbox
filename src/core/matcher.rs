use bio::data_structures::qgram_index::Match;
use itertools::Itertools;
pub mod read_matcher;

use crate::{
    myers::VarMyers,
    surface::Context,
    util::{Arena, Env, Ran},
};

use super::{EvalError, Val};
use std::{collections::HashMap, sync::Arc};

pub trait Matcher<'p>: Send + Sync {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a;
}

pub struct Chain<'p> {
    pub m1: Arc<dyn Matcher<'p> + 'p>,
    pub m2: Arc<dyn Matcher<'p> + 'p>,
}
impl<'p> Matcher<'p> for Chain<'p> {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        let r1 = self.m1.evaluate(arena, env, val)?;
        let r2 = self.m2.evaluate(arena, env, val)?;

        todo!()
        // Ok(r1.into_iter().chain(r2).collect::<Vec<_>>())
    }
}

pub struct FieldAccess<'p> {
    pub name: String,
    pub inner: Arc<dyn Matcher<'p> + 'p>,
}
impl<'p> Matcher<'p> for FieldAccess<'p> {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        match val {
            Val::Rec(rec) => match rec.get(self.name.as_bytes(), arena) {
                Ok(field) => self.inner.evaluate(arena, env, &field),
                // such errors are actually OK when pattern matching,
                // just means the pattern didn't match!
                Err(_) => Ok(vec![]),
            },
            _ => Ok(vec![]),
        }
    }
}

pub struct Succeed {}
impl<'p> Matcher<'p> for Succeed {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        // one world, with no binds
        Ok(vec![vec![]])
    }
}

pub struct Bind {}
impl<'p> Matcher<'p> for Bind {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        Ok(vec![vec![val.clone()]])
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
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        if val.eq(arena, &self.val) {
            // one world, with no binds
            Ok(vec![vec![]])
        } else {
            Ok(vec![])
        }
    }
}
