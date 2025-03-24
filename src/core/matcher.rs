use bio::data_structures::qgram_index::Match;
use itertools::Itertools;
pub mod read_matcher;

use crate::{
    myers::VarMyers,
    surface::Context,
    util::{Arena, Env, Ran},
};

use super::{eval, EvalError, Val};
use std::{collections::HashMap, sync::Arc};

pub trait Matcher<'p: 'a, 'a>: Send + Sync {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError>;
}

pub struct Chain<'p: 'a, 'a> {
    pub m1: Arc<dyn Matcher<'p, 'a> + 'a>,
    pub m2: Arc<dyn Matcher<'p, 'a> + 'a>,
}
impl<'p: 'a, 'a> Matcher<'p, 'a> for Chain<'p, 'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
        let r1 = self.m1.evaluate(arena, env, val)?;
        let r2 = self.m2.evaluate(arena, env, val)?;

        todo!()
        // Ok(r1.into_iter().chain(r2).collect::<Vec<_>>())
    }
}

pub struct FieldAccess<'p: 'a, 'a> {
    pub name: String,
    pub inner: Arc<dyn Matcher<'p, 'a> + 'a>,
}
impl<'p: 'a, 'a> Matcher<'p, 'a> for FieldAccess<'p, 'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
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
impl<'p: 'a, 'a> Matcher<'p, 'a> for Succeed {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
        // one world, with no binds
        Ok(vec![vec![]])
    }
}

pub struct Bind {}
impl<'p: 'a, 'a> Matcher<'p, 'a> for Bind {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
        Ok(vec![vec![val]])
    }
}

pub struct Equal<'p: 'a, 'a> {
    pub val: &'a Val<'p, 'a>,
}

impl<'p: 'a, 'a> Equal<'p, 'a> {
    pub fn new(val: &'a Val<'p, 'a>) -> Equal<'p, 'a> {
        Equal { val }
    }
}

impl<'p: 'a, 'a> Matcher<'p, 'a> for Equal<'p, 'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
        if val.eq(arena, &self.val) {
            // one world, with no binds
            Ok(vec![vec![]])
        } else {
            Ok(vec![])
        }
    }
}
