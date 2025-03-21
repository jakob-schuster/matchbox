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

pub trait Matcher<'a>: Send + Sync {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError>;
}

pub struct Chain<'a> {
    pub m1: Arc<dyn Matcher<'a> + 'a>,
    pub m2: Arc<dyn Matcher<'a> + 'a>,
}
impl<'a> Matcher<'a> for Chain<'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError> {
        let r1 = self.m1.evaluate(arena, env, val)?;
        let r2 = self.m2.evaluate(arena, env, val)?;

        todo!()
        // Ok(r1.into_iter().chain(r2).collect::<Vec<_>>())
    }
}

pub struct FieldAccess<'a> {
    pub name: String,
    pub inner: Arc<dyn Matcher<'a> + 'a>,
}
impl<'a> Matcher<'a> for FieldAccess<'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError> {
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
impl<'a> Matcher<'a> for Succeed {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError> {
        // one world, with no binds
        Ok(vec![vec![]])
    }
}

pub struct Bind {}
impl<'a> Matcher<'a> for Bind {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError> {
        Ok(vec![vec![val]])
    }
}

pub struct Equal<'a> {
    pub val: &'a Val<'a>,
}

impl<'a> Equal<'a> {
    pub fn new(val: &'a Val<'a>) -> Equal<'a> {
        Equal { val }
    }
}

impl<'a> Matcher<'a> for Equal<'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'a>>,
        val: &'a Val<'a>,
    ) -> Result<Vec<Vec<&'a Val<'a>>>, EvalError> {
        if val.eq(arena, &self.val) {
            // one world, with no binds
            Ok(vec![vec![]])
        } else {
            Ok(vec![])
        }
    }
}
