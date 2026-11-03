use bio::data_structures::qgram_index::Match;
use itertools::Itertools;
pub mod read_matcher2;

use crate::{
    core::eval2::env::{self, Env},
    myers::VarMyers,
    util::{location::Location, ran::Ran, Arena},
};

use super::{EvalError, Val};
use std::{collections::HashMap, fmt::Display, sync::Arc};

pub trait Matcher<'p>: Send + Sync + Display {
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a;
}

pub struct Chain<'p> {
    pub m1: Arc<dyn Matcher<'p> + 'p>,
    pub m2: Arc<dyn Matcher<'p> + 'p>,
}
impl<'p> Matcher<'p> for Chain<'p> {
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        let r1 = self.m1.eval2(global, local, val)?;
        let r2 = self.m2.eval2(global, local, val)?;

        Ok(r1.into_iter().chain(r2).collect::<Vec<_>>())
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
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        match val {
            Val::Rec { rec } => match rec.get(self.name.as_bytes()) {
                Ok(field) => self.inner.eval2(global, local, &field),
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
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        Ok(vec![Env::default()])
    }
}

impl Display for Succeed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "succeed()".fmt(f)
    }
}

pub struct Bind {}
impl<'p> Matcher<'p> for Bind {
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        Ok(vec![Env::default().with(val.clone())])
    }
}

impl Display for Bind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "bind()".fmt(f)
    }
}

pub struct Equal<'p> {
    pub val: Val<'p>,
}

impl<'p> Equal<'p> {
    pub fn new(val: &Val<'p>) -> Equal<'p> {
        Equal { val: val.clone() }
    }
}

impl<'p> Matcher<'p> for Equal<'p> {
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        if val.eq(&self.val) {
            // one world, with no binds
            Ok(vec![Env::default()])
        } else {
            // no worlds
            Ok(vec![])
        }
    }
}

impl<'p> Display for Equal<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("equal({})", self.val).fmt(f)
    }
}

pub struct ErrorMatcher {
    location: Location,
}

impl ErrorMatcher {
    fn new(location: &Location) -> ErrorMatcher {
        ErrorMatcher {
            location: location.clone(),
        }
    }
}

impl Display for ErrorMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "error".fmt(f)
    }
}

impl<'p> Matcher<'p> for ErrorMatcher {
    fn eval2<'q, 'r, 'a>(
        &'q self,
        global: &'q env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'q: 'r,
        'p: 'r,
        'r: 'a,
    {
        Err(EvalError::new(
            &self.location,
            "trying to evaluate error branch?!",
        ))
    }
}
