use bio::data_structures::qgram_index::Match;
use itertools::Itertools;

use crate::{
    myers::VarMyers,
    surface::Context,
    util::{Arena, Env, Ran},
};

use super::{eval, EvalError, Val};
use std::{collections::HashMap, sync::Arc};

pub trait Matcher<'a>: Send + Sync {
    fn evaluate<'b>(
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b;
}

pub struct Chain<'a> {
    pub m1: Arc<dyn Matcher<'a> + 'a>,
    pub m2: Arc<dyn Matcher<'a> + 'a>,
}
impl<'a> Matcher<'a> for Chain<'a> {
    fn evaluate<'b>(
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
    {
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
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
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
impl<'a> Matcher<'a> for Succeed {
    fn evaluate<'b>(
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
    {
        // one world, with no binds
        Ok(vec![vec![]])
    }
}

pub struct Bind {}
impl<'a> Matcher<'a> for Bind {
    fn evaluate<'b>(
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
    {
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
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
    {
        if val.eq(arena, &self.val) {
            // one world, with no binds
            Ok(vec![vec![]])
        } else {
            Ok(vec![])
        }
    }
}

pub struct ReadMatcher<'a> {
    /// The search operations to perform
    pub ops: Vec<OpVal<'a>>,
    /// The location ranges to bind at the end in each world
    pub binds: Vec<Ran<usize>>,
    /// The location to label to the end
    pub end: usize,
}

impl<'a> Matcher<'a> for ReadMatcher<'a> {
    fn evaluate<'b>(
        &self,
        arena: &'b Arena,
        env: &'b Env<&'b Val<'b>>,
        val: &'b Val<'b>,
    ) -> Result<Vec<Vec<&'b Val<'b>>>, EvalError>
    where
        'a: 'b,
    {
        if let Val::Rec(rec) = val {
            if let Val::Str { s: seq } = rec.get(b"seq", arena).expect("") {
                // first, generate all the worlds from the operations
                let loc_ctx = LocCtx::default().with(0, 0).with(self.end, seq.len());
                let worlds =
                    self.ops
                        .iter()
                        .fold(vec![(BindCtx::default(), loc_ctx)], |worlds, op| {
                            worlds
                                .iter()
                                .flat_map(|(bind_ctx, loc_ctx)| {
                                    op.exec(arena, env, seq, &loc_ctx, bind_ctx)
                                })
                                .flatten()
                                .collect::<Vec<_>>()
                        });

                // then, make all the binds
                let envs = worlds.iter().map(|(bind_ctx, loc_ctx)| {
                    // make all the binds from bind_ctx one by one
                    let vals = bind_ctx
                        .map
                        .iter()
                        .sorted_by_key(|(id, _)| **id)
                        .map(|(_, val)| *val)
                        .chain(
                            // then make the other sequence binds
                            self.binds.iter().map(|ran| {
                                let pos_ran = ran.map(|loc| loc_ctx.get(loc));
                                let sliced = Val::Rec(arena.alloc(rec.with(
                                    b"seq",
                                    arena.alloc(Val::Str {
                                        s: &seq[pos_ran.start..pos_ran.end],
                                    }),
                                    arena,
                                )));

                                arena.alloc(sliced) as &'b Val<'b>
                            }),
                        )
                        .collect::<Vec<_>>();

                    vals
                });

                Ok(envs.collect::<Vec<_>>())
            } else {
                todo!()
            }
        } else {
            todo!()
        }
    }
}

#[derive(Clone)]
pub enum OpTm<'a> {
    Let {
        // the location to assign to
        loc: usize,
        // the
        tm: LocTm<'a>,
    },
    Restrict {
        // list of new binds to make
        ids: Vec<String>,
        tm: core::Tm<'a>,
        // the location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
    },
}

impl<'a> OpTm<'a> {
    pub fn eval(
        &self,
        arena: &'a Arena,
        ctx: &Context<'a>,
        params: HashMap<String, Vec<&'a Val<'a>>>,
        param_indices: HashMap<String, usize>,
        error: f32,
    ) -> Result<OpVal<'a>, EvalError> {
        match self {
            OpTm::Let { loc, tm } => Ok(OpVal::Let {
                loc: *loc,
                tm: tm.clone(),
            }),

            OpTm::Restrict {
                ids,
                tm,
                ran,
                fixed,
                save,
            } => {
                if ids.is_empty() {
                    // special case - just wrap up the one value
                    let val = eval(arena, &ctx.tms, tm)?;

                    match &val {
                        Val::Str { s } => Ok(OpVal::Restrict {
                            ctxs: vec![(HashMap::default(), Seq::new(*s, error))],
                            ran: ran.clone(),
                            fixed: fixed.clone(),
                            save: save.clone(),
                        }),
                        _ => panic!("term in region was of the wrong type?!"),
                    }
                } else {
                    let new_binds = params
                        .iter()
                        .filter(|(id, vals)| ids.contains(id))
                        .map(|(id, vals)| {
                            vals.iter().map(|val| (id, val.clone())).collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();

                    let ctxs = match &new_binds[..] {
                        [first, rest @ ..] => {
                            first.iter().flat_map(|first_val: &(&String, &Val<'a>)| {
                                rest.iter().fold(
                                    vec![vec![*first_val]],
                                    |v: Vec<Vec<(&String, &Val<'a>)>>, bind| {
                                        v.iter()
                                            .cartesian_product(bind)
                                            .map(|(v, new)| {
                                                v.iter().chain([new]).cloned().collect::<Vec<_>>()
                                            })
                                            .collect::<Vec<_>>()
                                    },
                                )
                            })
                        }

                        // this can't happen
                        [] => {
                            panic!("binds were empty, but did not end up in special case block?!")
                        }
                    }
                    .map(|bind_ctx| {
                        // WARN this may not be binding things in the right order?
                        let new_ctx = bind_ctx.iter().fold(ctx.clone(), |ctx0, (name, val)| {
                            ctx0.bind_def((*name).clone(), &Val::StrTy, val)
                        });

                        let val = eval(arena, &new_ctx.tms, tm)?;

                        match val {
                            Val::Str { s } => Ok((
                                bind_ctx
                                    .iter()
                                    .map(|(name, val)| {
                                        (
                                            *param_indices
                                                .get(*name)
                                                .expect("bind index was invalid?!"),
                                            *val,
                                        )
                                    })
                                    .collect::<HashMap<_, _>>(),
                                Seq::new(&s, error),
                            )),
                            _ => panic!("term in read region wasn't a string?!"),
                        }
                    })
                    .collect::<Result<Vec<_>, EvalError>>()?;

                    Ok(OpVal::Restrict {
                        ctxs,
                        ran: ran.clone(),
                        fixed: fixed.clone(),
                        save: save.clone(),
                    })
                }
            }
        }
    }
}

pub enum OpVal<'a> {
    Let {
        // the location to assign to
        loc: usize,
        // the
        tm: LocTm<'a>,
    },
    Restrict {
        // list of new binds to make
        ctxs: Vec<(HashMap<usize, &'a Val<'a>>, Seq)>,
        // the location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
    },
}

#[derive(Clone, Default)]
pub struct LocCtx {
    map: HashMap<usize, usize>,
}

impl LocCtx {
    fn get(&self, loc: &usize) -> usize {
        *self
            .map
            .get(loc)
            .expect("couldn't find location {loc} in location context?!")
    }

    fn with(&self, loc: usize, pos: usize) -> LocCtx {
        let mut map = self.map.clone();
        map.insert(loc, pos);

        LocCtx { map }
    }
}

#[derive(Clone, Default)]
pub struct BindCtx<'a> {
    map: HashMap<usize, &'a Val<'a>>,
}

impl<'a> BindCtx<'a> {
    fn get(&self, id: &usize) -> Option<&'a Val<'a>> {
        self.map.get(id).map(|val| *val)
    }

    fn with(&self, id: usize, val: &'a Val<'a>) -> BindCtx<'a> {
        let mut map = self.map.clone();
        map.insert(id, val);

        BindCtx { map }
    }
}

impl<'b> OpVal<'b> {
    fn exec<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<&'a Val<'a>>,
        seq: &'a [u8],
        loc_ctx: &LocCtx,
        bind_ctx: &BindCtx<'a>,
    ) -> Result<Vec<(BindCtx<'a>, LocCtx)>, core::EvalError>
    where
        'b: 'a,
    {
        match self {
            OpVal::Let { loc, tm } => {
                let pos = eval_loc_tm(&arena, env, loc_ctx, tm)?;
                Ok(vec![(bind_ctx.clone(), loc_ctx.with(*loc, pos))])
            }
            OpVal::Restrict {
                ctxs,
                ran,
                fixed,
                save,
            } => {
                //
                let filtered = ctxs
                    .iter()
                    // filter out any that would incur a contradictory binding
                    .filter(|(bs, _)| {
                        bs.iter().all(|(id, val)| {
                            if let Some(val2) = bind_ctx.get(id) {
                                (*val).eq(arena, val2)
                            } else {
                                true
                            }
                        })
                    });

                let mut accumulated = vec![];
                for (local_binds, tm_seq) in filtered {
                    let matches =
                        tm_seq.find_all_disjoint(seq, &ran.map(|i| loc_ctx.get(i) as usize), fixed);

                    // if we have found the pattern sufficient times
                    if matches.len() >= save.len() {
                        // // get the
                        // let min_edit_dist = matches.iter()
                        //     .map(|a| a.dist)
                        //     .sorted()
                        //     .take(save.len())
                        //     .last();

                        let combs = matches
                            .iter()
                            .combinations(save.len())
                            .map(|c| {
                                (
                                    local_binds
                                        .iter()
                                        .fold(bind_ctx.clone(), |old_bind_ctx, (id, val)| {
                                            old_bind_ctx.with(*id, *val)
                                        }),
                                    c.iter().enumerate().fold(
                                        loc_ctx.clone(),
                                        |old_loc_ctx, (i, mat)| {
                                            old_loc_ctx
                                                .with(save.get(i).unwrap().start, mat.ran.start)
                                                .with(save.get(i).unwrap().end, mat.ran.end)
                                        },
                                    ),
                                    c.iter().fold(0, |acc, mat| acc + mat.dist),
                                )
                            })
                            .collect_vec();

                        // return early - we've found it!
                        // return Ok(combs)

                        // return best

                        // return all matching patterns
                        for v in combs {
                            accumulated.push(v);
                        }
                    }
                }

                // for best

                if !accumulated.is_empty() {
                    let best_dist = accumulated
                        .clone()
                        .into_iter()
                        .map(|(_, _, dist)| dist)
                        .min()
                        .unwrap();
                    let final_accumulated_2 = accumulated
                        .into_iter()
                        .filter(|(_, _, dist)| *dist == best_dist)
                        .map(|(a, b, _)| (a, b))
                        .collect();

                    Ok(final_accumulated_2)
                } else {
                    Ok(vec![])
                }
            }
        }
    }
}

use crate::core;

#[derive(Clone)]
pub enum LocTm<'a> {
    Var {
        loc: usize,
    },
    Offset {
        loc_tm: Arc<LocTm<'a>>,
        offset: core::Tm<'a>,
    },
}
fn eval_loc_tm<'a, 'b: 'a>(
    arena: &'a Arena,
    env: &Env<&'a Val<'a>>,
    loc_ctx: &LocCtx,
    loc_tm: &LocTm<'b>,
) -> Result<usize, core::EvalError> {
    match loc_tm {
        LocTm::Var { loc } => Ok(loc_ctx.get(loc)),
        LocTm::Offset { loc_tm, offset } => {
            if let Val::Num { n } = core::eval(arena, env, offset)? {
                let i = n.round() as i32;

                Ok((eval_loc_tm(arena, env, loc_ctx, loc_tm)? as i32 + i) as usize)
            } else {
                panic!("sized read pattern wasn't numeric type?!")
            }
        }
    }
}

pub struct Seq {
    bytes: Vec<u8>,
    dist: usize,
    myers: VarMyers,
}

impl Seq {
    pub fn new(bytes: &[u8], error: f32) -> Seq {
        Seq {
            bytes: bytes.to_ascii_uppercase(),
            myers: VarMyers::new(&bytes.to_ascii_uppercase()),
            dist: (bytes.len() as f32 * error).floor() as usize,
        }
    }

    fn find_all_disjoint(
        &self,
        seq: &[u8],
        search_ran: &Ran<usize>,
        fixed: &Ran<bool>,
    ) -> Vec<Mat> {
        let (start, end): (i32, i32) = match (fixed.start, fixed.end) {
            // somehow pin both ends
            // - i guess just calculate straight levenshtein distance?
            (true, true) => todo!(),

            // pin the start
            (true, false) => (
                search_ran.start as i32,
                (search_ran.start + self.bytes.len() + self.dist as usize) as i32,
            ),

            // pin the end
            (false, true) => (
                search_ran.end as i32 - self.bytes.len() as i32 - self.dist as i32,
                search_ran.end as i32,
            ),

            // just grab the whole slice of the sequence
            (false, false) => (search_ran.start as i32, search_ran.end as i32),
        };

        // return early if the range is inappropriate
        if start < 0 || end > seq.len() as i32 {
            return vec![];
        }

        let trimmed = &seq[start as usize..end as usize];

        self.myers
            .find_all_disjoint(trimmed, self.dist as u8)
            .iter()
            .map(|(mat_start, mat_end, dist)| {
                Mat::new(
                    Ran::new(*mat_start + start as usize, *mat_end + start as usize),
                    *dist as u8,
                )
            })
            .collect()
    }
}

#[derive(Debug)]
struct Mat {
    ran: Ran<usize>,
    dist: u8,
}

impl Mat {
    fn new(ran: Ran<usize>, dist: u8) -> Mat {
        Mat { ran, dist }
    }
}
