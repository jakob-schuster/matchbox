use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{self, matcher, EvalError, Val},
    myers::VarMyers,
    surface::{self, check_tm, Context, ElabError, Region, RegionData},
    util::{Arena, Env, Ran},
    visit,
};

use super::Matcher;

pub struct ReadMatcher<'p: 'a, 'a> {
    /// The search operations to perform
    pub ops: Vec<OpVal<'p, 'a>>,
    /// The location ranges to bind at the end in each world
    pub binds: Vec<Ran<usize>>,
    /// The location to label to the end
    pub end: usize,
}

impl<'p: 'a, 'a> Matcher<'p, 'a> for ReadMatcher<'p, 'a> {
    fn evaluate<'b>(
        &'b self,
        arena: &'a Arena,
        env: &'b Env<&'a Val<'p, 'a>>,
        val: &'a Val<'p, 'a>,
    ) -> Result<Vec<Vec<&'a Val<'p, 'a>>>, EvalError> {
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

                                arena.alloc(sliced) as &'a Val<'p, 'a>
                            }),
                        )
                        .collect::<Vec<_>>();

                    vals
                });

                Ok(envs.collect::<Vec<_>>())
            } else {
                panic!("gave non-read value to read matcher?!")
            }
        } else {
            panic!("gave non-record value to read matcher?!")
        }
    }
}

#[derive(Clone)]
enum Reg<'p: 'a, 'a> {
    Hole,
    Exp(Vec<String>, core::Tm<'p, 'a>),
}

fn flatten_regs<'a>(
    binds: &[String],
    bind_tys: &HashMap<String, &'a core::Val<'a, 'a>>,
    i: usize,
    regs: Vec<&'a Region>,
    sized_acc: Vec<(core::Tm<'a, 'a>, Ran<usize>)>,
    named_acc: Vec<(String, Ran<usize>)>,
    regs_acc: Vec<(Reg<'a, 'a>, Ran<usize>)>,
    arena: &'a Arena,
    ctx: &Context<'a, 'a>,
) -> Result<
    (
        Vec<(core::Tm<'a, 'a>, Ran<usize>)>,
        Vec<(String, Ran<usize>)>,
        Vec<(Reg<'a, 'a>, Ran<usize>)>,
    ),
    ElabError,
> {
    let first = regs.first();
    let rest = regs.iter().skip(1).cloned().collect_vec();

    match first {
        Some(reg) => match &reg.data {
            RegionData::Hole => flatten_regs(
                binds,
                bind_tys,
                i + 1,
                rest,
                sized_acc,
                named_acc,
                regs_acc
                    .into_iter()
                    .chain([(Reg::Hole, Ran::new(i, i + 1))])
                    .collect(),
                arena,
                ctx,
            ),
            RegionData::Term { tm } => {
                // get the ids used in the tm
                let ids = visit::ids_tm(&tm)
                    .into_iter()
                    .filter(|id| binds.contains(id))
                    .collect::<Vec<_>>();

                // build a new context with these bound
                let new_ctx = ids.iter().fold(ctx.clone(), |ctx0, id| {
                    ctx0.bind_param(id.clone(), bind_tys.get(id).unwrap(), arena)
                });

                // check that the tm is a string
                let ctm = check_tm(arena, &new_ctx, &tm, &core::Val::StrTy)?;

                flatten_regs(
                    binds,
                    bind_tys,
                    i + 1,
                    rest,
                    sized_acc,
                    named_acc,
                    regs_acc
                        .into_iter()
                        .chain([(
                            Reg::Exp(
                                // collect only the ids present in both the
                                // exp and the binds
                                ids, ctm,
                            ),
                            Ran::new(i, i + 1),
                        )])
                        .collect(),
                    arena,
                    ctx,
                )
            }
            RegionData::Named {
                name,
                regs: inner_regs,
            } => flatten_regs(
                binds,
                bind_tys,
                i,
                inner_regs.iter().chain(rest).collect_vec(),
                sized_acc,
                named_acc
                    .into_iter()
                    .chain([(name.clone(), Ran::new(i, i + inner_regs.len() as usize))])
                    .collect(),
                regs_acc,
                arena,
                ctx,
            ),
            RegionData::Sized {
                tm,
                regs: inner_regs,
            } => {
                // first check that tm is a numeric
                let ctm = check_tm(arena, ctx, &tm, &core::Val::NumTy)?;

                flatten_regs(
                    binds,
                    bind_tys,
                    i,
                    inner_regs.iter().chain(rest).collect_vec(),
                    sized_acc
                        .into_iter()
                        .chain([(ctm, Ran::new(i, i + inner_regs.len() as usize))])
                        .collect_vec(),
                    named_acc,
                    regs_acc,
                    arena,
                    ctx,
                )
            }
        },
        None => Ok((sized_acc, named_acc, regs_acc)),
    }
}

pub fn infer_read_pattern<'a>(
    arena: &'a Arena,
    ctx: &Context<'a, 'a>,
    regs: &'a [Region],
    params: Vec<(String, &'a core::Val<'a, 'a>, &'a core::Val<'a, 'a>)>,
    error: f32,
) -> Result<(ReadMatcher<'a, 'a>, Vec<String>), ElabError> {
    // first, separate all the regions out into
    // - a vec of hole/tm
    // - the list of named, sized, etc with location ranges

    let param_names = params
        .iter()
        .map(|(name, _, _)| name.clone())
        .collect::<Vec<_>>();

    let bind_tys = params
        .clone()
        .into_iter()
        .map(|(name, _, ty)| (name, ty))
        .collect::<HashMap<_, _>>();

    let (sized, named, regs) = flatten_regs(
        &param_names,
        &bind_tys,
        0,
        regs.iter().collect(),
        vec![],
        vec![],
        vec![],
        arena,
        ctx,
    )?;

    // then, walk along and generate an order for everything to happen in
    // gaining information as we go

    // the locations we know at the start
    let mut known = vec![0, regs.len()];

    let mut ops: Vec<OpTm> = vec![];

    fn check_all_fixed_lens<'a>(
        ops: &Vec<OpTm<'a, 'a>>,
        known: &mut Vec<usize>,
        sized: &Vec<(core::Tm<'a, 'a>, Ran<usize>)>,
        arena: &'a Arena,
    ) -> (Vec<usize>, Vec<OpTm<'a, 'a>>) {
        fn learn_new_fixed_lens<'aa>(
            known: &mut Vec<usize>,
            fixed_lens: &[(core::Tm<'aa, 'aa>, Ran<usize>)],
            arena: &'aa Arena,
        ) -> Vec<OpTm<'aa, 'aa>> {
            let mut ops = vec![];
            let mut new_known = known.clone();

            for (expr_num, ran) in fixed_lens {
                match ran.map(|l| known.contains(l)).to_tuple() {
                    // todo: insert something that actually verifies the length?
                    (true, true) => {}

                    // if one end is known, insert a let operation
                    (true, false) => {
                        ops.push(OpTm::Let {
                            loc: ran.end,
                            tm: LocTm::Offset {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.start }),
                                offset: expr_num.clone(),
                            },
                        });
                        new_known.push(ran.end);
                    }

                    (false, true) => {
                        ops.push(OpTm::Let {
                            loc: ran.start,
                            tm: LocTm::Offset {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.end }),
                                offset: core::Tm::new(
                                    expr_num.location.clone(),
                                    core::TmData::FunApp {
                                        head: Arc::new(
                                            check_tm(
                                                arena,
                                                &core::library::standard_library(arena, true),
                                                arena.alloc(surface::Tm::new(
                                                    expr_num.location.clone(),
                                                    surface::TmData::Name {
                                                        name: "unary_minus".to_string(),
                                                    },
                                                )),
                                                &core::Val::FunTy {
                                                    args: vec![&core::Val::NumTy],
                                                    body: &core::Val::NumTy,
                                                },
                                            )
                                            .expect("couldn't find unary minus operation?!"),
                                        ),
                                        args: vec![expr_num.clone()],
                                    },
                                ),
                            },
                        });

                        new_known.push(ran.start);
                    }

                    (false, false) => {}
                }
            }

            known.clone_from(&new_known);

            ops
        }

        // first check if we know one side of any fixed-length regions
        let mut ops = ops.clone();
        let mut known = known.clone();

        let mut ops_new = ops.clone();
        ops_new.extend(learn_new_fixed_lens(&mut known, sized, arena));
        // WARN have changed this -- see if it works
        while !ops.len().eq(&ops_new.len()) {
            // update the old ops
            ops.clone_from(&ops_new);
            // extend the new ops
            ops_new.extend(learn_new_fixed_lens(&mut known, sized, arena));
        }
        ops.clone_from(&ops_new);

        (known, ops)
    }

    // first, the naive approach - just bind everything left to right

    fn get_tightest_known(known_locs: &[usize], ran: &Ran<usize>) -> Ran<usize> {
        if let (Some(start), Some(end)) = (
            known_locs.iter().filter(|loc| **loc <= ran.start).max(),
            known_locs.iter().filter(|loc| **loc >= ran.end).min(),
        ) {
            Ran::new(*start, *end)
        } else {
            panic!("didn't know the necessary locations?!");
        }
    }

    // rank the binds - with ones as-yet-unknown ranking the worst

    // rank the fixed-size regions we know,
    // with as-yet-unknown ranking the worst

    // in that order, can we make any restrictions within these regions?

    let bindable_regs = regs.iter().flat_map(|(reg, ran)| match reg {
        Reg::Hole => None,
        Reg::Exp(binds, exp) => Some((ran.clone(), (binds, exp.clone()))),
    });

    for (ran, (binds, exp)) in bindable_regs {
        // first, do the whole checking-for-range-restrictions thing
        (known, ops) = check_all_fixed_lens(&ops, &mut known, &sized, arena);

        let search: Ran<usize> = get_tightest_known(&known, &ran);

        // todo: check if the loc is fixed
        let fixed = Ran::new(search.start == ran.start, search.end == ran.end);

        ops.push(OpTm::Restrict {
            ids: binds.clone(),
            tm: exp,
            ran: search,
            fixed,
            save: vec![ran.clone()],
        });

        known.push(ran.start);
        known.push(ran.end);
    }

    // check for range restriction one final time
    (_, ops) = check_all_fixed_lens(&ops, &mut known, &sized, arena);

    let param_vals = params
        .clone()
        .iter()
        .map(|(name, val, _)| match val {
            core::Val::List { v } => (name.clone(), v.clone()),
            _ => panic!("parameter was not drawing from list?"),
        })
        .collect::<HashMap<_, _>>();
    let param_indices = param_names
        .iter()
        .enumerate()
        .map(|(i, name)| (name.clone(), i))
        .collect::<HashMap<_, _>>();

    let final_ops = ops
        .iter()
        .map(|op| op.eval(arena, ctx, param_vals.clone(), param_indices.clone(), error))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ElabError::from_eval_error)?;

    Ok((
        ReadMatcher {
            ops: final_ops,
            binds: named.iter().map(|(_, ran)| ran.clone()).collect::<Vec<_>>(),
            end: regs.len(),
        },
        named.iter().map(|(name, _)| name.clone()).collect(),
    ))
}

#[derive(Clone)]
pub enum OpTm<'p: 'a, 'a> {
    Let {
        // the location to assign to
        loc: usize,
        // the
        tm: LocTm<'p, 'a>,
    },
    Restrict {
        // list of new binds to make
        ids: Vec<String>,
        tm: core::Tm<'p, 'a>,
        // the location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
    },
}

impl<'p: 'a, 'a> OpTm<'p, 'a> {
    pub fn eval(
        &self,
        arena: &'a Arena,
        ctx: &Context<'p, 'a>,
        params: HashMap<String, Vec<&'a Val<'p, 'a>>>,
        param_indices: HashMap<String, usize>,
        error: f32,
    ) -> Result<OpVal<'p, 'a>, EvalError> {
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
                    let val = core::eval(arena, &ctx.tms, tm)?;

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
                            first
                                .iter()
                                .flat_map(|first_val: &(&String, &Val<'p, 'a>)| {
                                    rest.iter().fold(
                                        vec![vec![*first_val]],
                                        |v: Vec<Vec<(&String, &Val<'p, 'a>)>>, bind| {
                                            v.iter()
                                                .cartesian_product(bind)
                                                .map(|(v, new)| {
                                                    v.iter()
                                                        .chain([new])
                                                        .cloned()
                                                        .collect::<Vec<_>>()
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
                            ctx0.bind_def((*name).clone(), &core::Val::StrTy, val)
                        });

                        let val = core::eval(arena, &new_ctx.tms, tm)?;

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

pub enum OpVal<'p: 'a, 'a> {
    Let {
        // the location to assign to
        loc: usize,
        // the
        tm: LocTm<'p, 'a>,
    },
    Restrict {
        // list of new binds to make
        ctxs: Vec<(HashMap<usize, &'a Val<'p, 'a>>, Seq)>,
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
pub struct BindCtx<'p: 'a, 'a> {
    map: HashMap<usize, &'a core::Val<'p, 'a>>,
}

impl<'p: 'a, 'a> BindCtx<'p, 'a> {
    fn get(&self, id: &usize) -> Option<&'a core::Val<'p, 'a>> {
        self.map.get(id).map(|val| *val)
    }

    fn with(&self, id: usize, val: &'a core::Val<'p, 'a>) -> BindCtx<'p, 'a> {
        let mut map = self.map.clone();
        map.insert(id, val);

        BindCtx { map }
    }
}

impl<'p: 'a, 'a> OpVal<'p, 'a> {
    fn exec(
        &self,
        arena: &'a Arena,
        env: &Env<&'a core::Val<'p, 'a>>,
        seq: &'a [u8],
        loc_ctx: &LocCtx,
        bind_ctx: &BindCtx<'p, 'a>,
    ) -> Result<Vec<(BindCtx<'p, 'a>, LocCtx)>, core::EvalError> {
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

#[derive(Clone)]
pub enum LocTm<'p: 'a, 'a> {
    Var {
        loc: usize,
    },
    Offset {
        loc_tm: Arc<LocTm<'p, 'a>>,
        offset: core::Tm<'p, 'a>,
    },
}
fn eval_loc_tm<'p: 'a, 'a>(
    arena: &'a Arena,
    env: &Env<&'a core::Val<'p, 'a>>,
    loc_ctx: &LocCtx,
    loc_tm: &LocTm<'p, 'a>,
) -> Result<usize, core::EvalError> {
    match loc_tm {
        LocTm::Var { loc } => Ok(loc_ctx.get(loc)),
        LocTm::Offset { loc_tm, offset } => {
            if let core::Val::Num { n } = core::eval(arena, env, offset)? {
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
            .find_all_disjoint(trimmed, self.dist)
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

impl<'p: 'a, 'a> Display for OpTm<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpTm::Let { loc, tm } => format!("{} = {}", loc, tm).fmt(f),
            OpTm::Restrict {
                ids,
                tm,
                ran,
                fixed,
                save,
            } => format!(
                "find ({}){} in {} fixed {} save {}",
                ids.join(","),
                tm,
                ran,
                fixed,
                "a"
            )
            .fmt(f),
        }
    }
}

impl<'p: 'a, 'a> Display for LocTm<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocTm::Var { loc } => loc.fmt(f),
            LocTm::Offset { loc_tm, offset } => format!("{} + {}", loc_tm, offset).fmt(f),
        }
    }
}

impl<'p: 'a, 'a> Display for Reg<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reg::Hole => "_".fmt(f),
            Reg::Exp(items, located) => format!("({}){}", items.join(","), located).fmt(f),
        }
    }
}
