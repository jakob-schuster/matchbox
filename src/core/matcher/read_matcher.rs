use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{self, matcher, EvalError, InternalError, Val},
    myers::VarMyers,
    surface::{self, check_tm, Context, ElabError, Region, RegionData},
    util::{bytes_to_string, Arena, Cache, Env, Location, Ran},
    visit,
};

use super::Matcher;

#[derive(Clone)]
pub enum MatchMode {
    All,
    One,
    Unique,
}

impl Display for MatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchMode::All => "all",
            MatchMode::One => "one",
            MatchMode::Unique => "unique",
        }
        .fmt(f)
    }
}

pub struct ReadMatcher<'a> {
    /// The search operations to perform
    pub ops: Vec<OpVal<'a>>,
    /// The location ranges to bind at the end in each world
    pub binds: Vec<Ran<usize>>,
    /// The location to label to the end
    pub end: usize,
    /// Whether the matcher will return all possible matches, or just one
    pub mode: MatchMode,
}

impl<'p> Matcher<'p> for ReadMatcher<'p> {
    fn evaluate<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        if let Val::Rec { rec } = val {
            if let Val::Str { s: seq } = rec.get(b"seq").expect("") {
                // first, generate all the worlds from the operations
                let loc_ctx = LocCtx::default().with(0, 0).with(self.end, seq.len());
                let worlds =
                    self.ops
                        .iter()
                        .fold(vec![(BindCtx::default(), loc_ctx, 0)], |worlds, op| {
                            worlds
                                .iter()
                                .flat_map(|(bind_ctx, loc_ctx, edit_dist)| {
                                    op.exec(arena, env, seq, loc_ctx, bind_ctx, *edit_dist)
                                })
                                .flatten()
                                .collect::<Vec<_>>()
                        });

                // choose either the first world,
                // or all the possible worlds
                match self.mode {
                    MatchMode::All => {
                        // then, make all the binds
                        let envs = worlds.iter().map(|(bind_ctx, loc_ctx, _)| {
                            // make all the binds from bind_ctx one by one
                            let vals = bind_ctx
                                .map
                                .iter()
                                .sorted_by_key(|(id, _)| **id)
                                .map(|(_, val)| Ok((*val).clone()))
                                .chain(
                                    // then make the other sequence binds
                                    self.binds.iter().map(|ran| {
                                        let pos_ran = ran.map(|loc| loc_ctx.get(loc));
                                        let sliced = rec.slice(pos_ran.start, pos_ran.end)?;

                                        Ok(sliced as Val<'a>)
                                    }),
                                )
                                .collect::<Result<Vec<_>, InternalError>>()
                                .map_err(|e| EvalError::from_internal(e, Location::new(0, 0)));

                            vals
                        });

                        envs.collect()
                    }
                    MatchMode::One => {
                        // then, make all the binds
                        if let Some((bind_ctx, loc_ctx, _)) = worlds
                            .iter()
                            .sorted_by_key(|(_, _, edit_dist)| *edit_dist)
                            // just pick an arbitrary first of the tied best worlds
                            .next()
                        {
                            // make all the binds from bind_ctx one by one
                            let vals = bind_ctx
                                .map
                                .iter()
                                .sorted_by_key(|(id, _)| **id)
                                .map(|(_, val)| Ok((*val).clone()))
                                .chain(
                                    // then make the other sequence binds
                                    self.binds.iter().map(|ran| {
                                        let pos_ran = ran.map(|loc| loc_ctx.get(loc));
                                        let sliced = rec.slice(pos_ran.start, pos_ran.end)?;

                                        Ok(sliced as Val<'a>)
                                    }),
                                )
                                .collect::<Result<Vec<_>, InternalError>>()
                                .map_err(|e| EvalError::from_internal(e, Location::new(0, 0)));

                            vals.map(|v| vec![v])
                        } else {
                            Ok(vec![])
                        }
                    }
                    MatchMode::Unique => {
                        // then, make all the binds
                        if let Some(best_dist) =
                            worlds.iter().map(|(_, _, edit_dist)| *edit_dist).min()
                        {
                            let all_best = worlds
                                .iter()
                                .filter(|(_, _, edit_dist)| edit_dist.eq(&best_dist))
                                .collect_vec();

                            match &all_best[..] {
                                // if there is exactly one best world
                                [(bind_ctx, loc_ctx, _)] => {
                                    // make all the binds from bind_ctx one by one
                                    let vals = bind_ctx
                                        .map
                                        .iter()
                                        .sorted_by_key(|(id, _)| **id)
                                        .map(|(_, val)| Ok((*val).clone()))
                                        .chain(
                                            // then make the other sequence binds
                                            self.binds.iter().map(|ran| {
                                                let pos_ran = ran.map(|loc| loc_ctx.get(loc));
                                                let sliced =
                                                    rec.slice(pos_ran.start, pos_ran.end)?;

                                                Ok(sliced as Val<'a>)
                                            }),
                                        )
                                        .collect::<Result<Vec<_>, InternalError>>()
                                        .map_err(|e| {
                                            EvalError::from_internal(e, Location::new(0, 0))
                                        });

                                    vals.map(|v| vec![v])
                                }
                                _ => Ok(vec![]),
                            }
                        } else {
                            Ok(vec![])
                        }
                    }
                }
            } else {
                panic!("gave non-read value {} to read matcher?!", val)
            }
        } else {
            panic!("gave non-record value {} to read matcher?!", val)
        }
    }
}

impl<'a> Display for ReadMatcher<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!(
            "read_matcher(binds: [{}], end: {}, mode: {}, ops: [{}])",
            self.binds
                .iter()
                .map(|b| b.to_string())
                .collect::<Vec<_>>()
                .join(","),
            self.end,
            self.mode,
            self.ops
                .iter()
                .map(|op| op.to_string())
                .collect::<Vec<_>>()
                .join("; ")
        )
        .fmt(f)
    }
}

#[derive(Clone)]
enum Reg<'p> {
    Hole,
    Exp(Vec<String>, core::Tm<'p>, core::Tm<'p>),
}

fn flatten_regs<'a>(
    binds: &[String],
    bind_tys: &HashMap<String, core::Val<'a>>,
    i: usize,
    regs: Vec<&'a Region>,
    sized_acc: Vec<(core::Tm<'a>, Ran<usize>)>,
    named_acc: Vec<(String, Ran<usize>)>,
    regs_acc: Vec<(Reg<'a>, Ran<usize>)>,
    arena: &'a Arena,
    ctx: &Context<'a>,
) -> Result<
    (
        Vec<(core::Tm<'a>, Ran<usize>)>,
        Vec<(String, Ran<usize>)>,
        Vec<(Reg<'a>, Ran<usize>)>,
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
            RegionData::Term { tm, error } => {
                // get the ids used in the tm
                let ids = visit::ids_tm(tm)
                    .into_iter()
                    .filter(|id| binds.contains(id))
                    .collect::<Vec<_>>();

                // build a new context with these bound
                let new_ctx = ids.iter().fold(ctx.clone(), |ctx0, id| {
                    ctx0.bind_param(id.clone(), bind_tys.get(id).unwrap().clone(), arena)
                });

                // check that the tm is a string
                let ctm = check_tm(arena, &new_ctx, tm, &core::Val::StrTy)?;
                // and that the error tm is a num
                let error_ctm = check_tm(arena, &new_ctx, error, &core::Val::NumTy)?;

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
                                ids, ctm, error_ctm,
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
                    .chain([(name.clone(), Ran::new(i, i + inner_regs.len()))])
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
                let ctm = check_tm(arena, ctx, tm, &core::Val::NumTy)?;

                flatten_regs(
                    binds,
                    bind_tys,
                    i,
                    inner_regs.iter().chain(rest).collect_vec(),
                    sized_acc
                        .into_iter()
                        .chain([(ctm, Ran::new(i, i + inner_regs.len()))])
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
    ctx: &Context<'a>,
    regs: &'a [Region],
    params: Vec<(String, core::Val<'a>, core::Val<'a>)>,
    mode: &MatchMode,
) -> Result<(ReadMatcher<'a>, Vec<String>), ElabError> {
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
        ops: &Vec<OpTm<'a>>,
        known: &mut Vec<usize>,
        sized: &Vec<(core::Tm<'a>, Ran<usize>)>,
        arena: &'a Arena,
    ) -> (Vec<usize>, Vec<OpTm<'a>>) {
        fn learn_new_fixed_lens<'aa>(
            known: &mut Vec<usize>,
            fixed_lens: &[(core::Tm<'aa>, Ran<usize>)],
            arena: &'aa Arena,
        ) -> Vec<OpTm<'aa>> {
            let mut ops = vec![];
            let mut new_known = known.clone();

            for (expr_num, ran) in fixed_lens {
                // get the closest flanking sequences
                let flanking = get_tightest_known(known, ran);

                match ran.map(|l| known.contains(l)).to_tuple() {
                    // todo: insert something that actually verifies the length?
                    (true, true) => {}

                    // if one end is known, insert a let operation
                    (true, false) => {
                        ops.push(OpTm::Let {
                            loc: ran.end,
                            tm: LocTm::Plus {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.start }),
                                offset: expr_num.clone(),
                            },
                            flanking,
                        });
                        new_known.push(ran.end);
                    }
                    (false, true) => {
                        ops.push(OpTm::Let {
                            loc: ran.start,
                            tm: LocTm::Minus {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.end }),
                                offset: expr_num.clone(),
                            },
                            flanking,
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
        Reg::Exp(binds, exp, error) => Some((ran.clone(), (binds, exp.clone(), error.clone()))),
    });

    for (ran, (binds, exp, error)) in bindable_regs {
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
            error,
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
        .map(|op| op.eval(arena, ctx, param_vals.clone(), param_indices.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ElabError::from_eval_error)?;

    Ok((
        ReadMatcher {
            ops: final_ops,
            binds: named.iter().map(|(_, ran)| ran.clone()).collect::<Vec<_>>(),
            end: regs.len(),
            mode: mode.clone(),
        },
        named.iter().map(|(name, _)| name.clone()).collect(),
    ))
}

#[derive(Clone)]
pub enum OpTm<'p> {
    Let {
        // the location to assign to
        loc: usize,
        // the
        tm: LocTm<'p>,
        // the closest known locations to check bounds against
        flanking: Ran<usize>,
    },
    Restrict {
        // list of new binds to make
        ids: Vec<String>,
        tm: core::Tm<'p>,
        // the location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
        // the error rate to apply
        error: core::Tm<'p>,
    },
}

impl<'p> OpTm<'p> {
    pub fn eval<'a>(
        &self,
        arena: &'a Arena,
        ctx: &Context<'a>,
        params: HashMap<String, Vec<Val<'a>>>,
        param_indices: HashMap<String, usize>,
    ) -> Result<OpVal<'a>, EvalError>
    where
        'p: 'a,
    {
        match self {
            OpTm::Let { loc, tm, flanking } => Ok(OpVal::Let {
                loc: *loc,
                tm: tm.coerce(arena),
                flanking: flanking.clone(),
            }),

            OpTm::Restrict {
                ids,
                tm,
                ran,
                fixed,
                save,
                error,
            } => {
                if ids.is_empty() {
                    // special case - just wrap up the one value
                    // (cache can be empty, because this is pre-caching)
                    let val = tm.eval(arena, &Env::default(), &Cache::default(), &ctx.tms)?;
                    let error_val =
                        error.eval(arena, &Env::default(), &Cache::default(), &ctx.tms)?;

                    match (&val, &error_val) {
                        (Val::Str { s }, Val::Num { n }) => Ok(OpVal::Restrict {
                            ctxs: vec![(HashMap::default(), Seq::new(*s, *n))],
                            ran: ran.clone(),
                            fixed: fixed.clone(),
                            save: save.clone(),
                        }),
                        _ => panic!("term in region was of the wrong type?!"),
                    }
                } else {
                    let new_binds = params
                        .into_iter()
                        .filter(|(id, vals)| ids.contains(id))
                        .map(|(id, vals)| {
                            vals.iter()
                                .map(|val| (id.clone(), val.clone()))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();

                    let ctxs = match &new_binds[..] {
                        [first, rest @ ..] => {
                            first.into_iter().flat_map(|first_val: &(String, Val<'a>)| {
                                rest.iter().fold(
                                    vec![vec![first_val.clone()]],
                                    |v: Vec<Vec<(String, Val<'a>)>>, bind| {
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
                        let new_ctx =
                            bind_ctx
                                .clone()
                                .into_iter()
                                .fold(ctx.clone(), |ctx0, (name, val)| {
                                    ctx0.bind_def(name, core::Val::StrTy, val as Val<'a>)
                                });

                        // WARN cache can be empty because this is pre-caching.
                        let val =
                            tm.eval(arena, &Env::default(), &Cache::default(), &new_ctx.tms)?;
                        let error_val =
                            error.eval(arena, &Env::default(), &Cache::default(), &new_ctx.tms)?;

                        match (val, error_val) {
                            (Val::Str { s }, Val::Num { n }) => Ok((
                                bind_ctx
                                    .into_iter()
                                    .map(|(name, val)| {
                                        (
                                            *param_indices
                                                .get(&name)
                                                .expect("bind index was invalid?!"),
                                            val,
                                        )
                                    })
                                    .collect::<HashMap<_, _>>(),
                                Seq::new(&s, n),
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
        // the closest known locations to check bounds against
        flanking: Ran<usize>,
    },
    Restrict {
        // list of new binds to make
        ctxs: Vec<(HashMap<usize, Val<'a>>, Seq)>,
        // the location range to search between
        ran: Ran<usize>,
        // whether the ends are fixed
        fixed: Ran<bool>,
        // the locations to save
        save: Vec<Ran<usize>>,
    },
}

impl<'a> Display for OpVal<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpVal::Let { loc, tm, flanking } => {
                format!("let {} = {} checking between {}", loc, tm, flanking).fmt(f)
            }
            OpVal::Restrict {
                ctxs,
                ran,
                fixed,
                save,
            } => format!(
                "restrict [{}] to {} with fixed {} saving to [{}]",
                ctxs.iter()
                    .map(|(map, seq)| format!(
                        "ctx(map: {}, seq: {})",
                        map.iter()
                            .map(|(a, b)| format!("({}, {})", a, b))
                            .collect_vec()
                            .join(","),
                        bytes_to_string(&seq.bytes).unwrap()
                    ))
                    .collect_vec()
                    .join(","),
                ran,
                fixed,
                save.iter().map(|a| a.to_string()).collect_vec().join(",")
            )
            .fmt(f),
        }
    }
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
    map: HashMap<usize, core::Val<'a>>,
}

impl<'a> BindCtx<'a> {
    fn get(&self, id: &usize) -> Option<&core::Val<'a>> {
        self.map.get(id)
    }

    fn with(&self, id: usize, val: core::Val<'a>) -> BindCtx<'a> {
        let mut map = self.map.clone();
        map.insert(id, val);

        BindCtx { map }
    }
}

impl<'p> OpVal<'p> {
    fn exec<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<core::Val<'a>>,
        seq: &'a [u8],
        loc_ctx: &LocCtx,
        bind_ctx: &BindCtx<'a>,
        edit_dist: u8,
    ) -> Result<Vec<(BindCtx<'a>, LocCtx, u8)>, core::EvalError>
    where
        'p: 'a,
    {
        match self {
            OpVal::Let { loc, tm, flanking } => {
                let pos = tm.eval(arena, env, loc_ctx)?;
                let flanking_pos = flanking.map(|loc| loc_ctx.get(loc));

                // return out if the range is inappropriate
                if pos > flanking_pos.start || pos < flanking_pos.end {
                    Ok(vec![])
                } else {
                    Ok(vec![(bind_ctx.clone(), loc_ctx.with(*loc, pos), edit_dist)])
                }
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
                                (*val).eq(val2)
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

                        let combs: Vec<(BindCtx<'a>, LocCtx, u8)> = matches
                            .iter()
                            .combinations(save.len())
                            .map(|c| {
                                (
                                    local_binds.iter().fold(
                                        bind_ctx.clone(),
                                        |old_bind_ctx, (id, val)| {
                                            old_bind_ctx.with(*id, val.coerce())
                                        },
                                    ) as BindCtx<'a>,
                                    c.iter().enumerate().fold(
                                        loc_ctx.clone(),
                                        |old_loc_ctx, (i, mat)| {
                                            old_loc_ctx
                                                .with(save.get(i).unwrap().start, mat.ran.start)
                                                .with(save.get(i).unwrap().end, mat.ran.end)
                                        },
                                    ) as LocCtx,
                                    c.iter().fold(0, |acc, mat| acc + mat.dist) as u8,
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
                        .map(|(a, b, c)| (a, b, c + edit_dist))
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
pub enum LocTm<'p> {
    Var {
        loc: usize,
    },
    Plus {
        loc_tm: Arc<LocTm<'p>>,
        offset: core::Tm<'p>,
    },
    Minus {
        loc_tm: Arc<LocTm<'p>>,
        offset: core::Tm<'p>,
    },
}

impl<'p> LocTm<'p> {
    fn eval<'a>(
        &self,
        arena: &'a Arena,
        env: &Env<core::Val<'a>>,
        loc_ctx: &LocCtx,
    ) -> Result<usize, core::EvalError>
    where
        'p: 'a,
    {
        match self {
            LocTm::Var { loc } => Ok(loc_ctx.get(loc)),
            LocTm::Plus { loc_tm, offset } => {
                // WARN changed the order of env and env default; see if this works
                // WARN cache can be empty because this is pre-caching
                if let core::Val::Num { n } =
                    offset.eval(arena, &Env::default(), &Cache::default(), env)?
                {
                    let i = n.round() as i32;

                    Ok((loc_tm.eval(arena, env, loc_ctx)? as i32 + i) as usize)
                } else {
                    panic!("sized read pattern wasn't numeric type?!")
                }
            }
            LocTm::Minus { loc_tm, offset } => {
                // WARN changed the order of env and env default; see if this works
                // WARN cache can be empty because this is pre-caching
                if let core::Val::Num { n } =
                    offset.eval(arena, &Env::default(), &Cache::default(), env)?
                {
                    let i = n.round() as i32;

                    Ok((loc_tm.eval(arena, env, loc_ctx)? as i32 - i) as usize)
                } else {
                    panic!("sized read pattern wasn't numeric type?!")
                }
            }
        }
    }

    fn coerce<'a>(&self, arena: &'a Arena) -> LocTm<'a>
    where
        'p: 'a,
    {
        match self {
            LocTm::Var { loc } => LocTm::Var { loc: *loc },
            LocTm::Plus { loc_tm, offset } => LocTm::Plus {
                loc_tm: Arc::new(loc_tm.coerce(arena)),
                offset: offset.coerce(),
            },
            LocTm::Minus { loc_tm, offset } => LocTm::Minus {
                loc_tm: Arc::new(loc_tm.coerce(arena)),
                offset: offset.coerce(),
            },
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

impl<'a> Display for OpTm<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpTm::Let { loc, tm, flanking } => {
                format!("{} = {} checking {}", loc, tm, flanking).fmt(f)
            }
            OpTm::Restrict {
                ids,
                tm,
                ran,
                fixed,
                save,
                error,
            } => format!(
                "find ({}){} in {} fixed {} save {} with error {}",
                ids.join(","),
                tm,
                ran,
                fixed,
                "a",
                error
            )
            .fmt(f),
        }
    }
}

impl<'a> Display for LocTm<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocTm::Var { loc } => loc.fmt(f),
            LocTm::Plus { loc_tm, offset } => format!("{} + {}", loc_tm, offset).fmt(f),
            LocTm::Minus { loc_tm, offset } => format!("{} - {}", loc_tm, offset).fmt(f),
        }
    }
}

impl<'a> Display for Reg<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reg::Hole => "_".fmt(f),
            Reg::Exp(items, located, error) => {
                format!("({}){}<{}>", items.join(","), located, error).fmt(f)
            }
        }
    }
}
