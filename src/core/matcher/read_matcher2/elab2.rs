use std::sync::Arc;

use itertools::{Either, Itertools};

use crate::{
    core::{
        self,
        eval2::env,
        matcher::{
            read_matcher2::{
                eval2::match_context::bind_context::BindContext, seq::Seq, Direction, LocOp, Op,
                OpData, ParamContext, ParamData, ReadMatcher,
            },
            ErrorMatcher,
        },
        EvalError, InternalError, ListVal, Val,
    },
    surface::{
        self,
        elab::ElabError,
        elab2::context::{Address, Context},
        MatchMode, ReadParameter, Region, RegionData,
    },
    util::{location::Location, ran::Ran},
};

#[derive(Clone)]
enum FlatRegion {
    Hole,
    Tm {
        // the term
        tm: surface::Tm,
        // the term representing the error rate
        error_tm: surface::Tm,
    },
}
enum CoreFlatRegionA {
    Hole,
    Tm {
        params: Vec<core::Tm>,
        tm: core::Tm,
        error_tm: core::Tm,
    },
}

enum CoreFlatRegion<'p> {
    Hole,
    Tm { data: OpData<'p> },
}

impl<'p> CoreFlatRegion<'p> {
    fn new_hole() -> CoreFlatRegion<'p> {
        CoreFlatRegion::Hole
    }

    fn new_tm(data: OpData<'p>) -> CoreFlatRegion<'p> {
        CoreFlatRegion::Tm { data }
    }
}

#[derive(Clone)]
pub struct CoreReadParameter<'p> {
    pub name: String,
    pub tm: core::Tm,
    pub ty: Val<'p>,
}

impl<'p> CoreReadParameter<'p> {
    pub fn reference<'r>(&'r self) -> CoreReadParameter<'r> {
        CoreReadParameter {
            name: self.name.clone(),
            tm: self.tm.clone(),
            ty: self.ty.reference(),
        }
    }
}

#[derive(Clone)]
pub enum CoreReadParameterData<'p> {
    Unevaluated { ctm: &'p core::Tm },
    Evaluated { vals: ListVal<'p> },
}

impl<'p> CoreReadParameterData<'p> {
    pub fn new(ctm: &'p core::Tm) -> CoreReadParameterData<'p> {
        CoreReadParameterData::Unevaluated { ctm }
    }

    pub fn is_neutral(&self) -> bool {
        match self {
            CoreReadParameterData::Unevaluated { ctm } => true,
            CoreReadParameterData::Evaluated { vals } => false,
        }
    }

    pub fn partial_eval<'r, 'a>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        location: &Location,
    ) -> Result<Either<CoreReadParameterData<'r>, ListVal<'r>>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match self {
            CoreReadParameterData::Unevaluated { ctm } => {
                let val = ctm.eval2(global, local)?;

                if val.is_neutral() {
                    Ok(Either::Left(CoreReadParameterData::Unevaluated {
                        ctm: ctm,
                    }))
                } else {
                    match val {
                        Val::List { v } => Ok(Either::Right(v)),
                        _ => Err(EvalError::new(
                            location,
                            "match parameter was not a list value?!",
                        )),
                    }
                }
            }
            CoreReadParameterData::Evaluated { vals } => Ok(Either::Right(vals.concretise())),
        }
    }

    pub fn get_list_or_neutral<'r>(&'r self) -> Option<ListVal<'r>>
    where
        'p: 'r,
    {
        match self {
            CoreReadParameterData::Unevaluated { ctm } => None,
            CoreReadParameterData::Evaluated { vals, .. } => Some(vals.reference()),
        }
    }

    // fn eval(&self) -> ListVal<'p> {
    //     match self {
    //         CoreReadParameterData::Unevaluated { tm } => ,
    //         CoreReadParameterData::Evaluated { vals } => vals.reference(),
    //     }
    // }

    pub fn reference<'r>(&'r self) -> CoreReadParameterData<'r>
    where
        'p: 'r,
    {
        match self {
            CoreReadParameterData::Unevaluated { ctm: tm } => self.clone(),
            CoreReadParameterData::Evaluated { vals } => CoreReadParameterData::Evaluated {
                vals: vals.reference(),
            },
        }
    }
}

struct ParamTable<'a> {
    vec: Vec<(String, core::Tm, Option<ListVal<'a>>)>,
}

impl<'a> ParamTable<'a> {
    fn new(c0: Context<'a>, params: &[ReadParameter]) -> (Option<ParamTable<'a>>, Context<'a>) {
        // elaborate each read parameter
        let mut cx = c0.reference();
        let mut v = vec![];
        for param in params {
            let (ctm, ty, cy) = param.data.tm.infer(&cx);
            cx = cy;

            // extract the inner type;
            let inner_ty = match ty {
                Val::ListTy { ty } => ty.reference(),
                _ => {
                    return (
                        None,
                        cx.report(ElabError::new(
                            &param.location,
                            &format!(
                                "parameter {} must be selecting from a List",
                                param.data.name
                            ),
                        )),
                    )
                }
            };

            // try to evaluate
            let owned_ctm = ctm.clone();

            let val = match owned_ctm.eval2(&cx.get_global_env(), &cx.get_local_env()) {
                Ok(val) => val.concretise(),
                Err(_) => return (None, cx),
            };

            if val.is_neutral() {
                v.push((param.data.name.clone(), ctm.clone(), None));
            } else {
                match &val {
                    Val::List { v: list_val } => {
                        v.push((param.data.name.clone(), owned_ctm, Some(val.clone())));
                    }
                    _ => return (None, cx),
                }
            }
        }

        todo!()
    }
}

impl<'p> ReadMatcher<'p> {
    pub fn infer(
        c0: &Context<'p>,
        regions: &[Region],
        params: &[ReadParameter],
        mode: &MatchMode,
        location: &Location,
    ) -> (Either<ReadMatcher<'p>, ErrorMatcher>, Context<'p>) {
        // first, separate all the regions out into
        // - a vec of hole/tm
        // - the list of named, sized, etc with location ranges

        // flatten regions into:
        // - a list of sizes and their spans within the pattern
        // - a list of names and their spans within the pattern
        // - a list of Regs, which are either holes or as-yet-unresolved terms
        let (sized, named, regs): (
            Vec<Ranged<surface::Tm>>,
            Vec<Ranged<String>>,
            Vec<Ranged<FlatRegion>>,
        ) = {
            let r = flatten(regions);

            (r.sized, r.named, r.regions)
        };

        // go through the params, and typecheck them,
        // building a context with all params in
        let (params_ctx, c1): (_, Context<'p>) = {
            let mut cx = c0.reference();

            let mut params_ctx = ParamContext::new();

            for (index, param) in params.iter().enumerate() {
                let (ctm, ty, cy) = param.data.tm.infer(&cx);
                cx = cy;

                // extract the inner type;
                let inner_ty = match ty {
                    Val::ListTy { ty } => ty.reference(),
                    _ => {
                        return (
                            Either::Right(ErrorMatcher::new(location)),
                            cx.report(ElabError::new(
                                location,
                                "parameter did not have list type?!",
                            )),
                        )
                    }
                };

                // try to evaluate the parameter
                let (val, cy) = match ctm.eval2(&cx.get_global_env(), &cx.get_local_env()) {
                    Ok(val) => (val.concretise(), cx),
                    Err(e) => (Val::Error, cx.report(ElabError::from_eval_error(e))),
                };
                cx = cy;

                if val.is_neutral() {
                    params_ctx = params_ctx.with(
                        &param.data.name,
                        ParamData::new_unevaluated(index, ctm.clone(), inner_ty),
                    )
                } else {
                    match val {
                        Val::List { v } => {
                            params_ctx = params_ctx.with(
                                &param.data.name,
                                ParamData::new_evaluated(index, v, inner_ty),
                            );
                        }
                        _ => {
                            return (
                                Either::Right(ErrorMatcher::new(location)),
                                cx.report(ElabError::new(
                                    location,
                                    "parameter did not have list type despite typechecking?!",
                                )),
                            )
                        }
                    }
                }
            }

            (params_ctx, cx)
        };

        // go through the sized, and typecheck them,
        // making sure that each is a number,
        // and evaluating only where possible
        let (sized_ctm, c2): (Vec<Ranged<core::Tm>>, Context<'p>) = {
            let mut cx = c1;
            let mut sized_ctm = vec![];

            for ranged_tm in &sized {
                let (ctm, cy) = ranged_tm.data.check(&cx, &Val::NumTy);
                cx = cy;

                sized_ctm.push(Ranged {
                    data: ctm,
                    ran: ranged_tm.ran.clone(),
                });
            }

            (sized_ctm, cx)
        };

        // now go through each term, and if all the params are already evaluated, then build ctxs and try to evaluate the tm and the error_tm
        let (core_regs, c4): (Vec<Ranged<CoreFlatRegion>>, Context<'p>) = {
            let mut core_regs = vec![];
            let mut cx = c2.reference();

            for reg in &regs {
                let core_reg = match &reg.data {
                    FlatRegion::Hole => Ranged::new(CoreFlatRegion::new_hole(), reg.ran.clone()),
                    FlatRegion::Tm { tm, error_tm } => {
                        // first, get the list of parameters that need to be bound in this region
                        // get the ids from the term that are also in the ids of the params
                        let param_names = params
                            .iter()
                            .map(|param| param.data.name.clone())
                            .collect::<Vec<_>>();

                        let param_names_in_tm = tm
                            .get_ids()
                            .iter()
                            .chain(error_tm.get_ids().iter())
                            .filter(|name| param_names.contains(name))
                            .unique()
                            .cloned()
                            .collect::<Vec<_>>();

                        let params = param_names_in_tm
                            .iter()
                            .flat_map(|name| Some((name.clone(), params_ctx.get(name)?)))
                            .collect::<Vec<_>>();

                        // build a context with all of these bound in order
                        let c3 = params
                            .iter()
                            .sorted_by_key(|(_, param)| param.get_index())
                            .fold(c2.reference(), |c0, (name, param)| {
                                c0.bind_param(name, param.get_ty())
                            });
                        // elaborate the terms
                        let (ctm, tm_ctx) = tm.check(&c3, &Val::StrTy);
                        let (error_ctm, error_tm_ctx) = error_tm.check(&tm_ctx, &Val::StrTy);
                        cx = cx.with_global_from(tm_ctx).with_global_from(error_tm_ctx);

                        if let Some(a) = params
                            .iter()
                            .map(|(_, param)| match param {
                                ParamData::Unevaluated { .. } => None,
                                ParamData::Evaluated { index, vals, ty } => {
                                    Some((*index, vals.concretise()))
                                }
                            })
                            .collect::<Option<Vec<_>>>()
                        {
                            // build all the contexts

                            // also, build an env we can evaluate in
                            let bind_ctxs = core_read_params_to_bind_ctxs(&a);

                            let mut all_tm_evaluated = true;
                            let mut all_error_tm_evaluated = true;

                            let mut vals = vec![];
                            let mut error_vals = vec![];
                            for bind_ctx in &bind_ctxs {
                                let local = bind_ctx.to_env(&cx.get_local_env());

                                if all_tm_evaluated {
                                    let val = match ctm.eval2(&cx.get_global_env(), &local) {
                                        Ok(val) => val.concretise(),
                                        Err(e) => {
                                            return (
                                                Either::Right(ErrorMatcher::new(location)),
                                                cx.report(ElabError::from_eval_error(e)),
                                            )
                                        }
                                    };

                                    if val.is_neutral() {
                                        all_tm_evaluated = false;
                                    } else {
                                        match val {
                                            Val::Str { s } => vals.push(s),
                                            _ => return (Either::Right(ErrorMatcher::new(location)), cx.report(ElabError::new(location, "pattern tm had wrong type despite typechecking?!")))
                                        }
                                    }
                                }

                                if all_error_tm_evaluated {
                                    let error_val =
                                        match error_ctm.eval2(&cx.get_global_env(), &local) {
                                            Ok(val) => val.concretise(),
                                            Err(e) => {
                                                return (
                                                    Either::Right(ErrorMatcher::new(location)),
                                                    cx.report(ElabError::from_eval_error(e)),
                                                )
                                            }
                                        };

                                    if error_val.is_neutral() {
                                        all_error_tm_evaluated = false;
                                    } else {
                                        match error_val {
                                            Val::Num { n } => error_vals.push(n),
                                            _ => return (Either::Right(ErrorMatcher::new(location)), cx.report(ElabError::new(location, "pattern error tm had wrong type despite typechecking?!")))
                                        }
                                    }
                                }
                            }

                            match (all_tm_evaluated, all_error_tm_evaluated) {
                                (true, true) => Ranged::new(
                                    CoreFlatRegion::new_tm(OpData::new_evaluated(
                                        &bind_ctxs
                                            .iter()
                                            .zip(vals)
                                            .zip(error_vals)
                                            .map(|((bind_ctx, str_val), error)| {
                                                (
                                                    bind_ctx.reference(),
                                                    Seq::new(str_val.get_reference(), error),
                                                )
                                            })
                                            .collect::<Vec<_>>()[..],
                                    )),
                                    reg.ran.clone(),
                                ),
                                (true, false) => Ranged::new(
                                    CoreFlatRegion::new_tm(OpData::new_partial_b(
                                        &bind_ctxs
                                            .iter()
                                            .zip(vals)
                                            .map(|(bind_ctx, str_val)| {
                                                (bind_ctx.reference(), str_val)
                                            })
                                            .collect::<Vec<_>>()[..],
                                        error_ctm.clone(),
                                    )),
                                    reg.ran.clone(),
                                ),
                                (false, true) => Ranged::new(
                                    CoreFlatRegion::new_tm(OpData::new_partial_c(
                                        &bind_ctxs
                                            .iter()
                                            .zip(error_vals)
                                            .map(|(bind_ctx, error_val)| {
                                                (bind_ctx.reference(), error_val)
                                            })
                                            .collect::<Vec<_>>()[..],
                                        ctm.clone(),
                                    )),
                                    reg.ran.clone(),
                                ),
                                (false, false) => Ranged::new(
                                    CoreFlatRegion::new_tm(OpData::new_partial_a(
                                        &bind_ctxs,
                                        ctm.clone(),
                                        error_ctm.clone(),
                                    )),
                                    reg.ran.clone(),
                                ),
                            }
                        } else {
                            // can't evaluate
                            Ranged::new(
                                CoreFlatRegion::new_tm(OpData::new_unevaluated(
                                    &params
                                        .iter()
                                        .map(|(_, param)| param.clone())
                                        .collect::<Vec<_>>()[..],
                                    ctm,
                                    error_ctm,
                                )),
                                reg.ran.clone(),
                            )
                        }
                    }
                };

                core_regs.push(core_reg);
            }

            (core_regs, cx)
        };

        // now we have a list of partially evaluated term forms which can live for 'p and we will just write an eval function which is a normal eval function

        let bindable_regs: Vec<Ranged<OpData<'_>>> = core_regs
            .iter()
            .flat_map(|ranged| match &ranged.data {
                CoreFlatRegion::Hole => None,
                CoreFlatRegion::Tm { data } => Some(Ranged {
                    data: data.concretise(),
                    ran: ranged.ran.clone(),
                }),
            })
            // sort by priority!
            // NOTE this is where we could, if we wanted to, generate a whole bunch of priority orderings,
            // and compare them against each other...
            .sorted_by_key(|ranged| ranged.data.get_priority())
            .collect();

        fn check_all_fixed_lens<'p>(
            ops: &[Op<'p>],
            known: &[usize],
            sized: &[Ranged<core::Tm>],
        ) -> Result<(Vec<usize>, Vec<Op<'p>>), InternalError> {
            fn learn_new_fixed_lens<'p>(
                known: &mut Vec<usize>,
                sized: &[Ranged<core::Tm>],
            ) -> Result<Vec<Op<'p>>, InternalError> {
                let mut ops = vec![];

                for a in sized {
                    let flanking = get_tightest_known(known, &a.ran)?;

                    match a.ran.map(|l| known.contains(l)).to_tuple() {
                        // TODO insert something that actually verifies length
                        (true, true) => {}

                        // if one end is known, insert a let operation
                        (true, false) => {
                            ops.push(Op::Let {
                                loc: a.ran.end,
                                op: LocOp::Offset {
                                    loc_op: Arc::new(LocOp::Var { loc: a.ran.start }),
                                    offset: a.data.clone(),
                                    direction: Direction::Plus,
                                },
                                flanking,
                            });
                            known.push(a.ran.start);
                        }
                        (false, true) => {
                            ops.push(Op::Let {
                                loc: a.ran.start,
                                op: LocOp::Offset {
                                    loc_op: Arc::new(LocOp::Var { loc: a.ran.end }),
                                    offset: a.data.clone(),
                                    direction: Direction::Minus,
                                },
                                flanking,
                            });
                            known.push(a.ran.start);
                        }

                        // can't get here
                        (false, false) => {
                            return Err(InternalError::new(
                                "trying to bind a region unbounded on both sides?!",
                            ))
                        }
                    }
                }

                Ok(ops)
            }

            let mut ops1 = ops.iter().map(Op::concretise).collect::<Vec<_>>();
            let mut known1 = known.to_vec();
            let mut previous_len = ops1.len();

            ops1.extend(learn_new_fixed_lens(&mut known1, sized)?);
            // find the least fixed point
            while !ops1.len().eq(&previous_len) {
                // update the old ops
                previous_len = ops1.len();
                // extend the new ops
                ops1.extend(learn_new_fixed_lens(&mut known1, sized)?);
            }

            Ok((known1, ops1))
        }

        fn get_tightest_known(
            known: &[usize],
            ran: &Ran<usize>,
        ) -> Result<Ran<usize>, InternalError> {
            if let (Some(start), Some(end)) = (
                known.iter().filter(|loc| **loc <= ran.start).max(),
                known.iter().filter(|loc| **loc >= ran.end).min(),
            ) {
                Ok(Ran::new(*start, *end))
            } else {
                Err(InternalError::new("didn't know the necessary locations?!"))
            }
        }

        let mut known = vec![0, core_regs.len()];
        let mut ops: Vec<Op<'_>> = vec![];

        //

        // now, we go through each of the bindable regions
        for op_data in &bindable_regs {
            let (known1, ops1) = match check_all_fixed_lens(&ops, &known, &sized_ctm) {
                Ok(data) => data,
                Err(e) => return (Either::Right(ErrorMatcher::new(location)), c4),
            };

            known = known1;
            ops = ops1;

            let search: Ran<usize> = match get_tightest_known(&known, &op_data.ran) {
                Ok(ran) => ran,
                Err(e) => return (Either::Right(ErrorMatcher::new(location)), c4),
            };

            let fixed = Ran::new(
                search.start.eq(&op_data.ran.start),
                search.end.eq(&op_data.ran.end),
            );

            ops.push(Op::Restrict {
                ran: search,
                fixed,
                save: vec![op_data.ran.clone()],
                data: op_data.data.reference(),
            });

            known.push(op_data.ran.start);
            known.push(op_data.ran.end);
        }

        // check for range restriction one final time
        let (known, ops) = match check_all_fixed_lens(&ops, &known, &sized_ctm) {
            Ok(data) => data,
            Err(e) => return (Either::Right(ErrorMatcher::new(location)), c4),
        };

        (
            Either::Left(ReadMatcher {
                ops: ops.iter().map(Op::concretise).collect(),
                binds: named.iter().map(|a| a.ran.clone()).collect(),
                end: regs.len(),
                mode: mode.elab(),
                location: location.clone(),
            }),
            c4,
        )
    }
}

#[derive(Clone)]
struct Ranged<T> {
    data: T,
    ran: Ran<usize>,
}

impl<T> Ranged<T> {
    fn new(data: T, ran: Ran<usize>) -> Ranged<T> {
        Ranged { data, ran }
    }
}

struct FlattenResult {
    sized: Vec<Ranged<crate::util::location::Located<surface::TmData>>>,
    named: Vec<Ranged<String>>,
    regions: Vec<Ranged<FlatRegion>>,
}

fn flatten(regions: &[Region]) -> FlattenResult {
    #[derive(Clone, Default)]
    struct Accumulator {
        i: usize,
        sized: Vec<Ranged<surface::Tm>>,
        named: Vec<Ranged<String>>,
        regions: Vec<Ranged<FlatRegion>>,
    }

    impl Accumulator {
        fn with_hole(&self, loc: usize) -> Accumulator {
            todo!()
        }

        fn with_named(&self, name: &str, loc: usize, length: usize) -> Accumulator {
            todo!()
        }

        fn with_sized(&self, size: surface::Tm, loc: usize, length: usize) -> Accumulator {
            todo!()
        }

        fn with_term(&self, tm: surface::Tm, error: surface::Tm, loc: usize) -> Accumulator {
            todo!()
        }
    }

    fn flatten_recursive(regs: &[Region], acc: &Accumulator) -> Accumulator {
        match &regs[..] {
            [] => acc.clone(),
            [first, rest @ ..] => {
                let first_acc = match &first.data {
                    RegionData::Hole => acc.with_hole(acc.i),
                    RegionData::Term { tm, error } => {
                        acc.with_term(tm.clone(), error.clone(), acc.i)
                    }
                    RegionData::Named { name, regs } => {
                        flatten_recursive(regs, acc).with_named(name, acc.i, get_length(regs))
                    }
                    RegionData::Sized { tm, regs } => {
                        flatten_recursive(regs, acc).with_sized(tm.clone(), acc.i, get_length(regs))
                    }
                };

                // then, thread the accumulator through
                flatten_recursive(rest, &first_acc)
            }
        }
    }

    let final_acc = flatten_recursive(regions, &Accumulator::default());

    FlattenResult {
        sized: final_acc.sized,
        named: final_acc.named,
        regions: final_acc.regions,
    }
}

fn get_length(regs: &[Region]) -> usize {
    fn get_length_recursive(regs: &[Region], i: usize) -> usize {
        match &regs[..] {
            // on empty list, just return out
            [] => i,
            [first, rest @ ..] => {
                let first_len = match &first.data {
                    RegionData::Hole | RegionData::Term { tm: _, error: _ } => 1,
                    RegionData::Named { name: _, regs } | RegionData::Sized { tm: _, regs } => {
                        get_length(&regs[..])
                    }
                };

                // where we're up to, plus the length of the contents of this region, plus the length of the rest of the regions
                i + first_len + get_length(rest)
            }
        }
    }

    get_length_recursive(regs, 0)
}

pub fn core_read_params_to_bind_ctxs<'p>(
    list_vals: &[(usize, ListVal<'p>)],
) -> Vec<BindContext<'p>> {
    let mut ctxs = vec![BindContext::default()];
    for (index, list_val) in list_vals {
        let mut new_ctxs = vec![];
        for ctx in &ctxs {
            for val in list_val.get_reference() {
                let new_ctx = ctx.with(*index, val.reference());
                new_ctxs.push(new_ctx);
            }
        }

        ctxs = new_ctxs;
    }

    ctxs
}
