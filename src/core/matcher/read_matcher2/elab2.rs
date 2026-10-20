use std::sync::Arc;

use itertools::{Either, Itertools};

use crate::{
    core::{
        self, EvalError, InternalError, ListVal, Val, eval2::env, matcher::{
            ErrorMatcher, read_matcher2::{A, Direction, LocOp, Op, ReadMatcher},
        },
    }, surface::{
        self, MatchMode, ReadParameter, Region, RegionData, elab::ElabError, elab2::context::{Address, Context},
    }, util::{location::Location, ran::Ran},
};

#[derive(Clone)]
enum FlatRegion {
    Hole,
    Tm {
        // the ids that appear in the term that are also in the param binds
        ids: Vec<String>,
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
    Tm { a: A<'p> },
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
            CoreReadParameterData::Evaluated { vals } => Ok(Either::Right(vals)),
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

struct ParamTable<'p> {
    vec: Vec<(String, ParamOption<'p>)>
}

impl<'p> ParamTable<'p> {
    fn new(c0: Context<'p>, params: &[ReadParameter]) -> (Option<ParamTable<'p>>, Context<'p>) {
        // elaborate each read parameter
        let mut cx = c0.reference();
        let mut v = vec![];
        for param in params {
            let (ctm, ty, cy) = param.data.tm.infer(&cx);
            cx = cy;

            // extract the inner type;
            let inner_ty = match ty {
                Val::ListTy { ty } => ty.reference(),
                _ => return (None, cx)
            };

            // try to evaluate
            let val = match ctm.eval2(&cx.get_global_env(), &cx.get_local_env()) {
                Ok(val) => val,
                Err(_) => return (None, cx),
            };

            if val.is_neutral() {
                v.push(ParamOption::Tm { tm: ctm.clone() });
            } else {
                v.push(ParamOption::Val { address: Address:: });
            }
        }

        todo!()
    }
}

enum ParamOption<'p> {
    Tm { tm: core::Tm },
    Val { address: Address }
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
        let (params_ctm, c1): (Vec<CoreReadParameter>, Context<'p>) = {
            let mut cx = c0.reference();
            let mut params_ctm = vec![];

            for param in params {
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

                params_ctm.push(CoreReadParameter {
                    name: param.data.name.clone(),
                    tm: ctm,
                    ty: inner_ty,
                });
            }

            (params_ctm, cx)
        };

        // then try and evaluate them if we can
        let mut v = vec![];
        for param_ctm in &params_ctm {
            let (val, c2) = match param_ctm.tm.eval2(&c1.get_global_env(), &c1.get_local_env()) {
                Ok(val) => (val, c1),
                Err(e) => (Val::Error, c1.report(ElabError::from_eval_error(e))),
            };
            
            if val.is_neutral() {
                v.push(Either::Left(param_ctm));
            } else {
                v.push(Either::Right((param_ctm.name.clone(), val, param_ctm.ty.reference())));
            }
        }

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

        // go through the FlatRegions and typecheck,
        // making sure that each is a string,
        // and evaluating only where possible
        // regs.iter()
        //     .fold((c2, vec![]), |(cx, v0), reg| match reg.data {
        //         FlatRegion::Hole => (cx.reference(), v0),
        //         FlatRegion::Tm { ids, tm, error_tm } => {
        //             todo!()
        //         }
        //     });
        let (core_regs, c3) = {
            let mut cx = c2.reference();
            let mut core_regs = vec![];
            // let mut regs_ctms = vec![];
            for ranged_reg in &regs {
                match &ranged_reg.data {
                    FlatRegion::Hole => {
                        core_regs.push(Ranged {
                            data: CoreFlatRegionA::Hole,
                            ran: ranged_reg.ran.clone(),
                        });
                    }
                    FlatRegion::Tm { ids, tm, error_tm } => {
                        let (c0_params, used_params) = {
                            // extract the parameter ids
                            let param_ids = tm
                                .get_ids()
                                .iter()
                                .merge(&error_tm.get_ids())
                                .unique()
                                .cloned()
                                .collect::<Vec<_>>();

                            let mut used_params = vec![];
                            for param in &params_ctm {
                                if param_ids.contains(&&param.name) {
                                    used_params.push(param.reference());
                                }
                            }

                            let c0_params = used_params.iter().fold(c0.reference(), |cx, param| {
                                cx.bind_param(&param.name, &param.ty)
                            });

                            (c0_params, used_params)
                        };

                        let (ctm, c1_params) = tm.check(&c0_params, &Val::StrTy);
                        let (error_ctm, c2_params) = tm.check(&c1_params, &Val::NumTy);

                        cx = cx.with_global_from(c2_params);
                        core_regs.push(Ranged::new(
                            CoreFlatRegionA::Tm {
                                params: used_params.iter().map(|p| p.tm.clone()).collect(),
                                tm: ctm.clone(),
                                error_tm: error_ctm.clone(),
                            },
                            ranged_reg.ran.clone(),
                        ));
                    }
                }
            }

            (core_regs, cx)
        };

        // now, go through and turn them into something
        let core_regs2 = {
            let mut core_regs2: Vec<Ranged<CoreFlatRegion<'_>>> = vec![];
            for reg in core_regs.iter() {
                let core_reg = match &reg.data {
                    CoreFlatRegionA::Hole => Ranged::new(CoreFlatRegion::Hole, reg.ran.clone()),
                    CoreFlatRegionA::Tm {
                        params,
                        tm,
                        error_tm,
                    } => {
                        
                        // first, eval all the params, binding them globally if we can
                        let a = {
                            let mut all = true;
                            let mut c_inner = c2.reference();
                            for param in params {
                                let (val, c2) = match param.eval2(&c1.get_global_env(), &c1.get_local_env()) {
                                    Ok(val) => (val, c1),
                                    Err(e) => (Val::Error, c1.report(ElabError::from_eval_error(e))),
                                };

                                if val.is_neutral() {
                                    // can't do anything -- return out early
                                    all = false;
                                } else {
                                    c_inner.bind(, , tm);
                                }
                            }

                            (true, c2)
                        }
                        
                        params.iter().map(|param| {
                            let (val, c2) = match param.eval2(&c1.get_global_env(), &c1.get_local_env()) {
                                Ok(val) => (val, c1),
                                Err(e) => (Val::Error, c1.report(ElabError::from_eval_error(e))),
                            };
                        });

                        // then, if all the params can be evalled, then eval the tm and error_tm

                        todo!();
                        
                        Ranged::new(
                        CoreFlatRegion::Tm {
                            a: A::new(params, tm, error_tm),
                        },
                        reg.ran.clone(),
                    ),}
                };

                core_regs2.push(core_reg);
            }

            core_regs2
        };

        let bindable_regs: Vec<Ranged<A<'_>>> = todo!()
            .iter()
            .flat_map(|ranged| match &ranged.data {
                CoreFlatRegion::Hole => None,
                CoreFlatRegion::Tm { a } => Some(Ranged {
                    data: a.reference(),
                    ran: ranged.ran.clone(),
                }),
            })
            // sort by priority!
            // NOTE this is where we could, if we wanted to, generate a whole bunch of priority orderings,
            // and compare them against each other...
            .sorted_by_key(|ranged| ranged.data.get_priority())
            .collect();

        fn check_all_fixed_lens<'p>(
            ops: &'p [Op<'p>],
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

            let mut ops1 = ops.iter().map(Op::reference).collect::<Vec<_>>();
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
        let mut ops: Vec<Op<'p>> = vec![];

        //

        // now, we go through each of the bindable regions
        for a in &bindable_regs {
            let (known, ops) = match check_all_fixed_lens(&ops, &known, &sized_ctm) {
                Ok(data) => data,
                Err(e) => return (Either::Right(ErrorMatcher::new(location)), c3),
            };

            let search: Ran<usize> = match get_tightest_known(&known, &a.ran) {
                Ok(ran) => ran,
                Err(e) => return (Either::Right(ErrorMatcher::new(location)), c3),
            };

            let fixed = Ran::new(search.start.eq(&a.ran.start), search.end.eq(&a.ran.end));

            ops.push(Op::Restrict {
                ran: search,
                fixed,
                save: vec![a.ran.clone()],
                a: a.data.reference(),
            });

            known.push(a.ran.start);
            known.push(a.ran.end);
        }

        // check for range restriction one final time
        let (known, ops) = match check_all_fixed_lens(&ops, &known, &sized_ctm) {
            Ok(data) => data,
            Err(e) => return (Either::Right(ErrorMatcher::new(location)), c3),
        };

        (
            Either::Left(ReadMatcher {
                ops,
                binds: named.iter().map(|a| a.ran.clone()).collect(),
                end: regs.len(),
                mode: mode.elab(),
                location: location.clone(),
            }),
            c2,
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
