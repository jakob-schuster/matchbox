use crate::core::matcher::read_matcher2::eval2::match_context::MatchContext;
use crate::util::location::Location;

use crate::core::matcher::read_matcher::Seq;

use crate::core::EvalError;

use crate::core::matcher::{read_matcher2::*, Matcher};

impl<'p> Matcher<'p> for ReadMatcher<'p> {
    fn evaluate<'a>(
        &self,
        arena: &'a crate::util::Arena,
        env: &crate::util::env::Env<Val<'a>>,
        val: &Val<'a>,
    ) -> Result<Vec<Vec<Val<'a>>>, EvalError>
    where
        'p: 'a,
    {
        todo!()
    }

    fn eval2<'r, 'a>(
        &'p self,
        global: &'p env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        val: &Val<'r>,
    ) -> Result<Vec<env::Env<Val<'r>>>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        if let Val::Rec { rec } = val {
            if let Val::Str { s: seq } = rec
                .get(b"seq")
                .map_err(|e| EvalError::from_internal(e, self.location.clone()))?
            {
                let ctx = MatchContext::default()
                    .with_loc(0, 0)
                    .with_loc(self.end, seq.get_reference().len() as i32);

                let worlds: Vec<MatchContext<'r>> =
                    self.ops.iter().fold(vec![ctx], |worlds, op| {
                        worlds
                            .iter()
                            .flat_map(|match_context| {
                                op.exec(
                                    global,
                                    local,
                                    seq.get_reference(),
                                    &match_context,
                                    &self.location,
                                )
                            })
                            .flatten()
                            .collect::<Vec<MatchContext<'r>>>()
                    });

                // choose either the first world,
                // or all the possible worlds
                match self.mode {
                    MatchMode::All => {
                        let mut envs = vec![];
                        for c0 in worlds {
                            let mut vals = local.reference();

                            for (_, val) in c0.binds.map.iter().sorted_by_key(|(id, _)| **id) {
                                vals.push(val.reference());
                            }

                            for ran in self.binds.iter() {
                                let pos_ran = ran.try_map(|loc| c0.get_loc(loc)).map_err(|e| {
                                    EvalError::from_internal(e, self.location.clone())
                                })?;
                                let sliced = rec
                                    .slice(pos_ran.start as usize, pos_ran.end as usize)
                                    .map_err(|e| {
                                        EvalError::from_internal(e, self.location.clone())
                                    })?;

                                vals.push(sliced);
                            }

                            envs.push(vals);
                        }

                        Ok(envs)
                    }
                    MatchMode::One => {
                        if let Some(c0) = worlds.iter().sorted_by_key(|c0| c0.edit_dist).next() {
                            let mut vals = local.reference();

                            for (_, val) in c0.binds.map.iter().sorted_by_key(|(id, _)| **id) {
                                vals.push(val.reference());
                            }

                            for ran in self.binds.iter() {
                                let pos_ran = ran.try_map(|loc| c0.get_loc(loc)).map_err(|e| {
                                    EvalError::from_internal(e, self.location.clone())
                                })?;
                                let sliced = rec
                                    .slice(pos_ran.start as usize, pos_ran.end as usize)
                                    .map_err(|e| {
                                        EvalError::from_internal(e, self.location.clone())
                                    })?;

                                vals.push(sliced);
                            }

                            Ok(vec![vals])
                        } else {
                            Ok(vec![])
                        }
                    }
                    MatchMode::Unique => {
                        if let Some(best_dist) = worlds.iter().map(|c0| c0.edit_dist).min() {
                            let all_best = worlds
                                .iter()
                                .filter(|c0| c0.edit_dist.eq(&best_dist))
                                .collect_vec();

                            match &all_best[..] {
                                [c0] => {
                                    let mut vals = local.reference();

                                    for (_, val) in
                                        c0.binds.map.iter().sorted_by_key(|(id, _)| **id)
                                    {
                                        vals.push(val.reference());
                                    }

                                    for ran in self.binds.iter() {
                                        let pos_ran =
                                            ran.try_map(|loc| c0.get_loc(loc)).map_err(|e| {
                                                EvalError::from_internal(e, self.location.clone())
                                            })?;
                                        let sliced = rec
                                            .slice(pos_ran.start as usize, pos_ran.end as usize)
                                            .map_err(|e| {
                                                EvalError::from_internal(e, self.location.clone())
                                            })?;

                                        vals.push(sliced);
                                    }

                                    Ok(vec![vals])
                                }
                                _ => Ok(vec![]),
                            }
                        } else {
                            Ok(vec![])
                        }
                    }
                }
            } else {
                Err(EvalError::from_internal(
                    InternalError::new(&format!("gave non-read value {} to read matcher?!", val)),
                    self.location.clone(),
                ))
            }
        } else {
            Err(EvalError::from_internal(
                InternalError::new(&format!("gave non-record value {} to read matcher?!", val)),
                self.location.clone(),
            ))
        }
    }
}

impl<'p> A<'p> {
    pub fn eval<'r: 'a, 'a>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        location: &Location,
    ) -> Result<&Vec<(BindContext<'p>, Seq)>, EvalError> {
        todo!()
    }

    pub fn partial_eval<'r, 'a>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        location: &Location,
    ) -> Result<A<'r>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match self {
            A::Unevaluated { params, tm, error } => todo!(),
            // {
            //     let mut new_params: Vec<Either<&'p core::Tm, ListVal<'r>>> = vec![];
            //     for param in params {
            //         match param {
            //             Either::Left(tm) => {
            //                 let val: Val<'r> = tm.eval2(global, local)?;

            //                 if val.is_neutral() {
            //                     new_params.push(Either::Left(tm));
            //                 } else {
            //                     match val {
            //                         Val::List { v } => {
            //                             let lv: ListVal<'r> = v;
            //                             new_params.push(Either::Right(lv));
            //                         }
            //                         _ => {
            //                             return Err(EvalError::new(
            //                                 location,
            //                                 "match parameter was not a list value?!",
            //                             ))
            //                         }
            //                     }
            //                 }
            //             }
            //             Either::Right(list_val) => {
            //                 new_params.push(Either::Right(list_val.reference()))
            //             }
            //         }
            //     }

            //     if new_params.iter().all(|a| a.is_right()) {
            //         let mut list_vals = vec![];
            //         for param in new_params {
            //             if let Either::Right(list_val) = param {
            //                 list_vals.push(list_val);
            //             } else {
            //                 return Err(EvalError::new(location, "message"));
            //             }
            //         }

            //         let ctxs = core_read_params_to_ctxs(&list_vals);
            //         Ok(A::PartialParams { ctxs, tm, error })
            //     } else {
            //         Ok(A::Unevaluated {
            //             params: new_params,
            //             tm,
            //             error,
            //         })
            //     }
            // }
            A::PartialParams { ctxs, tm, error } => {
                let val = tm.eval2(global, local)?;
                let error_val = tm.eval2(global, local)?;

                match (val.is_neutral(), error_val.is_neutral()) {
                    // can't do anything
                    (true, true) => Ok(A::PartialParams {
                        ctxs: ctxs.iter().map(|ctx| ctx.reference()).collect(),
                        tm: tm.clone(),
                        error: error.clone(),
                    }),
                    // simplify out the error
                    (true, false) => {
                        let new_ctxs = ctxs.iter().map(|ctx| );
                        
                        Ok(A::PartialError {
                        ctxs: new_ctxs,
                        tm: tm.clone(),
                    })},
                    // simplify out the
                    (false, true) => {
                        let s = match val {
                            Val::Str { s } => s,
                            _ => return Err(EvalError::new(location, "term was not a Str?!")),
                        };

                        Ok(A::PartialTm {
                            ctxs: ctxs.iter().map(|ctx| (ctx, )),
                            error: (),
                        })
                    }
                    (false, false) => todo!(),
                }
            }
            A::PartialTm { ctxs, error } => todo!(),
            A::PartialError { ctxs, tm, error } => todo!(),
            A::Evaluated { ctxs } => todo!(),
        }
    }
}

fn core_read_params_to_ctxs<'p>(list_vals: &[ListVal<'p>]) -> Vec<env::Env<Val<'p>>> {
    let mut ctxs = vec![env::Env::default()];
    for list_val in list_vals {
        let mut new_ctxs = vec![];
        for ctx in &ctxs {
            for val in list_val.get_reference() {
                let new_ctx = ctx.with(val.reference());
                new_ctxs.push(new_ctx);
            }
        }

        ctxs = new_ctxs;
    }

    ctxs
}

pub mod match_context {
    use crate::core::{
        matcher::read_matcher2::eval2::match_context::{
            bind_context::BindContext, location_context::LocationContext,
        },
        InternalError, Val,
    };

    #[derive(Default)]
    pub struct MatchContext<'p> {
        pub locs: LocationContext,
        pub binds: BindContext<'p>,
        pub edit_dist: u8,
    }

    impl<'p> MatchContext<'p> {
        pub fn new(
            locs: LocationContext,
            binds: BindContext<'p>,
            edit_dist: u8,
        ) -> MatchContext<'p> {
            MatchContext {
                locs,
                binds,
                edit_dist,
            }
        }

        pub fn get_loc(&self, loc: &usize) -> Result<i32, InternalError> {
            self.locs.get(loc)
        }

        pub fn get_bind(&self, id: &usize) -> Result<&Val<'p>, InternalError> {
            self.binds.get(id)
        }

        pub fn with_loc(&self, loc: usize, pos: i32) -> MatchContext<'p> {
            MatchContext {
                locs: self.locs.with(loc, pos),
                binds: self.binds.reference(),
                edit_dist: self.edit_dist,
            }
        }

        pub fn with_bind(&self, id: usize, val: Val<'p>) -> MatchContext<'p> {
            MatchContext {
                locs: self.locs.clone(),
                binds: self.binds.with(id, val),
                edit_dist: self.edit_dist,
            }
        }

        pub fn with_edit_dist(&self, edit_dist: u8) -> MatchContext<'p> {
            MatchContext {
                locs: self.locs.clone(),
                binds: self.binds.reference(),
                edit_dist: edit_dist,
            }
        }

        pub fn reference(&self) -> MatchContext<'p> {
            MatchContext {
                locs: self.locs.clone(),
                binds: self.binds.reference(),
                edit_dist: self.edit_dist,
            }
        }
    }

    pub mod location_context {
        use crate::core::InternalError;

        use std::collections::HashMap;

        #[derive(Clone, Default)]
        pub struct LocationContext {
            pub map: HashMap<usize, i32>,
        }

        impl LocationContext {
            pub fn get(&self, loc: &usize) -> Result<i32, InternalError> {
                match self.map.get(loc) {
                    Some(pos) => Ok(*pos),
                    None => Err(InternalError::new(&format!(
                        "couldn't find location {}?!",
                        *loc
                    ))),
                }
            }

            pub fn with(&self, loc: usize, pos: i32) -> LocationContext {
                let mut map = self.map.clone();
                map.insert(loc, pos);
                LocationContext { map }
            }
        }
    }

    pub mod bind_context {
        use crate::core::InternalError;

        use crate::core::Val;

        use std::collections::HashMap;

        #[derive(Clone, Default)]
        pub struct BindContext<'p> {
            pub map: HashMap<usize, Val<'p>>,
        }

        impl<'p> BindContext<'p> {
            pub fn get(&self, id: &usize) -> Result<&Val<'p>, InternalError> {
                match self.map.get(id) {
                    Some(val) => Ok(val),
                    None => Err(InternalError::new(&format!("couldn't find index {}?!", id))),
                }
            }

            pub fn with<'a>(&self, id: usize, val: Val<'a>) -> BindContext<'a>
            where
                'p: 'a,
            {
                let mut map = self
                    .map
                    .iter()
                    .map(|(id, val)| (*id, val.reference()))
                    .collect::<HashMap<usize, Val<'p>>>();
                map.insert(id, val);

                BindContext { map }
            }

            pub fn reference(&self) -> BindContext<'p> {
                BindContext {
                    map: self
                        .map
                        .iter()
                        .map(|(id, val)| (*id, val.reference()))
                        .collect(),
                }
            }

            pub fn iter(&self) -> std::collections::hash_map::Iter<'_, usize, Val<'_>> {
                self.map.iter()
            }
        }
    }
}

impl<'p> Op<'p> {
    pub fn exec<'a, 'r>(
        &'p self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        seq: &[u8],
        c0: &MatchContext<'p>,
        location: &Location,
    ) -> Result<Vec<MatchContext<'p>>, EvalError>
    where
        'p: 'r,
        'r: 'a,
    {
        match self {
            Op::Let { loc, op, flanking } => {
                let pos = op.eval(global, local, &c0.locs, location)?;
                let flanking_pos = flanking
                    .try_map(|loc| c0.get_loc(loc))
                    .map_err(|e| EvalError::from_internal(e, location.clone()))?;

                // return out if the range is inappropriate
                if pos > flanking_pos.start && pos < flanking_pos.end {
                    Ok(vec![c0.with_loc(*loc, pos)])
                } else {
                    Ok(vec![])
                }
            }
            Op::Restrict {
                ran,
                fixed,
                save,
                a,
            } => {
                // we must be able to concretise A at this point; otherwise, we're in trouble
                let ctxs = a.eval(global, local, location)?;

                let filtered = ctxs.iter().filter(|(bs, _)| {
                    bs.iter().all(|(id, val)| {
                        if let Ok(val2) = c0.get_bind(id) {
                            // if it's in both, they need to be equal in their value
                            (*val).eq(val2)
                        } else {
                            // if it's only in one, that's fine
                            true
                        }
                    })
                });

                let mut accumulated = vec![];
                for (local_binds, tm_seq) in filtered {
                    let matches = tm_seq.find_all_disjoint(
                        seq,
                        &ran.try_map(|i| c0.get_loc(i).map(|i| i as usize))
                            .map_err(|e| EvalError::from_internal(e, location.clone()))?,
                        fixed,
                    );

                    // if we have found the pattern sufficient times
                    if matches.len() >= save.len() {
                        let combinations = matches
                            .iter()
                            .combinations(save.len())
                            .map(|c| MatchContext {
                                binds: local_binds.iter().fold(
                                    c0.binds.reference(),
                                    |old_bind_ctx, (id, val)| {
                                        old_bind_ctx.with(*id, val.reference())
                                    },
                                ),
                                locs: c.iter().enumerate().fold(
                                    c0.locs.clone(),
                                    |old_loc_ctx, (i, mat)| {
                                        old_loc_ctx
                                            .with(save.get(i).unwrap().start, mat.ran.start as i32)
                                            .with(save.get(i).unwrap().end, mat.ran.end as i32)
                                    },
                                ),
                                edit_dist: c.iter().fold(0, |acc, mat| acc + mat.dist) as u8,
                            })
                            .collect::<Vec<_>>();

                        // return all matching patterns
                        for v in combinations {
                            accumulated.push(v);
                        }
                    }
                }

                if !accumulated.is_empty() {
                    let best_dist = match accumulated.iter().map(|c| c.edit_dist).min() {
                        Some(dist) => Ok(dist),
                        None => Err(EvalError::from_internal(
                            InternalError::new(
                                "could not find a best distance, yet accumulated was not empty?!",
                            ),
                            location.clone(),
                        )),
                    }?;
                    let accumulated_adjusted = accumulated
                        .iter()
                        .filter(|c| c.edit_dist == best_dist)
                        .map(|c| c.with_edit_dist(c.edit_dist + c0.edit_dist))
                        .collect();

                    Ok(accumulated_adjusted)
                } else {
                    Ok(vec![])
                }
            }
        }
    }
}

impl LocOp {
    pub fn eval<'p: 'r, 'r: 'a, 'a>(
        &self,
        global: &'a env::Env<Val<'p>>,
        local: &'a env::Env<Val<'r>>,
        locs: &LocationContext,
        location: &Location,
    ) -> Result<i32, EvalError> {
        match self {
            LocOp::Var { loc } => locs
                .get(loc)
                .map_err(|e| EvalError::from_internal(e, location.clone())),
            LocOp::Offset {
                loc_op,
                offset,
                direction,
            } => match offset.eval2(global, local)? {
                Val::Num { n } => {
                    let i = n.round() as i32;

                    Ok(loc_op.eval(global, local, locs, location)? + (i * direction.get()))
                }
                _ => Err(EvalError::from_internal(
                    InternalError::new("sized read pattern wasn't numeric type?!"),
                    location.clone(),
                )),
            },
        }
    }
}
