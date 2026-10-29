use std::sync::Arc;

use itertools::Itertools;

use crate::core::eval2::env::Env;
use crate::core::matcher::read_matcher2::ReadMatcher;
use crate::core::matcher::Matcher;
use crate::core::{self, library, EvalError, FunData, StrVal, Val};
use crate::surface::elab::ElabError;
use crate::surface::elab2::context::Address;
use crate::surface::{elab2::context::Context, Tm};
use crate::surface::{
    Branch, BranchData, MatchMode, Pattern, PatternBranch, PatternData, Stmt, StmtData,
    StrLitRegion, StrLitRegionData, TmData,
};
use crate::util::bytes_to_string;
use crate::util::location::Location;
use crate::util::recfield::CoreRecField;

pub mod context {
    use std::sync::Arc;

    use crate::{
        core::{eval2::env::Env, Neutral, Val},
        surface::elab::ElabError,
    };

    #[derive(Clone)]
    pub struct Context<'p> {
        local: Env<Val<'p>>,

        global: Env<Val<'p>>,

        // errors reported during elaboration
        errors: Vec<ElabError>,
    }

    impl<'p> Context<'p> {
        pub fn new() -> Context<'p> {
            todo!()
        }

        /// Create a new reference,
        /// simply making references of any carried values
        pub fn reference(&self) -> Context<'p> {
            Context {
                local: self.local.reference(),
                global: self.global.reference(),
                errors: self.errors.clone(),
            }
        }

        pub fn report(&self, error: ElabError) -> Context<'p> {
            let mut errors = self.errors.clone();
            errors.push(error);

            todo!()
        }

        pub fn get_local_env(&self) -> Env<Val<'p>> {
            // make a reference, for efficiency
            self.local.reference()
        }

        pub fn get_global_env(&self) -> Env<Val<'p>> {
            // make a reference, for efficiency
            self.global.reference()
        }

        pub fn lookup(&self, name: &str) -> Option<(Address, &Val<'p>)> {
            todo!()
        }

        fn size(&self) -> usize {
            todo!()
        }

        fn next_neutral(&self) -> Val<'static> {
            Val::Neutral {
                neutral: Neutral::Var { level: self.size() },
            }
        }

        fn bind_local<'a>(&self, name: &str, ty: Val<'a>, tm: Val<'a>) -> Context<'p>
        where
            'p: 'a,
        {
            // and then add this to the locals and assign the address correctly

            todo!()
        }

        fn bind_global<'a, 'b>(&self, name: &str, ty: Val<'a>, tm: Val<'b>) -> Context<'b>
        where
            'p: 'a,
            'p: 'b,
        {
            let global1: Env<Val<'p>> = self.global.with(tm.concretise());

            // and then add this to the global and assign the address correctly

            Context {
                local: self.local.reference(),
                global: global1,
                errors: self.errors.clone(),
            }
        }

        pub fn bind<'a, 'b>(&self, name: &str, ty: Val<'a>, tm: Val<'b>) -> Context<'b>
        where
            'p: 'a,
            'p: 'b,
        {
            self.bind_global(name, ty, tm)
        }

        pub fn bind_param<'a>(&self, name: &str, ty: &Val<'a>) -> Context<'p>
        where
            'p: 'a,
        {
            self.bind_local(name, ty.concretise(), self.next_neutral())
        }

        /// Retain the local bindings, but carry the global bindings and errors from the new context
        pub fn with_global_from(&self, c1: Context<'p>) -> Context<'p> {
            Context {
                local: self.local.reference(),
                global: c1.global.reference(),
                errors: c1.errors.clone(),
            }
        }
    }

    pub enum Address {
        Global { index: usize },
        Local { index: usize },
    }
}

impl Stmt {
    fn elab<'p>(&self, c0: &Context<'p>, rest: &[Stmt]) -> (core::Stmt<'p>, Context<'p>) {
        match &self.data {
            StmtData::Group { stmts } => {
                // evaluate all the statements in the group;
                // then pull out the parts of the context that need to remain (global)
                // while discarding the parts that were specific to that group (local)

                let (cstmt, c1) = match &stmts[..] {
                    [] => (
                        core::Stmt::new(self.location.clone(), core::StmtData::End),
                        c0.reference(),
                    ),
                    [next, rest1 @ ..] => {
                        let (next_cstmt, c1) = next.elab(c0, rest1);

                        // this is where we drop the global bindings
                        (next_cstmt, c0.with_global_from(c1))
                    }
                };

                let (rest_cstmt, c2) = match &rest[..] {
                    [] => (
                        core::Stmt::new(self.location.clone(), core::StmtData::End),
                        c1,
                    ),
                    [first, rest1 @ ..] => first.elab(&c1, rest1),
                };

                // append the rest of the stmts on to the group
                (cstmt.append(rest_cstmt), c2)
            }
            StmtData::Let { name, tm } => {
                let (ctm, ty, c1) = tm.infer(c0);

                // if the value can be evaluated at the statement level,
                // it can be omitted from core representation and simply cached.
                // otherwise, it needs to be represented as a core bind
                //
                // if there is an eval error, we also need to report it
                let (val, c2): (Val<'p>, Context<'_>) =
                    match ctm.eval2(&c1.get_global_env(), &c1.get_local_env()) {
                        Ok(val) => (val.concretise(), c1),
                        Err(e) => (Val::Error, c1.report(ElabError::from_eval_error(e))),
                    };

                // update the context accordingly
                let c3 = if val.is_neutral() {
                    c2.bind_param(name, &ty)
                } else {
                    c2.bind(name, ty, val.reference())
                };

                // elaborate the rest of the statements, using the new context
                let (rest_cstmt, c3) = match &rest[..] {
                    [] => (
                        core::Stmt::new(self.location.clone(), core::StmtData::End),
                        c3,
                    ),
                    [next, rest1 @ ..] => next.elab(&c3, rest1),
                };

                if val.is_neutral() {
                    // it needs to be represented as a core bind
                    (
                        core::Stmt::new(
                            self.location.clone(),
                            core::StmtData::Let {
                                tm: ctm.clone(),
                                next: Arc::new(rest_cstmt),
                            },
                        ),
                        c3,
                    )
                } else {
                    // it can be omitted from core representation, as it has been cached
                    (rest_cstmt, c3)
                }
            }
            StmtData::Tm { tm } => {
                let (ctm, c1) = tm.check(c0, &core::Val::EffectTy);

                // elaborate the rest of the statements, using the new context
                let (rest_cstmt, c2) = match &rest[..] {
                    [] => (
                        core::Stmt::new(self.location.clone(), core::StmtData::End),
                        c1,
                    ),
                    [next, rest1 @ ..] => next.elab(&c1, rest1),
                };

                (
                    core::Stmt::new(
                        self.location.clone(),
                        core::StmtData::Tm {
                            tm: ctm,
                            next: Arc::new(rest_cstmt),
                        },
                    ),
                    c2,
                )
            }
            StmtData::If { branches } => {
                for branch in branches {}

                todo!();

                todo!()
            }
        }
    }
}

impl Branch {
    fn elab<'p>(&self, c0: &Context<'p>) -> (core::Branch<'p>, Context<'p>) {
        match &self.data {
            BranchData::Bool { tm, stmt } => {
                let (ctm, c1) = tm.check(c0, &core::Val::BoolTy);
                let (cstmt, c2) = stmt.elab(&c1, &[]);

                (
                    core::Branch::new(
                        self.location.clone(),
                        core::BranchData::Bool {
                            tm: ctm,
                            stmt: cstmt,
                        },
                    ),
                    c2,
                )
            }
            BranchData::Matches { tm, branches } => {
                let (ctm, cty, c1) = tm.infer(c0);

                let mut final_ty = core::Val::AnyTy;

                let mut final_branches = vec![];
                let mut cx = c1;
                for branch in branches {
                    let (core_branch, core_branch_ty, cy) = branch.elab(&cx, &final_ty);
                    cx = cy;

                    // check that we can equate the branches
                    cx = if !final_ty.equiv(&core_branch_ty) {
                        cx.report(ElabError::new(
                            &self.location,
                            &format!(
                                "mismatched types: expected {}, found {}",
                                final_ty, core_branch_ty
                            ),
                        ))
                    } else {
                        cx
                    };

                    // then, hold on to whichever is more precise
                    final_ty = final_ty.most_precise(&core_branch_ty);
                    // and keep all the core branches
                    final_branches.push(core_branch);
                }

                // then, we could check for full coverage / completeness ... but we don't have to
                // because these are allowed to not produce any output!

                (
                    core::Branch::new(
                        self.location.clone(),
                        core::BranchData::Matches {
                            tm: ctm,
                            branches: final_branches,
                        },
                    ),
                    cx,
                )
            }
        }
    }
}

impl PatternBranch {
    fn elab<'p>(
        &self,
        c0: &Context<'p>,
        ty: &Val<'p>,
    ) -> (core::PatternBranch<'p>, Val<'p>, Context<'p>) {
        let (matcher, ty1, bind_tys) = self.data.pat.infer(c0, ty);

        let c1 = if !ty1.equiv(ty) {
            c0.report(ElabError::new(
                &self.location,
                &format!("mismatched types: expected {}, found {}", ty, ty1),
            ))
        } else {
            c0.reference()
        };

        let (stmt, c2) = self.data.stmt.elab(&c1, &[]);

        (
            core::PatternBranch::new(
                self.location.clone(),
                core::PatternBranchData { matcher, stmt },
            ),
            ty1,
            c2,
        )
    }
}

impl Pattern {
    fn infer<'p>(
        &self,
        c0: &Context<'p>,
        ty: &Val<'p>,
    ) -> (
        Arc<dyn core::matcher::Matcher<'p> + 'p>,
        Val<'p>,
        Context<'p>,
    ) {
        match &self.data {
            PatternData::Named { name, pattern } => {
                // TODO warn if name is already assigned

                let (matcher, ty1, c1) = pattern.infer(c0, ty);
                let c2 = c1.bind_param(name, &ty1);

                (matcher, ty1, c2)
            }
            PatternData::Hole => (
                Arc::new(core::matcher::Succeed {}),
                Val::AnyTy,
                c0.reference(),
            ),
            PatternData::BoolLit { b } => (
                Arc::new(core::matcher::Equal::new(&Val::Bool { b: *b })),
                Val::BoolTy,
                c0.reference(),
            ),
            PatternData::NumLit { n } => (
                Arc::new(core::matcher::Equal::new(&Val::Num { n: n.get_float() })),
                Val::NumTy,
                c0.reference(),
            ),
            PatternData::StrLit { s } => (
                // WARN could be wasteful
                Arc::new(core::matcher::Equal::new(&Val::Str {
                    s: core::StrVal::new_concrete(s.clone()),
                })),
                Val::StrTy,
                c0.reference(),
            ),
            PatternData::RecLit { fields } => {
                // TODO warn if the name is already assigned

                // for each field, infer the pattern
                let (matcher, tys, c1) = fields.iter().fold(
                    (
                        Arc::new(core::matcher::Succeed {}) as Arc<dyn core::matcher::Matcher>,
                        vec![],
                        c0.reference(),
                    ),
                    |(m0, tys, cx), field| {
                        let (m1, ty1, c1) = field.data.infer(&cx, ty);

                        let mut tys1 = tys;
                        tys1.push(CoreRecField::new(field.name.as_bytes(), ty1));

                        (
                            Arc::new(core::matcher::Chain {
                                m1: Arc::new(core::matcher::FieldAccess {
                                    name: field.name.clone(),
                                    inner: m1,
                                }),
                                m2: m0,
                            }),
                            tys1,
                            c1,
                        )
                    },
                );

                (matcher, Val::RecTy { fields: tys }, c1)
            }
            PatternData::Read { regs, binds, mode } => {
                // typecheck all the binds;
                // end up with the name of each bind, the (list) value that it is SELECTING from,
                // and the type of it itself

                // everything else happens in here
                let (matcher, c2) = ReadMatcher::infer(c0, regs, binds, mode, &self.location);

                let matcher1 = match matcher {
                    itertools::Either::Left(m) => Arc::new(m) as Arc<dyn Matcher>,
                    itertools::Either::Right(m) => Arc::new(m) as Arc<dyn Matcher>,
                };

                (
                    matcher1,
                    Val::RecWithTy {
                        fields: vec![CoreRecField {
                            name: b"seq".to_vec(),
                            data: Val::StrTy,
                        }],
                    },
                    c2,
                )
            }
        }
    }
}

impl Tm {
    pub fn check<'p>(&self, c0: &Context<'p>, expected: &Val<'p>) -> (core::Tm, Context<'p>) {
        todo!()
    }

    pub fn infer<'p, 'q>(&self, c0: &Context<'q>) -> (core::Tm, Val<'p>, Context<'q>)
    where
        'q: 'p,
    {
        match &self.data {
            TmData::BoolLit { b } => (
                core::Tm::new(self.location.clone(), core::TmData::BoolLit { b: *b }),
                Val::BoolTy,
                c0.clone(),
            ),
            TmData::Name { name } => {
                // first, look up the name and see what kind of thing this is
                match c0.lookup(name) {
                    Some((address, ty)) => (
                        core::Tm::new(
                            self.location.clone(),
                            match address {
                                Address::Global { index } => core::TmData::Global { index },
                                Address::Local { index } => core::TmData::Local { index },
                            },
                        ),
                        ty.reference(),
                        c0.clone(),
                    ),
                    // looking up unbound names is reported as an error
                    None => (
                        core::Tm::new_error(&self.location),
                        Val::ErrorTy,
                        c0.report(ElabError::new_unbound_name(&self.location, name)),
                    ),
                }
            }
            TmData::NumLit { n } => (
                core::Tm::new(
                    self.location.clone(),
                    core::TmData::NumLit { n: n.get_float() },
                ),
                Val::NumTy,
                c0.clone(),
            ),
            TmData::StrLit { regs } => {
                let (ctm, c1) = elab_str_lit_regs(regs, c0, &self.location);
                (ctm, Val::StrTy, c1)
            }
            TmData::RecTy { fields } => {
                let mut cx = c0.reference();
                let mut core_fields = vec![];

                for field in fields {
                    let (ctm, cy) = field.data.check(&cx, &Val::Univ);
                    cx = cy;

                    core_fields.push(crate::util::recfield::CoreRecField::new(
                        field.name.as_bytes(),
                        ctm,
                    ));
                }

                (
                    core::Tm::new(
                        self.location.clone(),
                        core::TmData::RecTy {
                            fields: core_fields,
                        },
                    ),
                    Val::Univ,
                    cx,
                )
            }
            TmData::RecWithTy { fields } => {
                let mut cx = c0.reference();
                let mut core_fields = vec![];

                for field in fields {
                    let (ctm, cy) = field.data.check(&cx, &Val::Univ);
                    cx = cy;

                    core_fields.push(crate::util::recfield::CoreRecField::new(
                        field.name.as_bytes(),
                        ctm,
                    ));
                }

                (
                    core::Tm::new(
                        self.location.clone(),
                        core::TmData::RecWithTy {
                            fields: core_fields,
                        },
                    ),
                    Val::Univ,
                    cx,
                )
            }
            TmData::RecLit { fields } => {
                let mut cx = c0.reference();
                let mut ctms = vec![];
                let mut tys = vec![];

                for field in fields {
                    let (ctm, ty, cy) = field.data.infer(&cx);
                    cx = cy;

                    ctms.push(crate::util::recfield::CoreRecField::new(
                        field.name.as_bytes(),
                        ctm,
                    ));

                    tys.push(crate::util::recfield::CoreRecField::new(
                        field.name.as_bytes(),
                        ty,
                    ));
                }

                (
                    core::Tm::new(self.location.clone(), core::TmData::RecLit { fields: ctms }),
                    Val::RecTy { fields: tys },
                    cx,
                )
            }
            TmData::RecProj { tm, name } => {
                let (ctm, ty, c1) = tm.infer(c0);
                match ty {
                    Val::RecTy { fields } => {
                        match fields.iter().find(|field| field.name.eq(name.as_bytes())) {
                            Some(field) => (
                                core::Tm::new(
                                    self.location.clone(),
                                    core::TmData::RecProj {
                                        tm: Arc::new(ctm),
                                        name: name.as_bytes().to_vec(),
                                    },
                                ),
                                field.data.reference(),
                                c1,
                            ),
                            None => (core::Tm::new_error(&self.location), Val::ErrorTy, c1.report(ElabError::new(&self.location, "trying to access field of a value which doesn't have any fields"))),
                        }
                    }
                    _ => (
                        core::Tm::new_error(&self.location),
                        Val::ErrorTy,
                        c1.report(ElabError::new_non_existent_field_access(
                            &self.location,
                            name,
                            &ty,
                        )),
                    ),
                }
            }
            TmData::ListTy { tm } => {
                let (ctm, c1) = tm.check(c0, &Val::Univ);

                (
                    core::Tm::new(
                        self.location.clone(),
                        core::TmData::ListTy { ty: Arc::new(ctm) },
                    ),
                    Val::Univ,
                    c1,
                )
            }
            TmData::ListLit { tms } => {
                // go through each tm, inferring and checking type
                let mut cx = c0.reference();

                let mut final_ty = Val::AnyTy;
                let mut ctms = vec![];
                for tm in tms {
                    let (ctm, ty, cy) = tm.infer(&cx);
                    cx = cy;

                    (cx, final_ty) = if !final_ty.equiv(&ty) {
                        (
                            cx.report(ElabError::new(
                                &self.location,
                                &format!("mismatched types: expected {}, found {}", final_ty, ty),
                            )),
                            Val::ErrorTy,
                        )
                    } else {
                        (cx, final_ty.most_precise(&ty))
                    };

                    ctms.push(ctm);
                }

                (
                    core::Tm::new(self.location.clone(), core::TmData::ListLit { tms: ctms }),
                    Val::ListTy {
                        ty: Arc::new(final_ty),
                    },
                    cx,
                )
            }
            TmData::FunTy { args, opts, body } => {
                let (core_args, c1) =
                    args.iter().fold((vec![], c0.reference()), |(acc, cx), tm| {
                        let (ctm, cy) = tm.check(&cx, &Val::Univ);
                        let mut acc1 = acc.clone();
                        acc1.push(ctm);

                        (acc1, cy)
                    });

                let (core_opts, c2) =
                    match opts
                        .iter()
                        .try_fold((vec![], c1.reference()), |(acc, cx), opt| {
                            let (ty, cx1) = opt.ty.check(&cx, &Val::Univ);
                            let ty_val = match ty.eval2(&cx1.get_global_env(), &cx1.get_local_env())
                            {
                                Ok(v) => v.concretise(),
                                Err(e) => {
                                    return Err((
                                        core::Tm::new_error(&self.location),
                                        Val::ErrorTy,
                                        c1.report(ElabError::from_eval_error(e)),
                                    ))
                                }
                            };

                            let (ctm, cx2) = opt.tm.check(&cx1, &ty_val);

                            let mut acc1 = acc.clone();
                            acc1.push((opt.name.as_bytes().to_vec(), ty, ctm));

                            Ok((acc1, cx2))
                        }) {
                        Ok(a) => a,
                        Err((_, _, c2)) => {
                            return (core::Tm::new_error(&self.location), Val::ErrorTy, c2)
                        }
                    };

                let (body_ctm, c3) = body.check(&c2, &Val::Univ);

                (
                    core::Tm::new(
                        self.location.clone(),
                        core::TmData::FunTy {
                            args: core_args,
                            opts: core_opts,
                            body: Arc::new(body_ctm),
                        },
                    ),
                    Val::Univ,
                    c3,
                )
            }
            TmData::FunLit { args, opts, body } => {
                // make sure each param has a type associated which is actually a type
                let (arg_ctms, c1) = {
                    let mut arg_ctms = vec![];
                    let mut cx = c0.reference();
                    for arg in args {
                        let (arg_ty_ctm, cy) = arg.ty.check(&cx, &Val::Univ);
                        cx = cy;

                        let (val, cy) =
                            match arg_ty_ctm.eval2(&c0.get_global_env(), &c0.get_local_env()) {
                                Ok(val) => (val.concretise(), cx),
                                Err(e) => (Val::Error, cx.report(ElabError::from_eval_error(e))),
                            };
                        cx = cy;

                        arg_ctms.push((arg.name.clone(), val));
                    }
                    (arg_ctms, cx)
                };

                // add together all the optional arguments and regular arguments
                let (opt_tys_ctms, c2) = {
                    let mut opt_tys_ctms = vec![];
                    let mut cx = c1.reference();
                    for opt in opts {
                        let (opt_ty_ctm, cy) = opt.ty.check(&cx, &Val::Univ);
                        cx = cy;

                        // fully evaluate the type
                        let opt_ty =
                            match opt_ty_ctm.eval2(&cx.get_global_env(), &cx.get_local_env()) {
                                Ok(v) => v.concretise(),
                                Err(e) => {
                                    return (
                                        core::Tm::new_error(&self.location),
                                        Val::ErrorTy,
                                        cx.report(ElabError::from_eval_error(e)),
                                    )
                                }
                            };

                        // check that the term matches the given type
                        let (opt_ctm, cy) = opt.tm.check(&cx, &opt_ty);
                        cx = cy;
                        opt_tys_ctms.push((opt.name.clone(), opt_ty, opt_ctm))
                    }
                    (opt_tys_ctms, cx)
                };

                let c_inner = {
                    let mut cx = c2.reference();
                    // first bind all the params
                    for (name, val) in &arg_ctms {
                        cx = cx.bind_param(&name, &val);
                    }
                    // then bind all the optionals
                    for (name, ty, _) in &opt_tys_ctms {
                        cx = cx.bind_param(&name, &ty);
                    }

                    cx
                };

                let (body_ctm, body_ty, c3) = body.infer(&c_inner);

                (
                    core::Tm::new(
                        self.location.clone(),
                        core::TmData::FunLit {
                            body: Arc::new(body_ctm),
                        },
                    ),
                    Val::FunTy {
                        args: arg_ctms.iter().map(|(_, val)| val.concretise()).collect(),
                        opts: opt_tys_ctms
                            .iter()
                            .map(|(name, val, tm)| {
                                (name.as_bytes().to_vec(), val.concretise(), tm.clone())
                            })
                            .collect(),
                        body: Arc::new(body_ty),
                    },
                    c3,
                )
            }
            TmData::FunLitForeign {
                args,
                opts,
                ty,
                name,
            } => {
                // make sure each param has a type associated which is actually a type
                let (arg_ctms, c1) = {
                    let mut arg_ctms = vec![];
                    let mut cx = c0.reference();
                    for arg in args {
                        let (arg_ty_ctm, cy) = arg.ty.check(&cx, &Val::Univ);
                        cx = cy;

                        let (val, cy) =
                            match arg_ty_ctm.eval2(&c0.get_global_env(), &c0.get_local_env()) {
                                Ok(val) => (val.concretise(), cx),
                                Err(e) => (Val::Error, cx.report(ElabError::from_eval_error(e))),
                            };
                        cx = cy;

                        arg_ctms.push((arg.name.clone(), val));
                    }
                    (arg_ctms, cx)
                };

                // add together all the optional arguments and regular arguments
                let (opt_tys_ctms, c2) = {
                    let mut opt_tys_ctms = vec![];
                    let mut cx = c1.reference();
                    for opt in opts {
                        let (opt_ty_ctm, cy) = opt.ty.check(&cx, &Val::Univ);
                        cx = cy;

                        // fully evaluate the type
                        let opt_ty =
                            match opt_ty_ctm.eval2(&cx.get_global_env(), &cx.get_local_env()) {
                                Ok(v) => v.concretise(),
                                Err(e) => {
                                    return (
                                        core::Tm::new_error(&self.location),
                                        Val::ErrorTy,
                                        cx.report(ElabError::from_eval_error(e)),
                                    )
                                }
                            };

                        // check that the term matches the given type
                        let (opt_ctm, cy) = opt.tm.check(&cx, &opt_ty);
                        cx = cy;
                        opt_tys_ctms.push((opt.name.clone(), opt_ty, opt_ctm))
                    }
                    (opt_tys_ctms, cx)
                };

                let c_inner = {
                    let mut cx = c2.reference();
                    // first bind all the params
                    for (name, val) in &arg_ctms {
                        cx = cx.bind_param(&name, &val);
                    }
                    // then bind all the optionals
                    for (name, ty, _) in &opt_tys_ctms {
                        cx = cx.bind_param(&name, &ty);
                    }

                    cx
                };

                let (cty, c3) = ty.check(&c_inner, &Val::Univ);
                let (cty_val, c4) =
                    match cty.eval2(&c_inner.get_global_env(), &c_inner.get_local_env()) {
                        Ok(val) => (val.concretise(), c3),
                        Err(e) => (Val::Error, c3.report(ElabError::from_eval_error(e))),
                    };

                let body_ty = if cty_val.is_neutral() {
                    // if it's neutral, we throw it away - need to create a val that is just a function frome some arguments
                    // to a new val (which we expect to be concrete)
                    Val::FunReturnTyAwaiting {
                        data: FunData {
                            global: c4.get_global_env(),
                            local: c4.get_local_env(),
                            body: cty.clone(),
                        },
                    }
                } else {
                    cty_val
                };

                let body = match library::foreign(&self.location, name) {
                    Ok(body) => body,
                    Err(e) => {
                        return (
                            core::Tm::new_error(&self.location),
                            Val::ErrorTy,
                            c4.report(ElabError::from_eval_error(e)),
                        )
                    }
                };

                (
                    core::Tm::new(self.location.clone(), core::TmData::FunForeignLit { body }),
                    Val::FunTy {
                        args: arg_ctms.iter().map(|(_, val)| val.concretise()).collect(),
                        opts: opt_tys_ctms
                            .iter()
                            .map(|(name, val, tm)| {
                                (name.as_bytes().to_vec(), val.concretise(), tm.clone())
                            })
                            .collect(),
                        body: Arc::new(body_ty),
                    },
                    c4,
                )
            }
            TmData::FunApp { head, args, opts } => {
                let (head_ctm, head_ty, c1) = head.infer(c0);

                match head_ty {
                    Val::FunTy {
                        args: args_ty,
                        opts: opts_ty,
                        body,
                    } => {
                        // first make sure the argument lists are the same length
                        if !args.len().eq(&args_ty.len()) {
                            return (
                                core::Tm::new_error(&self.location),
                                Val::ErrorTy,
                                c1.report(ElabError::new(
                                    &self.location,
                                    &format!(
                                        "function was given {} arguments, expected {}",
                                        args.len(),
                                        args_ty.len()
                                    ),
                                )),
                            );
                        }

                        // then, make sure that every optional argument was one of the ones provided for by the type
                        for (name, _) in opts {
                            if !opts_ty
                                .iter()
                                .map(|(name, _, _)| name)
                                .contains(&name.as_bytes().to_vec())
                            {
                                return (core::Tm::new_error(&self.location),Val::ErrorTy, c1.report(
                                ElabError::new(&self.location, &format!("found optional argument named '{}'; only expected optional arguments {{ {} }}", name, opts_ty.iter().map(|(name, ty, _)| format!("{}: {}", bytes_to_string(name).unwrap(), ty)).join(", ")))
                                ));
                            }
                        }

                        // then, go through the optional arguments from the type.
                        // where they have been provided in this application, overwrite their associated term
                        let (opt_ctms, c2) = {
                            let mut cx = c1.reference();
                            let mut ctms = vec![];
                            for (name, ty, tm) in &opts_ty {
                                match opts
                                    .iter()
                                    .find(|(name_in_app, _)| name_in_app.as_bytes().eq(name))
                                {
                                    Some((_, tm)) => {
                                        let (ctm, cy) = tm.check(&cx, ty);
                                        cx = cy;

                                        ctms.push(ctm);
                                    }
                                    None => ctms.push(tm.clone()),
                                }
                            }

                            (ctms, cx)
                        };

                        // elaborate each argument, and make sure they all
                        // correspond to the function's argument type
                        // - and if any are neutrals, the whole thing is neutral
                        let (arg_ctms, c3) = {
                            let mut cx = c2.reference();
                            let mut arg_ctms = vec![];
                            for (arg, arg_ty) in args.iter().zip(args_ty) {
                                let (arg_ctm, cy) = arg.check(&cx, &arg_ty);
                                cx = cy;

                                arg_ctms.push(arg_ctm);
                            }

                            (arg_ctms, cx)
                        };

                        let body_ty = match body.as_ref() {
                            Val::FunReturnTyAwaiting { data } => {
                                //
                                let arg_vals = {
                                    let mut a = vec![];
                                    for arg in &arg_ctms {
                                        let v = match arg
                                            .eval2(&c3.get_global_env(), &c3.get_local_env())
                                        {
                                            Ok(v) => v.concretise(),
                                            Err(e) => {
                                                return (
                                                    core::Tm::new_error(&self.location),
                                                    Val::ErrorTy,
                                                    c3.report(ElabError::from_eval_error(e)),
                                                )
                                            }
                                        };

                                        a.push(v);
                                    }

                                    a
                                };

                                // apply the function to get the concrete type out
                                let ty_val = match data
                                    .app(arg_vals)
                                    // remap the location of the error; it currently holds the location of the literal definition,
                                    // but it should report at the application site
                                    .map_err(|e| EvalError {
                                        location: self.location.clone(),
                                        message: e.message,
                                    }) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        return (
                                            core::Tm::new_error(&self.location),
                                            Val::ErrorTy,
                                            c3.report(ElabError::from_eval_error(e)),
                                        )
                                    }
                                };

                                ty_val
                            }
                            _ => body.concretise(),
                        };

                        // combine mandatory positional and optional named fields
                        let all_arg_tms = arg_ctms.into_iter().chain(opt_ctms).collect();

                        (
                            core::Tm::new(
                                self.location.clone(),
                                core::TmData::FunApp {
                                    head: Arc::new(head_ctm),
                                    args: all_arg_tms,
                                },
                            ),
                            body_ty.concretise(),
                            c3,
                        )
                    }
                    _ => (
                        core::Tm::new_error(&self.location),
                        Val::ErrorTy,
                        c1.report(ElabError::new(
                            &self.location,
                            &format!(
                                "trying to apply something as a function when it's {}",
                                head_ty
                            ),
                        )),
                    ),
                }
            }
            TmData::BinOp { tm0, tm1, op } => todo!(),
            TmData::UnOp { tm, op } => todo!(),
        }
    }

    pub fn get_ids(&self) -> Vec<String> {
        match &self.data {
            TmData::BoolLit { .. } | TmData::NumLit { .. } => vec![],
            TmData::StrLit { regs } => todo!(),
            TmData::RecTy { fields } => todo!(),
            TmData::RecWithTy { fields } => todo!(),
            TmData::RecLit { fields } => todo!(),
            TmData::RecProj { tm, name } => todo!(),
            TmData::ListTy { tm } => todo!(),
            TmData::ListLit { tms } => todo!(),
            TmData::FunTy { args, opts, body } => todo!(),
            TmData::FunLit { args, opts, body } => todo!(),
            TmData::FunLitForeign {
                args,
                opts,
                ty,
                name,
            } => todo!(),
            TmData::FunApp { head, args, opts } => todo!(),
            TmData::BinOp { tm0, tm1, op } => todo!(),
            TmData::UnOp { tm, op } => todo!(),
            TmData::Name { name } => vec![name.clone()],
        }
    }
}

fn elab_str_lit_regs<'p>(
    regs: &[StrLitRegion],
    c0: &Context<'p>,
    location: &Location,
) -> (core::Tm, Context<'p>) {
    match regs {
        [reg, rest @ ..] => match rest {
            [] => reg.elab(c0),
            rest => {
                let (head, c1) = Tm::new(
                    location.clone(),
                    TmData::Name {
                        name: "str_concat".to_string(),
                    },
                )
                .check(
                    c0,
                    &Val::FunTy {
                        args: vec![Val::StrTy, Val::StrTy],
                        opts: vec![],
                        body: Arc::new(Val::StrTy),
                    },
                );

                let (core_reg, c2) = reg.elab(&c1);
                let (core_rest, c3) = elab_str_lit_regs(rest, &c2, location);

                (
                    core::Tm::new(
                        location.clone(),
                        core::TmData::FunApp {
                            head: Arc::new(head),
                            args: vec![core_reg, core_rest],
                        },
                    ),
                    c3,
                )
            }
        },

        // we started with an empty region
        [] => (
            core::Tm::new(location.clone(), core::TmData::StrLit { s: vec![] }),
            c0.reference(),
        ),
    }
}

impl StrLitRegion {
    pub fn elab<'p>(&self, c0: &Context<'p>) -> (core::Tm, Context<'p>) {
        match &self.data {
            StrLitRegionData::Str { s } => (
                core::Tm::new(self.location.clone(), core::TmData::StrLit { s: s.clone() }),
                c0.reference(),
            ),
            StrLitRegionData::Tm { tm } => tm.check(c0, &Val::StrTy),
        }
    }
}

impl MatchMode {
    pub fn elab(&self) -> core::matcher::read_matcher2::MatchMode {
        match self {
            MatchMode::All => core::matcher::read_matcher2::MatchMode::All,
            MatchMode::AllBest => core::matcher::read_matcher2::MatchMode::One,
            MatchMode::OneBest => core::matcher::read_matcher2::MatchMode::Unique,
        }
    }
}
