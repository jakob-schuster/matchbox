use std::sync::Arc;

use crate::core::eval2::env::Env;
use crate::core::matcher::read_matcher2::ReadMatcher;
use crate::core::matcher::Matcher;
use crate::core::{self, StrVal, Val};
use crate::surface::elab::ElabError;
use crate::surface::elab2::context::Address;
use crate::surface::{elab2::context::Context, Tm};
use crate::surface::{
    Branch, MatchMode, Pattern, PatternBranch, PatternData, Stmt, StmtData, TmData,
};
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
            super::BranchData::Bool { tm, stmt } => {
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
            super::BranchData::Is { tm, branches } => {
                let (ctm, cty, c1) = tm.infer(c0);

                for branch in branches {}

                todo!()
            }
        }
    }
}

impl PatternBranch {
    fn elab<'p>(&self, c0: &Context<'p>) -> () {
        // let (matcher, cty1, bind_tys) = ;

        todo!()
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
    pub fn check<'p>(&self, ctx: &Context<'p>, expected: &Val<'p>) -> (core::Tm, Context<'p>) {
        todo!()
    }

    pub fn infer<'p, 'q>(&self, ctx: &Context<'q>) -> (core::Tm, Val<'p>, Context<'q>)
    where
        'q: 'p,
    {
        match &self.data {
            TmData::BoolLit { b } => (
                core::Tm::new(self.location.clone(), core::TmData::BoolLit { b: *b }),
                Val::BoolTy,
                ctx.clone(),
            ),
            TmData::Name { name } => {
                // first, look up the name and see what kind of thing this is
                match ctx.lookup(name) {
                    Some((address, ty)) => (
                        core::Tm::new(
                            self.location.clone(),
                            match address {
                                Address::Global { index } => core::TmData::Global { index },
                                Address::Local { index } => core::TmData::Local { index },
                            },
                        ),
                        ty.reference(),
                        ctx.clone(),
                    ),
                    // looking up unbound names is reported as an error
                    None => (
                        core::Tm::new(self.location.clone(), core::TmData::Error),
                        Val::ErrorTy,
                        ctx.report(ElabError::new_unbound_name(&self.location, name)),
                    ),
                }
            }

            _ => todo!(),
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

impl MatchMode {
    pub fn elab(&self) -> core::matcher::read_matcher2::MatchMode {
        match self {
            MatchMode::All => core::matcher::read_matcher2::MatchMode::All,
            MatchMode::AllBest => core::matcher::read_matcher2::MatchMode::One,
            MatchMode::OneBest => core::matcher::read_matcher2::MatchMode::Unique,
        }
    }
}
