use std::{collections::HashMap, io::Read, ops::Deref, rc::Rc, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{
        self, eval, library,
        matcher::{self, LocTm, OpTm},
        EvalError,
    },
    parse,
    util::{Arena, CoreRecField, Env, Located, Location, Ran, RecField},
    visit, GlobalConfig,
};

/// An error that will be raised if there was a problem in the surface syntax,
/// usually as a result of type errors. This is normal, and should be rendered
/// nicely to the programmer.
#[derive(Debug, Clone)]
pub struct ElabError {
    pub location: Location,
    pub message: String,
}

impl ElabError {
    fn new(location: &Location, message: &str) -> ElabError {
        ElabError {
            location: location.clone(),
            message: message.to_string(),
        }
    }

    fn from_eval_error(eval_error: EvalError) -> ElabError {
        ElabError {
            location: eval_error.location,
            message: eval_error.message,
        }
    }
}

pub type Prog = Located<ProgData>;
#[derive(Debug)]
pub struct ProgData {
    pub stmts: Vec<Stmt>,
}

pub type Stmt = Located<StmtData>;
#[derive(Debug, Clone)]
pub enum StmtData {
    /// A group of statements, executed together.
    /// [ { let tso: AGCTG; read.name } ]
    Group { stmts: Vec<Stmt> },

    /// A let statement, binding a name to the value of a term.
    /// [ let tso: AGCTGA ]
    Let { name: String, tm: Tm },

    /// An output statement, sending a value to an output destination.
    /// (For now, these are built-in rather than functions - may need to change)
    /// [ read.name |> file('out.txt') ]
    Out { tm0: Tm, tm1: Tm },

    /// A conditional structure.
    /// [ if read is [ |3| rest:_ ] => rest |> trimmed ]
    If { branches: Vec<Branch> },
}

pub type Branch = Located<BranchData>;
#[derive(Debug, Clone)]
pub enum BranchData {
    /// A boolean branch
    /// [ read.seq.len() > 100 => read.name |> names ]
    Bool { tm: Tm, stmt: Stmt },

    /// A pattern-matching branch
    /// [ read is [_ tso _] => read |> filtered ]
    Is {
        tm: Tm,
        branches: Vec<PatternBranch>,
    },
}

/// The pattern is attempted; if successful, the bindings from the pattern
/// match are used when executing the statement.
pub type PatternBranch = Located<PatternBranchData>;
#[derive(Debug, Clone)]
pub struct PatternBranchData {
    pub pat: Pattern,
    pub stmt: Stmt,
}

/// A pattern is a boolean test that, if successful,
/// also produces a context of bound values.
/// Could be a literal, or something more complex.
pub type Pattern = Located<PatternData>;
#[derive(Debug, Clone)]
pub enum PatternData {
    /// Named pattern (match the body, return a binding)
    Named {
        name: String,
        pattern: Rc<Pattern>,
    },

    /// Wild / hole (match anything, return no bindings)
    Hole,

    /// Literals (return no bindings)
    BoolLit {
        b: bool,
    },
    NumLit {
        n: Num,
    },
    StrLit {
        s: Vec<u8>,
    },
    RecLit {
        fields: Vec<RecField<Pattern>>,
    },

    /// Any other built in patterns (e.g. list pattern?)
    // List {},

    /// Read pattern (returns bindings!)
    Read {
        regs: Vec<Region>,
        binds: Vec<ReadParameter>,
        error: f32,
    },
}

/// A parameter in a read pattern
pub type ReadParameter = Located<ReadParameterData>;
#[derive(Debug, Clone)]
pub struct ReadParameterData {
    pub name: String,
    pub tm: Tm,
}

pub type Region = Located<RegionData>;
#[derive(Debug, Clone)]
pub enum RegionData {
    Hole,
    Term { tm: Tm },

    Named { name: String, regs: Vec<Region> },
    Sized { tm: Tm, regs: Vec<Region> },
}

pub type Tm = Located<TmData>;
#[derive(Clone, Debug)]
pub enum TmData {
    /// Value literals
    BoolLit {
        b: bool,
    },
    NumLit {
        n: Num,
    },
    StrLit {
        regs: Vec<StrLitRegion>,
    },

    RecTy {
        fields: Vec<RecField<Tm>>,
    },
    RecLit {
        fields: Vec<RecField<Tm>>,
    },
    RecProj {
        tm: Rc<Tm>,
        name: String,
    },

    FunTy {
        args: Vec<Tm>,
        body: Rc<Tm>,
    },
    FunLit {
        args: Vec<Param>,
        body: Rc<Tm>,
    },
    FunLitForeign {
        args: Vec<Param>,
        ty: Rc<Tm>,
        name: String,
    },
    /// Function application
    FunApp {
        head: Rc<Tm>,
        args: Vec<Tm>,
    },
    /// Binary operations
    BinOp {
        tm0: Rc<Tm>,
        tm1: Rc<Tm>,
        op: BinOp,
    },
    /// Unary operations
    UnOp {
        tm: Rc<Tm>,
        op: UnOp,
    },

    /// Named things
    Name {
        name: String,
    },
}

#[derive(Clone, Debug)]
pub struct Param {
    pub name: String,
    pub ty: Tm,
}

#[derive(Clone, Debug)]
pub enum Num {
    Int(i32),
    Float(f32),
}

impl Num {
    fn get_float(&self) -> f32 {
        match self {
            Num::Int(i) => *i as f32,
            Num::Float(f) => *f,
        }
    }
}

#[derive(Clone, Debug)]
pub enum BinOp {
    Plus,
    Minus,
    And,
    Or,
    Equal,
    NotEqual,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,

    Times,
    Division,
    Modulo,
}

#[derive(Clone, Debug)]
pub enum UnOp {
    Minus,
}

pub type StrLitRegion = Located<StrLitRegionData>;
#[derive(Clone, Debug)]
pub enum StrLitRegionData {
    Str { s: Vec<u8> },
    Tm { tm: Tm },
}

#[derive(Clone, Default)]
pub struct Context<'a> {
    size: usize,
    names: Env<String>,
    tys: Env<&'a core::Val<'a>>,
    pub tms: Env<&'a core::Val<'a>>,
}

impl<'a> Context<'a> {
    /// Returns the next variable that will be bound in the context after
    /// calling bind_def or bind_param
    pub fn next_var(&self, arena: &'a Arena) -> &'a core::Val<'a> {
        arena.alloc(core::Val::Neutral {
            neutral: core::Neutral::Var { level: self.size },
        })
    }

    /// Binds a definition in the context
    pub fn bind_def(
        &self,
        name: String,
        ty: &'a core::Val<'a>,
        tm: &'a core::Val<'a>,
    ) -> Context<'a> {
        Context {
            size: self.size + 1,
            names: self.names.with(name),
            tys: self.tys.with(ty),
            tms: self.tms.with(tm),
        }
    }

    /// Binds a parameter in the context
    pub fn bind_param(&self, name: String, ty: &'a core::Val<'a>, arena: &'a Arena) -> Context<'a> {
        self.bind_def(name, ty, self.next_var(arena))
    }

    /// Looks up a name in the context
    pub fn lookup(&self, name: String) -> Option<(usize, &'a core::Val<'a>)> {
        // Find the index of most recent binding in the context identified by
        // name, starting from the most recent binding. This gives us the
        // de Bruijn index of the variable.
        let index = self.size - 1 - self.names.find_last(&name)?;
        let val = self.tys.get_index(index);

        Some((index, val))
    }
}

pub fn elab_prog<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    prog: &'a Prog,
) -> Result<core::Prog<'a>, ElabError> {
    Ok(core::Prog::new(
        prog.location.clone(),
        core::ProgData {
            stmt: match &prog.data.stmts[..] {
                [first, rest @ ..] => {
                    elab_stmt(arena, ctx, first, &rest.iter().collect::<Vec<_>>())?
                }
                [] => core::Stmt::new(Location::new(0, 0), core::StmtData::End),
            },
        },
    ))
}

fn elab_stmt<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    stmt: &'a Stmt,
    rest: &[&'a Stmt],
) -> Result<core::Stmt<'a>, ElabError> {
    match &stmt.data {
        // fold across all the statements, ending the block with an End
        StmtData::Group { stmts } => match &stmts[..] {
            [first_stmt, rest_stmts @ ..] => elab_stmt(
                arena,
                ctx,
                first_stmt,
                &rest_stmts
                    .iter()
                    .chain(rest.iter().map(|a| *a))
                    .collect::<Vec<_>>(),
            ),
            [] => match rest {
                [first, rest @ ..] => todo!(),
                [] => Ok(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
            },
        },

        // todo: i don't think this works, because you're not actually making the bind
        StmtData::Let { name, tm } => {
            let (ctm, ty) = infer_tm(arena, ctx, tm)?;

            // now we evaluate the terms as best we can, in case we need them at the static stage (we probably will)
            let new_ctx = ctx.bind_def(
                name.clone(),
                ty,
                eval(arena, &ctx.tms, &ctm).map_err(ElabError::from_eval_error)?,
            );

            match rest {
                [] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Let {
                        tm: ctm,
                        next: Arc::new(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
                    },
                )),

                [next, rest @ ..] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Let {
                        tm: ctm,
                        next: Arc::new(elab_stmt(arena, &new_ctx, next, rest)?),
                    },
                )),
            }
        }
        StmtData::Out { tm0, tm1 } => {
            // todo: check types here if we like (LHS is portable, RHS is handler)
            let (ctm0, _) = infer_tm(arena, ctx, tm0)?;
            let (ctm1, _) = infer_tm(arena, ctx, tm1)?;

            match rest {
                [] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Out {
                        tm0: ctm0,
                        tm1: ctm1,
                        next: Arc::new(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
                    },
                )),

                [next, rest @ ..] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Out {
                        tm0: ctm0,
                        tm1: ctm1,
                        next: Arc::new(elab_stmt(arena, ctx, next, rest)?),
                    },
                )),
            }
        }
        StmtData::If { branches } => match rest {
            [] => Ok(core::Stmt::new(
                stmt.location.clone(),
                core::StmtData::If {
                    branches: branches
                        .iter()
                        .map(|branch| elab_branch(arena, ctx, branch))
                        .collect::<Result<Vec<_>, _>>()?,
                    next: Arc::new(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
                },
            )),

            [next, rest @ ..] => Ok(core::Stmt::new(
                stmt.location.clone(),
                core::StmtData::If {
                    branches: branches
                        .iter()
                        .map(|branch| elab_branch(arena, ctx, branch))
                        .collect::<Result<Vec<_>, _>>()?,
                    next: Arc::new(elab_stmt(arena, ctx, next, rest)?),
                },
            )),
        },
    }
}

fn elab_branch<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    branch: &'a Branch,
) -> Result<core::Branch<'a>, ElabError> {
    match &branch.data {
        BranchData::Bool { tm, stmt } => {
            let (ctm, ty) = infer_tm(arena, ctx, tm)?;
            // check that the type of the guard is Bool!
            equate_ty(&tm.location, ty, &core::Val::BoolTy)?;

            Ok(core::Branch::new(
                branch.location.clone(),
                core::BranchData::Bool {
                    tm: ctm,
                    stmt: elab_stmt(arena, ctx, stmt, &[])?,
                },
            ))
        }
        BranchData::Is { tm, branches } => {
            // need to also check that each branch's pattern's ty matches the ty of tm
            let (core_tm, core_ty) = infer_tm(arena, ctx, tm)?;

            let mut final_ty = core::Val::AnyTy;

            let mut final_branches = vec![];
            for branch in branches {
                let (core_branch, core_branch_ty) =
                    infer_pattern_branch(arena, ctx, branch, core_ty)?;

                // check that we can equate the branches
                equate_ty(&branch.location, &core_branch_ty, &final_ty)?;
                // then, hold on to whichever is more precise
                final_ty = final_ty.most_precise(arena, &core_branch_ty);
                // and keep all the core branches
                final_branches.push(core_branch);
            }

            // then, we could check for full coverage / completeness ... but we don't have to
            // because these are allowed to not produce any output!

            Ok(core::Branch::new(
                tm.location.clone(),
                core::BranchData::Is {
                    tm: core_tm,
                    branches: final_branches,
                },
            ))
        }
    }
}

fn infer_pattern_branch<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    branch: &'a PatternBranch,
    ty: &'a core::Val<'a>,
) -> Result<(core::PatternBranch<'a>, core::Val<'a>), ElabError> {
    // just check the type of the pattern itself
    let (matcher, ty, bind_tys) = infer_pattern(arena, ctx, &branch.data.pat, ty)?;

    let stmt = elab_stmt(
        arena,
        // bind everything from the pattern when elaborating the statement
        &bind_tys.iter().fold(ctx.clone(), |ctx0, (name, val)| {
            ctx0.bind_param(name.clone(), val, arena)
        }),
        &branch.data.stmt,
        &[],
    )?;

    Ok((
        core::PatternBranch::new(
            branch.location.clone(),
            core::PatternBranchData { matcher, stmt },
        ),
        ty,
    ))
}

fn infer_pattern<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    pattern: &'a Pattern,
    ty: &'a core::Val<'a>,
) -> Result<
    (
        Arc<dyn core::matcher::Matcher<'a> + 'a>,
        core::Val<'a>,
        Vec<(String, &'a core::Val<'a>)>,
    ),
    ElabError,
> {
    match &pattern.data {
        PatternData::Named { name, pattern } => {
            let (matcher, ty, binds) = infer_pattern(arena, ctx, pattern, ty)?;

            let mut new_binds = binds;
            new_binds.push((name.clone(), arena.alloc(ty.clone())));

            Ok((matcher, ty, new_binds))
        }
        PatternData::Hole => Ok((
            Arc::new(core::matcher::Succeed {}),
            core::Val::AnyTy,
            vec![],
        )),
        PatternData::BoolLit { b } => Ok((
            Arc::new(core::matcher::Equal::new(
                arena.alloc(core::Val::Bool { b: *b }),
            )),
            core::Val::BoolTy,
            vec![],
        )),
        PatternData::NumLit { n } => Ok((
            Arc::new(core::matcher::Equal::new(
                arena.alloc(core::Val::Num { n: n.get_float() }),
            )),
            core::Val::NumTy,
            vec![],
        )),
        PatternData::StrLit { s } => Ok((
            Arc::new(core::matcher::Equal::new(arena.alloc(core::Val::Str {
                s: arena.alloc(s.clone()),
            }))),
            core::Val::StrTy,
            vec![],
        )),
        PatternData::RecLit { fields } => {
            // for each field, infer the pattern
            let (matcher, tys, names): (
                Arc<dyn core::matcher::Matcher<'a> + 'a>,
                Vec<CoreRecField<&'a core::Val>>,
                Vec<(String, &core::Val)>,
            ) = fields.iter().try_fold(
                (
                    Arc::new(core::matcher::Succeed {}) as Arc<dyn core::matcher::Matcher>,
                    vec![],
                    vec![],
                ),
                |(acc, tys, names), field| {
                    let (m1, ty1, names1) = infer_pattern(arena, ctx, &field.data, ty)?;

                    let mut tys1 = tys.clone();
                    tys1.push(CoreRecField::new(
                        arena.alloc(field.name.as_bytes().to_vec()),
                        arena.alloc(ty1) as &core::Val<'a>,
                    ));

                    let names = names.iter().chain(&names1).cloned().collect::<Vec<_>>();

                    Ok((
                        Arc::new(core::matcher::Chain {
                            m1: Arc::new(core::matcher::FieldAccess {
                                name: field.name.clone(),
                                inner: m1,
                            }),
                            m2: acc,
                        }) as Arc<dyn core::matcher::Matcher>,
                        tys1,
                        names,
                    ))
                },
            )?;

            Ok((matcher, core::Val::RecTy { fields: tys }, names))
        }

        PatternData::Read { regs, binds, error } => {
            // typecheck all the binds;
            // end up with the name of each bind, the (list) value that it is SELECTING from,
            // and the type of it itself
            let params = binds
                .iter()
                .map(|param| {
                    let (ctm, cty) = infer_tm(arena, ctx, &param.data.tm)?;
                    let vtm = eval(arena, &ctx.tms, &ctm).map_err(ElabError::from_eval_error)?;

                    match cty {
                        core::Val::ListTy { ty } => Ok((param.data.name.clone(), vtm, *ty)),
                        _ => Err(ElabError::new(&param.location, "hello")),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            // this should be enough information to walk the regions and typecheck them

            let (matcher, named) = infer_read_pattern(arena, &ctx, regs, params.clone(), *error)?;

            let new_binds = params
                .iter()
                .map(|(name, _, ty)| (name.clone(), *ty))
                .chain(
                    named
                        .iter()
                        .map(|name| (name.clone(), ty))
                        .collect::<Vec<_>>(),
                )
                .collect::<Vec<_>>();

            // need to get a vector of just the tys of each param and each bind
            // which should be constant across worlds

            Ok((
                Arc::new(matcher),
                core::Val::RecWithTy {
                    fields: vec![CoreRecField::new(
                        *arena.alloc(b"seq") as &[u8],
                        arena.alloc(core::Val::StrTy),
                    )],
                },
                new_binds,
            ))
        }
    }
}

#[derive(Clone)]
enum Reg<'a> {
    Hole,
    Exp(Vec<String>, core::Tm<'a>),
}

fn flatten_regs<'a>(
    binds: &[String],
    bind_tys: &HashMap<String, &'a core::Val<'a>>,
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

fn infer_read_pattern<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    regs: &'a [Region],
    params: Vec<(String, &'a core::Val<'a>, &'a core::Val<'a>)>,
    error: f32,
) -> Result<(matcher::ReadMatcher<'a>, Vec<String>), ElabError> {
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
        println!(
            "entering check_all_fixed_lens with {}",
            known.iter().join(", ")
        );
        fn learn_new_fixed_lens<'aa>(
            known: &mut Vec<usize>,
            fixed_lens: &[(core::Tm<'aa>, Ran<usize>)],
            arena: &'aa Arena,
        ) -> Vec<OpTm<'aa>> {
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
                            tm: matcher::LocTm::Offset {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.start }),
                                offset: expr_num.clone(),
                            },
                        });
                        new_known.push(ran.end);
                    }

                    (false, true) => {
                        ops.push(OpTm::Let {
                            loc: ran.start,
                            tm: matcher::LocTm::Offset {
                                loc_tm: Arc::new(LocTm::Var { loc: ran.end }),
                                offset: core::Tm::new(
                                    expr_num.location.clone(),
                                    core::TmData::FunApp {
                                        head: Arc::new(
                                            check_tm(
                                                arena,
                                                &core::library::standard_library(arena, true),
                                                arena.alloc(Tm::new(
                                                    expr_num.location.clone(),
                                                    TmData::Name {
                                                        name: "un_minus".to_string(),
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
        matcher::ReadMatcher {
            ops: final_ops,
            binds: named.iter().map(|(_, ran)| ran.clone()).collect::<Vec<_>>(),
            end: regs.len(),
        },
        named.iter().map(|(name, _)| name.clone()).collect(),
    ))
}

fn infer_read_region<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    reg: &Region,
    params: Vec<(String, &'a core::Val<'a>, &'a core::Val<'a>)>,
) {
    match &reg.data {
        RegionData::Hole => todo!(),
        RegionData::Term { tm } => todo!(),
        RegionData::Named { name, regs } => todo!(),
        RegionData::Sized { tm, regs } => todo!(),
    }
}

fn equate_ty<'a>(
    location: &Location,
    ty1: &core::Val<'a>,
    ty2: &core::Val<'a>,
) -> Result<(), ElabError> {
    if ty1.equiv(ty2) {
        Ok(())
    } else {
        Err(ElabError::new(
            location,
            &format!("mismatched types: expected {}, found {}", ty2, ty1),
        ))
    }
}

fn check_tm<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    tm: &'a Tm,
    expected_ty: &core::Val<'a>,
) -> Result<core::Tm<'a>, ElabError> {
    match (&tm.data, expected_ty) {
        (_, _) => {
            let (tm1, ty1) = infer_tm(arena, ctx, tm)?;
            equate_ty(&tm.location, &ty1, expected_ty)?;
            Ok(tm1)
        }
    }
}

fn infer_tm<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    tm: &'a Tm,
) -> Result<(core::Tm<'a>, &'a core::Val<'a>), ElabError> {
    match &tm.data {
        TmData::BoolLit { b } => Ok((
            core::Tm::new(tm.location.clone(), core::TmData::BoolLit { b: *b }),
            arena.alloc(core::Val::BoolTy),
        )),
        TmData::NumLit { n } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::NumLit { n: n.get_float() },
            ),
            arena.alloc(core::Val::NumTy),
        )),
        TmData::StrLit { regs } => Ok((
            elab_str_lit_regs(arena, ctx, &tm.location, &regs[..])?,
            arena.alloc(core::Val::StrTy),
        )),
        TmData::RecTy { fields } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::RecTy {
                    fields: fields
                        .iter()
                        .map(|field| {
                            Ok(crate::util::CoreRecField::new(
                                field.name.as_bytes(),
                                check_tm(arena, ctx, &field.data, &core::Val::Univ)?,
                            ))
                        })
                        .collect::<Result<Vec<_>, ElabError>>()?,
                },
            ),
            arena.alloc(core::Val::Univ),
        )),
        TmData::RecLit { fields } => {
            let mut tms = vec![];
            let mut tys = vec![];

            for field in fields {
                let (tm, ty) = infer_tm(arena, ctx, &field.data)?;

                tms.push(CoreRecField::new(field.name.as_bytes(), tm));
                tys.push(CoreRecField::new(
                    field.name.as_bytes(),
                    arena.alloc(ty) as &core::Val<'a>,
                ));
            }

            Ok((
                core::Tm::new(tm.location.clone(), core::TmData::RecLit { fields: tms }),
                arena.alloc(core::Val::RecTy { fields: tys }),
            ))
        }
        TmData::RecProj { tm: head_tm, name } => {
            let (head_tm, ty) = infer_tm(arena, ctx, head_tm)?;

            match ty {
                core::Val::RecTy { fields } => {
                    match fields.iter().find(|field| field.name.eq(name.as_bytes())) {
                        Some(field) => Ok((
                            core::Tm::new(
                                tm.location.clone(),
                                core::TmData::RecProj {
                                    tm: Arc::new(head_tm),
                                    name: name.as_bytes(),
                                },
                            ),
                            field.data,
                        )),
                        None => Err(ElabError::new(
                            &tm.location,
                            "trying to access non-existent field",
                        )),
                    }
                }
                // todo: improve this message
                _ => Err(ElabError::new(
                    &tm.location,
                    "trying to access field of a non-record",
                )),
            }
        }
        TmData::FunTy { args, body } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::FunTy {
                    args: args
                        .iter()
                        .map(|arg| check_tm(arena, ctx, arg, &core::Val::Univ))
                        .collect::<Result<Vec<_>, ElabError>>()?,
                    body: Arc::new(check_tm(arena, ctx, body, &core::Val::Univ)?),
                },
            ),
            arena.alloc(core::Val::Univ),
        )),
        TmData::FunLit { args, body } => {
            // then, make sure each param has a type associated which is actually a type
            let param_tms = args
                .iter()
                .map(|arg| {
                    Ok((
                        arg.name.clone(),
                        check_tm(arena, ctx, &arg.ty, &core::Val::Univ)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let arg_tms = param_tms.iter().map(|(_, a)| a.clone()).collect::<Vec<_>>();
            let arg_vals = arg_tms
                .clone()
                .iter()
                .map(|arg| core::eval(arena, &ctx.tms, arg))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ElabError::from_eval_error)?;

            // create a new context with all the parameters bound
            let new_ctx = param_tms
                .clone()
                .iter()
                .try_fold(ctx.clone(), |ctx0, (name, ty)| {
                    // evaluate the type
                    let val =
                        core::eval(arena, &ctx.tms, ty).map_err(ElabError::from_eval_error)?;
                    // bind it in the context
                    Ok(ctx0.bind_param(name.clone(), val, arena))
                })?;

            // then, get the type of the body
            // (note that this WON'T be FunReturnTyAwaiting, as it's all inferred;
            // that only happens in the return type of foreign functions, which are specified)
            let (body_tm, body_ty) = infer_tm(arena, &new_ctx, body)?;

            Ok((
                core::Tm::new(
                    tm.location.clone(),
                    core::TmData::FunLit {
                        args: arg_tms,
                        body: Arc::new(body_tm),
                    },
                ),
                body_ty,
            ))
        }
        TmData::FunLitForeign { args, ty, name } => {
            // then, make sure each param has a type associated which is actually a type
            let param_tms = args
                .iter()
                .map(|arg| {
                    Ok((
                        arg.name.clone(),
                        check_tm(arena, ctx, &arg.ty, &core::Val::Univ)?,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()?;

            let arg_tms = param_tms.iter().map(|(_, a)| a.clone()).collect::<Vec<_>>();
            let arg_vals = arg_tms
                .clone()
                .iter()
                .map(|arg| core::eval(arena, &ctx.tms, arg))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ElabError::from_eval_error)?;

            // create a new context with all the parameters bound
            let new_ctx = param_tms
                .clone()
                .iter()
                .try_fold(ctx.clone(), |ctx0, (name, ty)| {
                    // evaluate the type
                    let val =
                        core::eval(arena, &ctx.tms, ty).map_err(ElabError::from_eval_error)?;
                    // bind it in the context
                    Ok(ctx0.bind_param(name.clone(), val, arena))
                })?;

            // then, get the type of the body
            // (note that this might be FunReturnTyAwaiting, as it's all inferred;
            // that only happens in the return type of foreign functions, which ar specified)
            let ty = check_tm(arena, &new_ctx, ty, &core::Val::Univ)?;
            let val = core::eval(arena, &new_ctx.tms, &ty).map_err(ElabError::from_eval_error)?;
            let final_ty = match val {
                core::Val::Neutral { neutral } => {
                    // if it's neutral, we need to create a val that is just a function frome some arguments
                    // to a new val (which we expect to be concrete)
                    arena.alloc(core::Val::FunReturnTyAwaiting {
                        data: core::FunData {
                            env: ctx.tms.clone(),
                            body: ty,
                        },
                    })
                }
                // otherwise just send through whatever the value is
                _ => val,
            };

            Ok((
                core::Tm::new(
                    tm.location.clone(),
                    core::TmData::FunForeignLit {
                        args: arg_tms,
                        body: library::foreign(&tm.location, name)
                            .map_err(ElabError::from_eval_error)?,
                    },
                ),
                final_ty,
            ))
        }
        TmData::FunApp { head, args } => {
            let (head_tm, head_ty) = infer_tm(arena, ctx, head)?;

            match head_ty {
                core::Val::FunTy {
                    args: args_ty,
                    body,
                } => {
                    // first make sure the argument lists are the same length
                    if !args.len().eq(&args_ty.len()) {
                        return Err(ElabError::new(
                            &tm.location,
                            &format!(
                                "function was given {} arguments, expected {}",
                                args.len(),
                                args_ty.len()
                            ),
                        ));
                    }

                    // elaborate each argument, and make sure they all
                    // correspond to the function's argument type
                    // - and if any are neutrals, the whole thing is neutral

                    let arg_tms = args
                        .iter()
                        .zip(args_ty)
                        .map(|(arg, arg_ty)| {
                            let arg_tm = check_tm(arena, ctx, arg, arg_ty)?;
                            Ok(arg_tm)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // check if the return type needs to be evaluated
                    let body_ty = match body {
                        core::Val::FunReturnTyAwaiting { data } => {
                            let arg_vals = arg_tms
                                .iter()
                                .map(|arg| core::eval(arena, &ctx.tms, arg))
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(ElabError::from_eval_error)?;

                            // apply the function to get the concrete type out
                            let ty_val = data
                                .app(arena, &arg_vals)
                                .map_err(ElabError::from_eval_error)?;

                            ty_val
                        }
                        _ => body,
                    };

                    println!("body ty is {}", body_ty);
                    Ok((
                        core::Tm::new(
                            tm.location.clone(),
                            core::TmData::FunApp {
                                head: Arc::new(head_tm),
                                args: arg_tms,
                            },
                        ),
                        body_ty,
                    ))
                }
                // todo: add more info to this error message
                _ => Err(ElabError::new(
                    &tm.location,
                    "trying to apply something as a function when it's not",
                )),
            }
        }
        TmData::Name { name } => match ctx.lookup(name.clone()) {
            Some((index, ty)) => Ok((
                core::Tm::new(tm.location.clone(), core::TmData::Var { index }),
                ty,
            )),
            None => Err(ElabError::new(&tm.location, "unbound name")),
        },
        TmData::BinOp { tm0, tm1, op } => infer_bin_op(op, arena, ctx, &tm.location, tm0, tm1),
        TmData::UnOp { tm, op } => infer_un_op(op, arena, ctx, &tm.location, tm),
    }
}

/// Takes a binary operator and the types of its two arguments,
/// returns the name of the function that it corresponds to
/// and the return type.
/// Checks that the input types are appropriate.
///
/// This function is where any polymorphism of operators
/// (an operator referring to different functions depending on input types)
/// would be provided.
fn infer_bin_op<'a>(
    bin_op: &BinOp,
    arena: &'a Arena,
    ctx: &Context<'a>,
    location: &Location,
    tm0: &'a Tm,
    tm1: &'a Tm,
) -> Result<(core::Tm<'a>, &'a core::Val<'a>), ElabError> {
    let non_parametric_operator = |ty0: &core::Val<'a>,
                                   ty1: &core::Val<'a>,
                                   name: &str|
     -> Result<(core::Tm<'a>, &'a core::Val<'a>), ElabError> {
        let ctm0 = check_tm(arena, ctx, tm0, ty0)?;
        let ctm1 = check_tm(arena, ctx, tm1, ty1)?;

        match ctx.lookup(name.to_string()) {
            Some((index, ty)) => match ty {
                core::Val::FunTy { args, body } => Ok((
                    core::Tm::new(
                        location.clone(),
                        core::TmData::FunApp {
                            head: Arc::new(core::Tm::new(
                                location.clone(),
                                core::TmData::Var { index },
                            )),
                            args: vec![ctm0, ctm1],
                        },
                    ),
                    body,
                )),

                _ => panic!("operator has non-function type?!"),
            },
            None => Err(ElabError::new(location, "unbound name")),
        }
    };

    match bin_op {
        BinOp::Plus => non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_plus"),
        BinOp::Times => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_times")
        }
        BinOp::Minus => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_minus")
        }
        BinOp::And => non_parametric_operator(&core::Val::BoolTy, &core::Val::BoolTy, "binary_and"),
        BinOp::Or => non_parametric_operator(&core::Val::BoolTy, &core::Val::BoolTy, "binary_or"),
        BinOp::Equal => {
            non_parametric_operator(&core::Val::AnyTy, &core::Val::AnyTy, "binary_equal")
        }
        BinOp::NotEqual => {
            non_parametric_operator(&core::Val::AnyTy, &core::Val::AnyTy, "binary_not_equal")
        }
        BinOp::LessThan => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_less_than")
        }
        BinOp::GreaterThan => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_greater_than")
        }
        BinOp::LessThanOrEqual => non_parametric_operator(
            &core::Val::NumTy,
            &core::Val::NumTy,
            "binary_less_than_or_equal",
        ),
        BinOp::GreaterThanOrEqual => non_parametric_operator(
            &core::Val::NumTy,
            &core::Val::NumTy,
            "binary_greater_than_or_equal",
        ),
        BinOp::Division => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_division")
        }
        BinOp::Modulo => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_modulo")
        }
    }
}

fn infer_un_op<'a>(
    un_op: &UnOp,
    arena: &'a Arena,
    ctx: &Context<'a>,
    location: &Location,
    tm: &'a Tm,
) -> Result<(core::Tm<'a>, &'a core::Val<'a>), ElabError> {
    let non_parametric_operator =
        |ty0: &core::Val<'a>, name: &str| -> Result<(core::Tm<'a>, &'a core::Val<'a>), ElabError> {
            let ctm0 = check_tm(arena, ctx, tm, ty0)?;

            match ctx.lookup(name.to_string()) {
                Some((index, ty)) => Ok((
                    core::Tm::new(
                        location.clone(),
                        core::TmData::FunApp {
                            head: Arc::new(core::Tm::new(
                                location.clone(),
                                core::TmData::Var { index },
                            )),
                            args: vec![ctm0],
                        },
                    ),
                    ty,
                )),
                None => Err(ElabError::new(location, "unbound name")),
            }
        };

    match un_op {
        // since this operator is parametric, it is slightly custom
        UnOp::Minus => {
            let (ctm0, cty0) = infer_tm(arena, ctx, tm)?;

            match cty0 {
                core::Val::NumTy => match ctx.lookup("unary_minus".to_string()) {
                    Some((index, ty)) => Ok((
                        core::Tm::new(
                            location.clone(),
                            core::TmData::FunApp {
                                head: Arc::new(core::Tm::new(
                                    location.clone(),
                                    core::TmData::Var { index },
                                )),
                                args: vec![ctm0],
                            },
                        ),
                        ty,
                    )),
                    None => Err(ElabError::new(location, "unbound name")),
                },
                core::Val::StrTy => match ctx.lookup("unary_reverse_complement".to_string()) {
                    Some((index, ty)) => Ok((
                        core::Tm::new(
                            location.clone(),
                            core::TmData::FunApp {
                                head: Arc::new(core::Tm::new(
                                    location.clone(),
                                    core::TmData::Var { index },
                                )),
                                args: vec![ctm0],
                            },
                        ),
                        ty,
                    )),
                    None => Err(ElabError::new(location, "unbound name")),
                },
                core::Val::FunReturnTyAwaiting { .. } => panic!(
                    "trying to apply an operation on a value whose type is not yet defined; todo"
                ),
                _ => Err(ElabError::new(
                    &ctm0.location,
                    &format!("mismatched types: expected Num or Str, got {}", cty0),
                )),
            }
        }
    }
}

fn elab_str_lit_regs<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    location: &Location,
    regs: &'a [StrLitRegion],
) -> Result<core::Tm<'a>, ElabError> {
    match regs {
        [reg, rest @ ..] => {
            match rest {
                // handle the last region differently
                [] => elab_str_lit_reg(arena, ctx, reg),

                // otherwise, recursive case
                rest => Ok(core::Tm::new(
                    location.clone(),
                    core::TmData::FunApp {
                        head: Arc::new(check_tm(
                            arena,
                            ctx,
                            arena.alloc(Tm::new(
                                reg.location.clone(),
                                TmData::Name {
                                    name: "str_concat".to_string(),
                                },
                            )),
                            &core::Val::StrTy,
                        )?),
                        args: vec![
                            elab_str_lit_reg(arena, ctx, reg)?,
                            elab_str_lit_regs(arena, ctx, location, rest)?,
                        ],
                    },
                )),
            }
        }

        // if we started with an empty region
        [] => Ok(core::Tm::new(
            location.clone(),
            core::TmData::StrLit {
                s: arena.alloc(vec![]),
            },
        )),
    }
}

fn elab_str_lit_reg<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    reg: &'a StrLitRegion,
) -> Result<core::Tm<'a>, ElabError> {
    match &reg.data {
        StrLitRegionData::Str { s } => Ok(core::Tm::new(
            reg.location.clone(),
            core::TmData::StrLit { s },
        )),
        StrLitRegionData::Tm { tm } => {
            // formatting a string is sugar for adding implicit type conversion
            let wrapped_tm = Tm::new(
                reg.location.clone(),
                TmData::FunApp {
                    head: Rc::new(Tm::new(
                        reg.location.clone(),
                        TmData::Name {
                            name: "to_str".to_string(),
                        },
                    )),
                    args: vec![tm.clone()],
                },
            );

            // is this arena allocation a bit rogue? this is not what this arena was meant for...
            // should one arena have one distinct semantic content?
            check_tm(arena, ctx, arena.alloc(wrapped_tm), &core::Val::StrTy)
        }
    }
}
