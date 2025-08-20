use std::{collections::HashMap, fmt::Display, io::Read, ops::Deref, rc::Rc, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{
        self, library,
        matcher::read_matcher::{self, LocTm, OpTm},
        EvalError,
    },
    parse,
    read::FileType,
    util::{bytes_to_string, Arena, Cache, CoreRecField, Env, Located, Location, Ran, RecField},
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
    pub fn new(location: &Location, message: &str) -> ElabError {
        ElabError {
            location: location.clone(),
            message: message.to_string(),
        }
    }

    pub fn new_unbound_name(location: &Location, name: &str) -> ElabError {
        ElabError {
            location: location.clone(),
            message: format!("unbound name '{}'", name),
        }
    }

    pub fn new_non_existent_field_access<'a>(
        location: &Location,
        name: &str,
        ty: &core::Val<'a>,
    ) -> ElabError {
        ElabError {
            location: location.clone(),
            message: format!(
                "trying to access non-existent field '{}' from a record with fields {}",
                name, ty,
            ),
        }
    }

    pub fn from_eval_error(eval_error: EvalError) -> ElabError {
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
    Tm { tm: Tm },

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

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum MatchMode {
    All,
    AllBest,
    OneBest,
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
        mode: MatchMode,
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
    Term { tm: Tm, error: Tm },

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
    RecWithTy {
        fields: Vec<RecField<Tm>>,
    },
    RecLit {
        fields: Vec<RecField<Tm>>,
    },
    RecProj {
        tm: Rc<Tm>,
        name: String,
    },

    ListTy {
        tm: Rc<Tm>,
    },
    ListLit {
        tms: Vec<Tm>,
    },

    FunTy {
        args: Vec<Tm>,
        opts: Vec<OptParam>,
        body: Rc<Tm>,
    },
    FunLit {
        args: Vec<Param>,
        opts: Vec<OptParam>,
        body: Rc<Tm>,
    },
    FunLitForeign {
        args: Vec<Param>,
        opts: Vec<OptParam>,
        ty: Rc<Tm>,
        name: String,
    },
    /// Function application
    FunApp {
        head: Rc<Tm>,
        args: Vec<Tm>,
        opts: Vec<(String, Tm)>,
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
pub struct OptParam {
    pub name: String,
    pub ty: Tm,
    pub tm: Tm,
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
    Exponent,
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

#[derive(Clone, Default, Debug)]
pub struct Context<'a> {
    size: usize,
    names: Env<String>,
    tys: Env<core::Val<'a>>,
    pub tms: Env<core::Val<'a>>,
}

impl<'a> Context<'a> {
    /// Returns the next variable that will be bound in the context after
    /// calling bind_def or bind_param
    pub fn next_var(&self, arena: &'a Arena) -> core::Val<'a> {
        core::Val::Neutral {
            neutral: core::Neutral::Var { level: self.size },
        }
    }

    /// Binds a definition in the context
    pub fn bind_def(&self, name: String, ty: core::Val<'a>, tm: core::Val<'a>) -> Context<'a> {
        Context {
            size: self.size + 1,
            names: self.names.with(name),
            tys: self.tys.with(ty),
            tms: self.tms.with(tm),
        }
    }

    pub fn unbind_def(&self, name: &str) -> Context<'a> {
        // name must be present
        let (index, _) = self
            .names
            .iter()
            .find_position(|name0| (*name0).eq(name))
            .unwrap();

        Context {
            size: self.size - 1,
            names: self.names.without(index),
            tys: self.tys.without(index),
            tms: self.tms.without(index),
        }
    }

    /// Binds a parameter in the context
    pub fn bind_param(&self, name: String, ty: core::Val<'a>, arena: &'a Arena) -> Context<'a> {
        self.bind_def(name, ty, self.next_var(arena))
    }

    /// Looks up a name in the context
    pub fn lookup(&self, name: String) -> Option<(usize, &core::Val<'a>)> {
        // Find the index of most recent binding in the context identified by
        // name, starting from the most recent binding. This gives us the
        // de Bruijn index of the variable.
        let index = self.size - 1 - self.names.find_last(&name)?;
        let val = self.tys.get_index(index);

        Some((index, val))
    }

    /// Binds the read to a parameter in the context
    pub fn bind_read(&self, arena: &'a Arena, filename: String) -> Context<'a> {
        // filetype must be allocated since it must survive
        let filename_bytes = arena.alloc(filename.to_string().as_bytes().to_vec());

        self.bind_param(
            "read".to_string(),
            library::read_ty(
                arena,
                &Location::new(0, 0),
                &[crate::core::Val::Str { s: filename_bytes }],
            )
            .unwrap(),
            arena,
        )
    }

    /// Binds the read to a parameter in the context,
    /// taking into account an optional read pair.
    pub fn bind_read_paired(
        &self,
        arena: &'a Arena,
        filename: String,
        paired_filetype: &Option<String>,
    ) -> Context<'a> {
        match paired_filetype {
            Some(paired_filetype) => {
                // filetype must be allocated since it must survive
                let filetype_bytes = arena.alloc(filename.to_string().as_bytes().to_vec());
                let paired_filetype_bytes =
                    arena.alloc(paired_filetype.to_string().as_bytes().to_vec());

                self.bind_param(
                    "read".to_string(),
                    core::library::paired_read_ty(
                        arena,
                        &Location::new(0, 0),
                        &[
                            core::Val::Str { s: filetype_bytes },
                            core::Val::Str {
                                s: paired_filetype_bytes,
                            },
                        ],
                    )
                    .unwrap(),
                    arena,
                )
            }
            None => self.bind_read(arena, filename),
        }
    }

    pub fn assert_unique_name(&self, location: &Location, name: &String) -> Result<(), ElabError> {
        // check that the name isn't already assigned
        if self.names.iter().contains(name) {
            Err(ElabError::new(
                &location,
                &format!(
                    "tried to name a variable '{}', but that name is already in use!",
                    name
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl<'a> Display for Context<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (0..self.size)
            .map(|i| {
                format!(
                    "{}: {} = {}",
                    self.names.get_index(i),
                    self.tys.get_index(i),
                    self.tms.get_index(i)
                )
            })
            .join("\n")
            .fmt(f)
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
        // WARN it looks like anything that comes after the group keeps all the binds from the group
        // this needs fixing
        StmtData::Group { stmts } => match &stmts[..] {
            [first_stmt, rest_stmts @ ..] => elab_stmt(
                arena,
                ctx,
                first_stmt,
                &rest_stmts
                    .iter()
                    .chain(rest.iter().copied())
                    .collect::<Vec<_>>(),
            ),
            [] => match rest {
                [first, rest @ ..] => todo!(),
                [] => Ok(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
            },
        },

        StmtData::Let { name, tm } => {
            // check that the name isn't already assigned
            ctx.assert_unique_name(&tm.location, name)?;

            let (ctm, ty) = infer_tm(arena, ctx, tm)?;
            // now we evaluate the terms as best we can, in case we need them at the static stage (we probably will)
            let new_ctx = ctx.bind_def(
                name.clone(),
                ty,
                // WARN cache can be empty because this is pre-caching
                ctm.eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                    .map_err(ElabError::from_eval_error)?,
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
        StmtData::Tm { tm } => {
            // make sure that all surface-level terms evaluate to effects
            let ctm = check_tm(arena, ctx, tm, &core::Val::EffectTy)?;

            match rest {
                [] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Tm {
                        tm: ctm,
                        next: Arc::new(core::Stmt::new(stmt.location.clone(), core::StmtData::End)),
                    },
                )),

                [next, rest @ ..] => Ok(core::Stmt::new(
                    stmt.location.clone(),
                    core::StmtData::Tm {
                        tm: ctm,
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

pub fn elab_prog_for_ctx<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    prog: &'a Prog,
) -> Result<Context<'a>, ElabError> {
    match &prog.data.stmts[..] {
        [first, rest @ ..] => elab_stmt_for_ctx(arena, ctx, first, &rest.iter().collect_vec()),
        [] => Ok(ctx.clone()),
    }
}

/// Elaborate a statement, but just return the context generated
/// This is for processing the standard library, and any other modules that
/// need to be read prior to reading the user's code.
fn elab_stmt_for_ctx<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    stmt: &'a Stmt,
    rest: &[&'a Stmt],
) -> Result<Context<'a>, ElabError> {
    match &stmt.data {
        // any time a let is encountered, make the bind and carry it through to the next statements
        StmtData::Let { name, tm } => {
            let (ctm, ty) = infer_tm(arena, ctx, tm)?;

            // now we evaluate the terms as best we can, in case we need them at the static stage (we probably will)
            let new_ctx = ctx.bind_def(
                name.clone(),
                ty,
                ctm.eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                    .map_err(ElabError::from_eval_error)?,
            );

            match rest {
                [] => Ok(new_ctx),

                [next, rest @ ..] => elab_stmt_for_ctx(arena, &new_ctx, next, rest),
            }
        }

        // for any of the other stmt types,
        // there's no chance of making a binding at this level
        // so we can just move on
        _ => match rest {
            [next, rest @ ..] => elab_stmt_for_ctx(arena, ctx, next, rest),
            [] => Ok(ctx.clone()),
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
            equate_ty(&tm.location, &ty, &core::Val::BoolTy)?;

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
                    infer_pattern_branch(arena, ctx, branch, core_ty.clone())?;

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
    ty: core::Val<'a>,
) -> Result<(core::PatternBranch<'a>, core::Val<'a>), ElabError> {
    // just check the type of the pattern itself
    let (matcher, ty1, bind_tys) = infer_pattern(arena, ctx, &branch.data.pat, ty.clone())?;
    // make sure that it aligns with the type that you're putting in
    equate_ty(&branch.data.pat.location, &ty1, &ty)?;

    let stmt = elab_stmt(
        arena,
        // bind everything from the pattern when elaborating the statement
        &bind_tys.into_iter().fold(ctx.clone(), |ctx0, (name, val)| {
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
        ty1,
    ))
}

fn elab_match_mode(mode: &MatchMode) -> Result<core::matcher::read_matcher::MatchMode, ElabError> {
    Ok(match mode {
        MatchMode::All => core::matcher::read_matcher::MatchMode::All,
        MatchMode::AllBest => core::matcher::read_matcher::MatchMode::One,
        MatchMode::OneBest => core::matcher::read_matcher::MatchMode::Unique,
    })
}

fn infer_pattern<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    pattern: &'a Pattern,
    ty: core::Val<'a>,
) -> Result<
    (
        Arc<dyn core::matcher::Matcher<'a> + 'a>,
        core::Val<'a>,
        Vec<(String, core::Val<'a>)>,
    ),
    ElabError,
> {
    match &pattern.data {
        PatternData::Named { name, pattern } => {
            // check that the name isn't already assigned
            ctx.assert_unique_name(&pattern.location, name)?;

            let (matcher, ty, binds) = infer_pattern(arena, ctx, pattern, ty)?;

            let mut new_binds = binds;
            new_binds.push((name.clone(), ty.clone()));

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
            // first, check that all the names are new
            fields.iter().map(|field|
                // check that the name isn't already assigned
                match ctx.names.iter().contains(&field.name) {
                    true => Err(ElabError::new(
                                        &pattern.location,
                                        &format!(
                                            "tried to bind a variable to name '{}', but that name is already in use!",
                                            field.name
                                        ),
                                    )),
                    false => Ok(()),
                }
                ).collect::<Result<Vec<_>,_>>()?;

            // for each field, infer the pattern
            let (matcher, tys, names): (
                Arc<dyn core::matcher::Matcher<'a> + 'a>,
                Vec<CoreRecField<core::Val>>,
                Vec<(String, core::Val)>,
            ) = fields.iter().try_fold(
                (
                    Arc::new(core::matcher::Succeed {}) as Arc<dyn core::matcher::Matcher>,
                    vec![],
                    vec![],
                ),
                |(acc, tys, names), field| {
                    let (m1, ty1, names1) = infer_pattern(arena, ctx, &field.data, ty.clone())?;

                    let mut tys1 = tys.clone();
                    tys1.push(CoreRecField::new(
                        arena.alloc(field.name.as_bytes().to_vec()),
                        ty1 as core::Val<'a>,
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

        PatternData::Read { regs, binds, mode } => {
            // typecheck all the binds;
            // end up with the name of each bind, the (list) value that it is SELECTING from,
            // and the type of it itself
            let params = binds
                .iter()
                .map(|param| {
                    let (ctm, cty) = infer_tm(arena, ctx, &param.data.tm)?;
                    let vtm = ctm
                        // WARN cache can be empty because this is pre-caching
                        .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                        .map_err(ElabError::from_eval_error)?;

                    match cty {
                        core::Val::ListTy { ty } => {
                            Ok((param.data.name.clone(), vtm, (*ty).clone()))
                        }
                        _ => Err(ElabError::new(&param.location, "hello")),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            // this should be enough information to walk the regions and typecheck them

            let (matcher, named) = core::matcher::read_matcher::infer_read_pattern(
                arena,
                ctx,
                regs,
                params.clone(),
                &elab_match_mode(mode)?,
            )?;

            let new_binds = params
                .into_iter()
                .map(|(name, _, ty)| (name.clone(), ty))
                .chain(
                    named
                        .iter()
                        .map(|name| (name.clone(), ty.clone()))
                        .collect::<Vec<_>>(),
                )
                .collect::<Vec<_>>();

            // assert that all the names are unique
            new_binds
                .iter()
                .map(|(name, _)| ctx.assert_unique_name(&pattern.location, name))
                .collect::<Result<Vec<_>, ElabError>>()?;

            // need to get a vector of just the tys of each param and each bind
            // which should be constant across worlds

            Ok((
                Arc::new(matcher),
                core::Val::RecWithTy {
                    fields: vec![CoreRecField::new(
                        *arena.alloc(b"seq") as &[u8],
                        core::Val::StrTy,
                    )],
                },
                new_binds,
            ))
        }
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

pub fn check_tm<'a>(
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

pub fn infer_tm<'a>(
    arena: &'a Arena,
    ctx: &Context<'a>,
    tm: &'a Tm,
) -> Result<(core::Tm<'a>, core::Val<'a>), ElabError> {
    match &tm.data {
        TmData::BoolLit { b } => Ok((
            core::Tm::new(tm.location.clone(), core::TmData::BoolLit { b: *b }),
            core::Val::BoolTy,
        )),
        TmData::NumLit { n } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::NumLit { n: n.get_float() },
            ),
            core::Val::NumTy,
        )),
        TmData::StrLit { regs } => Ok((
            elab_str_lit_regs(arena, ctx, &tm.location, &regs[..])?,
            core::Val::StrTy,
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
            core::Val::Univ,
        )),
        TmData::RecWithTy { fields } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::RecWithTy {
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
            core::Val::Univ,
        )),
        TmData::RecLit { fields } => {
            let mut tms = vec![];
            let mut tys = vec![];

            for field in fields {
                let (tm, ty) = infer_tm(arena, ctx, &field.data)?;

                tms.push(CoreRecField::new(field.name.as_bytes(), tm));
                tys.push(CoreRecField::new(
                    field.name.as_bytes(),
                    ty as core::Val<'a>,
                ));
            }

            Ok((
                core::Tm::new(tm.location.clone(), core::TmData::RecLit { fields: tms }),
                core::Val::RecTy { fields: tys },
            ))
        }
        TmData::RecProj { tm: head_tm, name } => {
            let (head_tm, ty) = infer_tm(arena, ctx, head_tm)?;

            match &ty {
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
                            field.data.clone(),
                        )),
                        None => Err(ElabError::new_non_existent_field_access(
                            &tm.location,
                            name,
                            &ty,
                        )),
                    }
                }
                // todo: improve this message
                _ => Err(ElabError::new(
                    &tm.location,
                    "trying to access field of a value which doesn't have any fields",
                )),
            }
        }

        TmData::ListTy { tm } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::ListTy {
                    ty: Arc::new(check_tm(arena, ctx, tm, &core::Val::Univ)?),
                },
            ),
            core::Val::Univ,
        )),
        TmData::ListLit { tms } => {
            // infer the type of each tm
            let tms_and_tys = tms
                .iter()
                .map(|tm| infer_tm(arena, ctx, tm))
                .collect::<Result<Vec<_>, _>>()?;

            // make sure that the types are all equivalent, and find the most precise type
            let inner_ty = tms_and_tys
                .iter()
                .try_fold(core::Val::AnyTy, |ty0, (_, ty1)| {
                    // first make sure the types are equivalent
                    equate_ty(&tm.location, &ty0, ty1)?;

                    // then return the most precise type
                    Ok(ty0.most_precise(arena, ty1))
                })?;

            let core_tms = tms_and_tys.into_iter().map(|(tm, _)| tm).collect();

            Ok((
                core::Tm::new(tm.location.clone(), core::TmData::ListLit { tms: core_tms }),
                core::Val::ListTy {
                    ty: Arc::new(inner_ty),
                },
            ))
        }

        TmData::FunTy { args, opts, body } => Ok((
            core::Tm::new(
                tm.location.clone(),
                core::TmData::FunTy {
                    args: args
                        .iter()
                        .map(|arg| check_tm(arena, ctx, arg, &core::Val::Univ))
                        .collect::<Result<Vec<_>, ElabError>>()?,
                    opts: opts
                        .iter()
                        .map(|opt| {
                            let core_ty = check_tm(arena, ctx, &opt.ty, &core::Val::Univ)?;
                            let core_ty_val = core_ty
                                .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                                .map_err(ElabError::from_eval_error)?;
                            let core_tm = check_tm(arena, ctx, &opt.tm, &core_ty_val)?;

                            Ok((opt.name.as_bytes(), core_ty, core_tm))
                        })
                        .collect::<Result<Vec<_>, ElabError>>()?,
                    body: Arc::new(check_tm(arena, ctx, body, &core::Val::Univ)?),
                },
            ),
            core::Val::Univ,
        )),
        TmData::FunLit { args, opts, body } => {
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

            // add together all the optional arguments and regular arguments
            let arg_tms = param_tms.iter().map(|(_, a)| a.clone()).collect::<Vec<_>>();
            let core_opts = opts
                .iter()
                .map(|opt| {
                    let core_ty = check_tm(arena, ctx, &opt.ty, &core::Val::Univ)?;
                    let core_ty_val = core_ty
                        .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                        .map_err(ElabError::from_eval_error)?;
                    let core_tm = check_tm(arena, ctx, &opt.tm, &core_ty_val)?;

                    Ok((opt.name.as_bytes(), core_ty_val, core_tm))
                })
                .collect::<Result<Vec<_>, ElabError>>()?;
            let opt_tms = core_opts.iter().map(|(_, _, tm)| tm).collect::<Vec<_>>();
            let all_arg_tms = arg_tms.iter().chain(opt_tms).cloned().collect::<Vec<_>>();

            let arg_vals = arg_tms
                .clone()
                .iter()
                // WARN cache can be empty because this is pre-caching
                .map(|arg| arg.eval(arena, &ctx.tms, &Cache::default(), &Env::default()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ElabError::from_eval_error)?;

            // create a new context with all the parameters bound
            let new_ctx = param_tms
                .clone()
                .iter()
                .try_fold(ctx.clone(), |ctx0, (name, ty)| {
                    // evaluate the type
                    let val = ty
                        // WARN cache can be empty because this is pre-caching
                        .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                        .map_err(ElabError::from_eval_error)?;
                    // bind it in the context
                    Ok(ctx0.bind_param(name.clone(), val, arena))
                })?;

            // then, bind all the opts too
            let new_ctx = core_opts
                .iter()
                .fold(new_ctx.clone(), |ctx0, (name, ty, core_tm)| {
                    ctx0.bind_param(bytes_to_string(name).unwrap(), (*ty).clone(), arena)
                });

            // then, get the type of the body
            // (note that this WON'T be FunReturnTyAwaiting, as it's all inferred;
            // that only happens in the return type of foreign functions, which are specified)
            let (body_tm, body_ty) = infer_tm(arena, &new_ctx, body)?;

            Ok((
                core::Tm::new(
                    tm.location.clone(),
                    core::TmData::FunLit {
                        body: Arc::new(body_tm),
                    },
                ),
                core::Val::FunTy {
                    args: arg_vals,
                    opts: core_opts,
                    body: Arc::new(body_ty),
                },
            ))
        }
        TmData::FunLitForeign {
            args,
            opts,
            ty,
            name,
        } => {
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
            let core_opts = opts
                .iter()
                .map(|opt| {
                    let core_ty = check_tm(arena, ctx, &opt.ty, &core::Val::Univ)?;
                    let core_ty_val = core_ty
                        .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                        .map_err(ElabError::from_eval_error)?;
                    let core_tm = check_tm(arena, ctx, &opt.tm, &core_ty_val)?;

                    Ok((opt.name.as_bytes(), core_ty_val, core_tm))
                })
                .collect::<Result<Vec<_>, ElabError>>()?;

            let arg_vals = arg_tms
                .clone()
                .iter()
                // WARN cache can be empty because this is pre-caching
                .map(|arg| arg.eval(arena, &ctx.tms, &Cache::default(), &Env::default()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ElabError::from_eval_error)?;

            // create a new context with all the parameters bound
            let new_ctx = param_tms
                .clone()
                .iter()
                .try_fold(ctx.clone(), |ctx0, (name, ty)| {
                    // evaluate the type
                    let val = ty
                        .eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                        .map_err(ElabError::from_eval_error)?;
                    // bind it in the context
                    Ok(ctx0.bind_param(name.clone(), val, arena))
                })?;

            // then, bind all the opts too
            let new_ctx = core_opts
                .iter()
                .fold(new_ctx.clone(), |ctx0, (name, ty, core_tm)| {
                    ctx0.bind_param(bytes_to_string(name).unwrap(), (*ty).clone(), arena)
                });

            // then, get the type of the body
            // (note that this MIGHT be FunReturnTyAwaiting, as it may include parameters)
            let ty = check_tm(arena, &new_ctx, ty, &core::Val::Univ)?;
            let val = ty
                .eval(arena, &new_ctx.tms, &Cache::default(), &Env::default())
                .map_err(ElabError::from_eval_error)?;
            let final_ty = if val.is_neutral() {
                // if it's neutral, we throw it away - need to create a val that is just a function frome some arguments
                // to a new val (which we expect to be concrete)
                core::Val::FunReturnTyAwaiting {
                    data: core::FunData {
                        env: ctx
                            .tms
                            .iter()
                            .fold(Env::default(), |env0, val| env0.with((*val).clone())),
                        body: ty.clone(),
                    },
                }
            } else {
                // otherwise just send through whatever the value is
                val
            };

            Ok((
                core::Tm::new(
                    tm.location.clone(),
                    core::TmData::FunForeignLit {
                        body: library::foreign(&tm.location, name)
                            .map_err(ElabError::from_eval_error)?,
                    },
                ),
                core::Val::FunTy {
                    args: arg_vals,
                    opts: core_opts,
                    body: Arc::new(final_ty),
                },
            ))
        }
        TmData::FunApp { head, args, opts } => {
            let (head_tm, head_ty) = infer_tm(arena, ctx, head)?;

            match head_ty {
                core::Val::FunTy {
                    args: args_ty,
                    opts: opts_ty,
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

                    // then, make sure that every optional argument was one of the ones provided for by the type
                    opts.iter().map(|(name, _)| {
                        match opts_ty
                            .iter()
                            .find(|(name1, _, _)| (*name1).eq(name.as_bytes()))
                        {
                            Some(_) => Ok(()),
                            None => Err(
                                ElabError::new(&tm.location, &format!("found optional argument named '{}'; only expected optional arguments {{ {} }}", name, opts_ty.iter().map(|(name, ty, _)| format!("{}: {}", bytes_to_string(name).unwrap(), ty)).join(", ")))
                            ),
                        }
                    }).collect::<Result<Vec<_>,_>>()?;

                    // then, go through the optional arguments from the type.
                    // where they have been provided in this application, overwrite their associated term
                    let opt_tms = opts_ty
                        .iter()
                        .map(|(name, ty, val)| {
                            match opts
                                .iter()
                                .find(|(name_in_app, _)| (*name_in_app.as_bytes()).eq(*name))
                            {
                                Some((_, tm)) => {
                                    let core_tm = check_tm(arena, ctx, tm, &ty)?;

                                    Ok(core_tm)
                                }
                                None => Ok(val.clone()),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // elaborate each argument, and make sure they all
                    // correspond to the function's argument type
                    // - and if any are neutrals, the whole thing is neutral

                    let arg_tms = args
                        .iter()
                        .zip(args_ty)
                        .map(|(arg, arg_ty)| {
                            let arg_tm = check_tm(arena, ctx, arg, &arg_ty)?;
                            Ok(arg_tm)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    // check if the return type needs to be evaluated
                    let body_ty = match body.as_ref() {
                        core::Val::FunReturnTyAwaiting { data } => {
                            let arg_vals = arg_tms
                                .iter()
                                // WARN cache can be empty because this is pre-caching
                                .map(|arg| {
                                    arg.eval(arena, &ctx.tms, &Cache::default(), &Env::default())
                                })
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(ElabError::from_eval_error)?;

                            // apply the function to get the concrete type out
                            let ty_val = data
                                .app(arena, arg_vals)
                                // remap the location of the error; it currently holds the location of the literal definition,
                                // but it should report at the application site
                                .map_err(|e| EvalError {
                                    location: tm.location.clone(),
                                    message: e.message,
                                })
                                .map_err(ElabError::from_eval_error)?;

                            ty_val
                        }
                        _ => body.as_ref().clone(),
                    };

                    // combine mandatory positional and optional named fields
                    let all_arg_tms = arg_tms.into_iter().chain(opt_tms).collect();

                    Ok((
                        core::Tm::new(
                            tm.location.clone(),
                            core::TmData::FunApp {
                                head: Arc::new(head_tm),
                                args: all_arg_tms,
                            },
                        ),
                        body_ty,
                    ))
                }
                // todo: add more info to this error message
                _ => Err(ElabError::new(
                    &tm.location,
                    &format!(
                        "trying to apply something as a function when it's {}",
                        head_ty
                    ),
                )),
            }
        }
        TmData::Name { name } => match ctx.lookup(name.clone()) {
            Some((index, ty)) => Ok((
                core::Tm::new(tm.location.clone(), core::TmData::Var { index }),
                ty.clone(),
            )),
            None => Err(ElabError::new_unbound_name(&tm.location, name)),
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
) -> Result<(core::Tm<'a>, core::Val<'a>), ElabError> {
    let non_parametric_operator = |ty0: &core::Val<'a>,
                                   ty1: &core::Val<'a>,
                                   name: &str|
     -> Result<(core::Tm<'a>, core::Val<'a>), ElabError> {
        let ctm0 = check_tm(arena, ctx, tm0, ty0)?;
        let ctm1 = check_tm(arena, ctx, tm1, ty1)?;

        match ctx.lookup(name.to_string()) {
            Some((index, ty)) => match ty {
                core::Val::FunTy { args, body, opts } => Ok((
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
                    (*body).as_ref().clone(),
                )),

                _ => panic!("operator has non-function type?!"),
            },
            None => Err(ElabError::new_unbound_name(location, &name)),
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
        BinOp::Exponent => {
            non_parametric_operator(&core::Val::NumTy, &core::Val::NumTy, "binary_exponent")
        }
    }
}

fn infer_un_op<'a>(
    un_op: &UnOp,
    arena: &'a Arena,
    ctx: &Context<'a>,
    location: &Location,
    tm: &'a Tm,
) -> Result<(core::Tm<'a>, core::Val<'a>), ElabError> {
    let non_parametric_operator =
        |ty0: &core::Val<'a>, name: &str| -> Result<(core::Tm<'a>, core::Val<'a>), ElabError> {
            let ctm0 = check_tm(arena, ctx, tm, ty0)?;

            match ctx.lookup(name.to_string()) {
                Some((index, ty)) => match ty {
                    core::Val::FunTy { args, body, opts } => Ok((
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
                        body.as_ref().clone(),
                    )),
                    _ => panic!("operator has non-function type?!"),
                },
                None => Err(ElabError::new_unbound_name(location, name)),
            }
        };

    match un_op {
        // since this operator is parametric, it is slightly custom
        UnOp::Minus => {
            let (ctm0, cty0) = infer_tm(arena, ctx, tm)?;

            if cty0.equiv(&core::Val::NumTy) {
                match ctx.lookup("unary_minus".to_string()) {
                    Some((index, ty)) => match ty {
                        core::Val::FunTy { args, body, opts } => Ok((
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
                            body.as_ref().clone(),
                        )),
                        _ => panic!("operator has non-function type?!"),
                    },
                    None => Err(ElabError::new_unbound_name(location, "unary_minus")),
                }
            } else if cty0.equiv(&core::Val::StrTy) {
                match ctx.lookup("unary_reverse_complement".to_string()) {
                    Some((index, ty)) => match ty {
                        core::Val::FunTy { body, .. } => Ok((
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
                            body.as_ref().clone(),
                        )),
                        _ => panic!("operator has non-function type?!"),
                    },
                    None => Err(ElabError::new_unbound_name(
                        location,
                        "unary_reverse_complement",
                    )),
                }
            } else if cty0.equiv(&core::Val::RecWithTy {
                fields: vec![CoreRecField::new(b"seq", core::Val::StrTy)],
            }) {
                match ctx.lookup("unary_read_reverse_complement".to_string()) {
                    Some((index, ty)) => match ty {
                        core::Val::FunTy { body, .. } => Ok((
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
                            body.as_ref().clone(),
                        )),
                        _ => panic!("operator has non-function type?!"),
                    },
                    None => Err(ElabError::new_unbound_name(
                        location,
                        "unary_read_reverse_complement",
                    )),
                }
            } else {
                Err(ElabError::new(
                    &ctm0.location,
                    &format!("mismatched types: expected Num or Str, got {}", cty0),
                ))
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
                            &core::Val::FunTy {
                                args: vec![core::Val::StrTy, core::Val::StrTy],
                                opts: vec![],
                                body: Arc::new(core::Val::StrTy),
                            },
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
            Ok(core::Tm::new(
                reg.location.clone(),
                core::TmData::FunApp {
                    head: Arc::new(check_tm(
                        arena,
                        ctx,
                        arena.alloc(Tm::new(
                            reg.location.clone(),
                            TmData::Name {
                                name: "to_str".to_string(),
                            },
                        )),
                        &core::Val::FunTy {
                            args: vec![core::Val::AnyTy],
                            opts: vec![],
                            body: Arc::new(core::Val::StrTy),
                        },
                    )?),
                    args: vec![check_tm(arena, ctx, tm, &core::Val::AnyTy)?],
                },
            ))
        }
    }
}

impl<'a> Display for RegionData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegionData::Hole => "_".fmt(f),
            RegionData::Term { tm, error } => format!("{:?}<{:?}>", tm, error).fmt(f),
            RegionData::Named { name, regs } => {
                format!("{}:({})", name, regs.iter().join(" ")).fmt(f)
            }
            RegionData::Sized { tm, regs } => {
                format!("|{:?}:{}|", tm, regs.iter().join(" ")).fmt(f)
            }
        }
    }
}

impl Display for MatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatchMode::All => "all",
            MatchMode::AllBest => "one",
            MatchMode::OneBest => "unique",
        }
        .fmt(f)
    }
}
