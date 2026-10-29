//! Define the surface language, and elaborate it to the core language using bidirectional type inference.

use std::{collections::HashMap, fmt::Display, io::Read, ops::Deref, rc::Rc, sync::Arc};

use itertools::Itertools;

use crate::{
    core::{
        self, library,
        matcher::read_matcher::{self, LocTm, OpTm},
        EvalError, Val,
    },
    input::{FileType, InputError, Reader},
    parse,
    util::{
        bytes_to_string, cache::Cache, env::Env, location::Located, location::Location, ran::Ran,
        recfield::CoreRecField, recfield::RecField, Arena,
    },
};

pub mod elab;
pub mod elab2;

pub type Prog = Located<ProgData>;
/// A matchbox program, consisting of a list of statements.
#[derive(Debug)]
pub struct ProgData {
    pub stmts: Vec<Stmt>,
}

pub type Stmt = Located<StmtData>;
/// A statement.
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
    /// [ if read matches [ |3| rest:_ ] => rest |> trimmed ]
    If { branches: Vec<Branch> },
}

pub type Branch = Located<BranchData>;
/// A branch of a conditional. Either a boolean branch, or a match branch.
#[derive(Debug, Clone)]
pub enum BranchData {
    /// A boolean branch
    /// [ read.seq.len() > 100 { read.name |> names } ]
    Bool { tm: Tm, stmt: Stmt },

    /// A pattern-matching branch
    /// [ read matches [_ tso _] => read |> filtered ]
    Matches {
        tm: Tm,
        branches: Vec<PatternBranch>,
    },
}

pub type PatternBranch = Located<PatternBranchData>;
/// A pattern branch, containing a pattern to test and a statement to execute.
#[derive(Debug, Clone)]
pub struct PatternBranchData {
    pub pat: Pattern,
    pub stmt: Stmt,
}

/// Match mode, assignable using the --match-mode parameter.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum MatchMode {
    All,
    AllBest,
    OneBest,
}

pub type Pattern = Located<PatternData>;
/// A pattern is a boolean test that, if successful,
/// also produces a context of bound values.
/// Could be a literal, or something more complex.
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

pub type ReadParameter = Located<ReadParameterData>;
/// A parameter in a read pattern. Contains a name, and a term.
#[derive(Debug, Clone)]
pub struct ReadParameterData {
    pub name: String,
    pub tm: Tm,
}

pub type Region = Located<RegionData>;
/// A region in a read pattern.
#[derive(Debug, Clone)]
pub enum RegionData {
    Hole,
    Term { tm: Tm, error: Tm },

    Named { name: String, regs: Vec<Region> },
    Sized { tm: Tm, regs: Vec<Region> },
}

pub type Tm = Located<TmData>;
/// A term.
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

/// A parameter, with a name and a type.
#[derive(Clone, Debug)]
pub struct Param {
    pub name: String,
    pub ty: Tm,
}

/// An optional parameter, with a name, a default value and a type.
#[derive(Clone, Debug)]
pub struct OptParam {
    pub name: String,
    pub ty: Tm,
    pub tm: Tm,
}

/// A number literal, either an int or a float.
#[derive(Clone, Debug)]
pub enum Num {
    Int(i32),
    Float(f64),
}

impl Num {
    fn get_float(&self) -> f64 {
        match self {
            Num::Int(i) => *i as f64,
            Num::Float(f) => *f,
        }
    }
}

/// A binary operation.
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

/// A unary operation.
#[derive(Clone, Debug)]
pub enum UnOp {
    Minus,
    Not,
}

pub type StrLitRegion = Located<StrLitRegionData>;
/// A region of a string literal. Either a string, or a term to be formatted.
#[derive(Clone, Debug)]
pub enum StrLitRegionData {
    Str { s: Vec<u8> },
    Tm { tm: Tm },
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
