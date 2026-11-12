//! Define the core language, and evaluate it given the context of a single read to produce output effects.

use std::{collections::HashMap, fmt::Display, sync::Arc};

use itertools::Itertools;
use matcher::Matcher;

use crate::{
    core::{eval::env::Env, rec::RecVal},
    util::{
        self, bytes_to_string,
        location::{Located, Location},
        recfield::CoreRecField,
    },
};

pub mod eval;
pub mod library;
pub mod matcher;
pub mod rec;

/// An error raised if there was a problem in evaluating core syntax,
/// usually as a result of dynamic function errors. This is normal, and should be rendered
/// nicely to the programmer.
#[derive(Clone, Debug)]
pub struct EvalError {
    pub location: Location,
    pub message: String,
}

impl EvalError {
    fn new(location: &Location, message: &str) -> EvalError {
        EvalError {
            location: location.clone(),
            message: message.to_string(),
        }
    }

    fn from_internal(e: InternalError, location: Location) -> EvalError {
        EvalError {
            location,
            message: e.message,
        }
    }
}

pub type Prog<'p> = Located<ProgData<'p>>;
/// A matchbox program, consisting of a list of statements.
#[derive(Clone)]
pub struct ProgData<'p> {
    pub stmt: Stmt<'p>,
}

pub type Stmt<'p> = Located<StmtData<'p>>;
/// A statement, which can be a binding, a term on its own, or a conditional.
#[derive(Clone)]
pub enum StmtData<'p> {
    Let {
        tm: Tm,
        next: Arc<Stmt<'p>>,
    },
    Tm {
        tm: Tm,
        next: Arc<Stmt<'p>>,
    },
    If {
        branches: Vec<Branch<'p>>,
        next: Arc<Stmt<'p>>,
    },
    End,
}

impl<'p> Stmt<'p> {
    pub fn append(&self, stmt: Stmt<'p>) -> Stmt<'p> {
        match &self.data {
            StmtData::Let { tm, next } => Stmt::new(
                self.location.clone(),
                StmtData::Let {
                    tm: tm.clone(),
                    next: Arc::new(next.append(stmt)),
                },
            ),
            StmtData::Tm { tm, next } => Stmt::new(
                self.location.clone(),
                StmtData::Tm {
                    tm: tm.clone(),
                    next: Arc::new(next.append(stmt)),
                },
            ),
            StmtData::If { branches, next } => Stmt::new(
                self.location.clone(),
                StmtData::If {
                    branches: branches.clone(),
                    next: Arc::new(next.append(stmt)),
                },
            ),
            StmtData::End => stmt,
        }
    }
}

pub type Branch<'p> = Located<BranchData<'p>>;
/// A conditional branch which is either a single conditional, or a list of match branches.
#[derive(Clone)]
pub enum BranchData<'p> {
    Bool {
        tm: Tm,
        stmt: Stmt<'p>,
    },

    Matches {
        tm: Tm,
        branches: Vec<PatternBranch<'p>>,
    },
}

pub type PatternBranch<'p> = Located<PatternBranchData<'p>>;
/// A pattern-matching branch which contains a matcher,
/// and a statement to execute on successful matches.
#[derive(Clone)]
pub struct PatternBranchData<'p> {
    pub matcher: Arc<dyn Matcher<'p> + 'p>,
    pub stmt: Stmt<'p>,
}

pub type Tm = Located<TmData>;
/// A term.
#[derive(Clone)]
pub enum TmData {
    Global {
        index: usize,
    },
    Local {
        index: usize,
    },

    Univ,
    AnyTy,

    BoolTy,
    BoolLit {
        b: bool,
    },

    NumTy,
    NumLit {
        n: f64,
    },

    StrTy,
    StrLit {
        s: Vec<u8>,
    },

    // list types
    ListTy {
        ty: Arc<Tm>,
    },
    ListLit {
        tms: Vec<Tm>,
    },

    FunTy {
        args: Vec<Tm>,
        opts: Vec<(Vec<u8>, Tm, Tm)>,
        body: Arc<Tm>,
    },
    // can't remember why we need args still...
    FunLit {
        body: Arc<Tm>,
    },
    FunForeignLit {
        body:
            Arc<dyn for<'b> Fn(&Location, &[Val<'b>]) -> Result<Val<'b>, EvalError> + Send + Sync>,
    },
    FunApp {
        head: Arc<Tm>,
        args: Vec<Tm>,
    },

    RecTy {
        fields: Vec<CoreRecField<Tm>>,
    },
    RecWithTy {
        fields: Vec<CoreRecField<Tm>>,
    },
    RecLit {
        fields: Vec<CoreRecField<Tm>>,
    },
    RecProj {
        tm: Arc<Tm>,
        name: Vec<u8>,
    },

    // the return type of effectful functions
    EffectTy,

    ErrorTy,
    Error,
}

impl Tm {
    /// Creates a new error term given a location
    pub fn new_error(location: &Location) -> Tm {
        Tm::new(location.clone(), TmData::Error)
    }
}

/// A function signature, which contains a list of arguments,
/// a list of named optional arguments with default values, and a return type.
#[derive(Clone)]
struct FunSig {
    args: Vec<Tm>,
    opts: Vec<(Vec<u8>, Tm, Tm)>,
    body: Arc<Tm>,
}

/// Function that explicitly captures its environment.
#[derive(Clone)]
pub struct FunData<'r> {
    pub global: Env<Val<'r>>,
    pub local: Env<Val<'r>>,
    pub body: Tm,
}

impl<'r> FunData<'r> {
    /// Applies a function to a list of arguments, returning the result.
    pub fn app<'q: 'r>(&'q self, args: Vec<Val<'r>>) -> Result<Val<'r>, EvalError> {
        let mut new_local = self.local.reference();
        for arg in args {
            new_local.push(arg);
        }

        self.body.eval2(&self.global, &new_local)
    }

    pub fn reference<'a>(&'a self) -> FunData<'a> {
        FunData {
            global: self.global.reference(),
            local: self.local.reference(),
            body: self.body.clone(),
        }
    }

    pub fn concretise<'p>(&self) -> FunData<'p> {
        FunData {
            global: self.global.concretise(),
            local: self.local.concretise(),
            body: self.body.clone(),
        }
    }
}

/// A value.
#[derive(Clone)]
pub enum Val<'r> {
    /// Atomic values
    Univ,
    AnyTy,

    BoolTy,
    Bool {
        b: bool,
    },

    NumTy,
    Num {
        n: f64,
    },

    StrTy,
    Str {
        s: StrVal<'r>,
    },

    ListTy {
        ty: Arc<Val<'r>>,
    },
    List {
        v: ListVal<'r>,
    },

    /// Record values; can have any backend that implements the trait.
    /// This allows us to have some Record values that merely wrap the
    /// structures provided by readers.
    RecTy {
        fields: Vec<CoreRecField<Val<'r>>>,
    },
    /// A record type which requires certain fields;
    /// other fields can also be harmlessly present
    RecWithTy {
        fields: Vec<CoreRecField<Val<'r>>>,
    },
    Rec {
        rec: RecVal<'r>,
    },

    /// Function value; defunctionalised, carries the context it needs
    /// and the Rust function it will execute, which takes this carried
    /// context and any arguments.
    FunTy {
        args: Vec<Val<'r>>,
        opts: Vec<(Vec<u8>, Val<'r>, Tm)>,
        body: Arc<Val<'r>>,
    },
    Fun {
        data: FunData<'r>,
    },
    FunForeign {
        body:
            Arc<dyn for<'b> Fn(&Location, &[Val<'b>]) -> Result<Val<'b>, EvalError> + Send + Sync>,
    },
    /// Represents a dependent return type of a function,
    /// which can only be elaborated when the function is actually applied.
    /// Should probably come back and clean all of this up conceptually.
    FunReturnTyAwaiting {
        data: FunData<'r>, // expected_ty: Arc<Val<'a>>,
    },

    Neutral {
        neutral: Neutral<'r>,
    },

    EffectTy,
    // by the time it's an effect, the values have already been made portable
    Effect {
        effect: Effect,
    },

    // errors, or holes
    ErrorTy,
    Error,
}

impl<'r> Val<'r> {
    pub fn reference<'a>(&'a self) -> Val<'a> {
        match self {
            Val::Univ => Val::Univ,
            Val::AnyTy => Val::AnyTy,
            Val::BoolTy => Val::BoolTy,
            Val::Bool { b } => Val::Bool { b: *b },
            Val::NumTy => Val::NumTy,
            Val::Num { n } => Val::Num { n: *n },
            Val::StrTy => Val::StrTy,
            Val::Str { s } => Val::Str { s: s.reference() },
            Val::ListTy { ty } => Val::ListTy {
                ty: Arc::new(ty.reference()),
            },
            Val::List { v } => Val::List { v: v.reference() },
            Val::RecTy { fields } => Val::RecTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.reference()))
                    .collect(),
            },
            Val::RecWithTy { fields } => Val::RecWithTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.reference()))
                    .collect(),
            },
            Val::Rec { rec } => Val::Rec {
                rec: rec.reference(),
            },
            Val::FunTy { args, opts, body } => Val::FunTy {
                args: args.iter().map(|arg| arg.reference()).collect(),
                opts: opts
                    .iter()
                    .map(|(name, ty, tm)| (name.clone(), ty.reference(), tm.clone()))
                    .collect(),
                body: Arc::new(body.reference()),
            },
            Val::Fun { data } => Val::Fun {
                data: data.reference(),
            },
            Val::FunForeign { body } => Val::FunForeign { body: body.clone() },
            Val::FunReturnTyAwaiting { data } => Val::FunReturnTyAwaiting {
                data: data.reference(),
            },
            Val::Neutral { neutral } => Val::Neutral {
                neutral: neutral.reference(),
            },
            Val::EffectTy => Val::EffectTy,
            Val::Effect { effect } => Val::Effect {
                effect: effect.clone(),
            },
            Val::ErrorTy => Val::ErrorTy,
            Val::Error => Val::Error,
        }
    }

    pub fn concretise<'p>(&self) -> Val<'p> {
        match self {
            Val::Univ => Val::Univ,
            Val::AnyTy => Val::AnyTy,
            Val::BoolTy => Val::BoolTy,
            Val::Bool { b } => Val::Bool { b: *b },
            Val::NumTy => Val::NumTy,
            Val::Num { n } => Val::Num { n: *n },
            Val::StrTy => Val::StrTy,
            Val::Str { s } => Val::Str { s: s.concretise() },
            Val::ListTy { ty } => Val::ListTy {
                ty: Arc::new(ty.concretise()),
            },
            Val::List { v } => Val::List { v: v.concretise() },
            Val::RecTy { fields } => Val::RecTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.concretise()))
                    .collect(),
            },
            Val::RecWithTy { fields } => Val::RecWithTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.concretise()))
                    .collect(),
            },
            Val::Rec { rec } => Val::Rec {
                rec: rec.concretise(),
            },
            Val::FunTy { args, opts, body } => Val::FunTy {
                args: args.iter().map(|arg| arg.concretise()).collect(),
                opts: opts
                    .iter()
                    .map(|(name, ty, tm)| (name.clone(), ty.concretise(), tm.clone()))
                    .collect(),
                body: Arc::new(body.concretise()),
            },
            Val::Fun { data } => Val::Fun {
                data: data.concretise(),
            },
            Val::FunForeign { body } => Val::FunForeign { body: body.clone() },
            Val::FunReturnTyAwaiting { data } => Val::FunReturnTyAwaiting {
                data: data.concretise(),
            },
            Val::Neutral { neutral } => Val::Neutral {
                neutral: neutral.concretise(),
            },
            Val::EffectTy => Val::EffectTy,
            Val::Effect { effect } => Val::Effect {
                effect: effect.clone(),
            },
            Val::ErrorTy => Val::ErrorTy,
            Val::Error => Val::Error,
        }
    }
}

/// A string value, either a concrete u8 vector or a slice elsewhere (presumably in the global or local env)
#[derive(Clone)]
pub enum StrVal<'r> {
    Reference { s: &'r [u8] },
    Concrete { s: Vec<u8> },
}

impl<'r> StrVal<'r> {
    pub fn new_reference(s: &'r [u8]) -> StrVal<'r> {
        StrVal::Reference { s }
    }

    pub fn new_concrete(s: Vec<u8>) -> StrVal<'r> {
        StrVal::Concrete { s }
    }

    /// Get the slice
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            StrVal::Reference { s } => s,
            StrVal::Concrete { s } => s,
        }
    }

    fn slice(&self, start: usize, end: usize) -> StrVal<'r> {
        match self {
            StrVal::Reference { s } => StrVal::Reference { s: &s[start..end] },
            // WARN could be costly
            StrVal::Concrete { s } => StrVal::Concrete {
                s: (&s[start..end]).to_vec(),
            },
        }
    }

    pub fn reference(&'r self) -> StrVal<'r> {
        match self {
            StrVal::Reference { s } => StrVal::Reference { s },
            StrVal::Concrete { s } => StrVal::Reference { s },
        }
    }

    pub fn concretise<'p>(&self) -> StrVal<'p> {
        match self {
            StrVal::Reference { s } => StrVal::Concrete { s: s.to_vec() },
            StrVal::Concrete { s } => StrVal::Concrete { s: s.clone() },
        }
    }
}

/// A list value, either a concrete value vector or a slice elsewhere (presumably in the global or local env)
#[derive(Clone)]
enum ListVal<'r> {
    Reference { v: &'r [Val<'r>] },
    Concrete { v: Vec<Val<'r>> },
}

impl<'r> ListVal<'r> {
    fn new_reference(v: &'r [Val<'r>]) -> ListVal<'r> {
        ListVal::Reference { v }
    }

    fn new_concrete(v: Vec<Val<'r>>) -> ListVal<'r> {
        ListVal::Concrete { v }
    }

    /// Get the slice
    fn get_reference(&self) -> &[Val<'r>] {
        match self {
            ListVal::Reference { v } => v,
            ListVal::Concrete { v } => v,
        }
    }

    pub fn reference(&'r self) -> ListVal<'r> {
        match self {
            ListVal::Reference { v } => ListVal::Reference { v },
            ListVal::Concrete { v } => ListVal::Reference { v },
        }
    }

    pub fn concretise<'p>(&'r self) -> ListVal<'p> {
        match self {
            ListVal::Reference { v } => ListVal::Concrete {
                v: v.iter().map(|v| v.concretise()).collect(),
            },
            ListVal::Concrete { v } => ListVal::Concrete {
                v: v.iter().map(|v| v.concretise()).collect(),
            },
        }
    }
}

/// An effect, carrying a value and a handler.
#[derive(Clone, PartialEq, Debug)]
pub enum Effect {
    Stdout { val: PortableVal },
    Out { val: PortableVal, name: Vec<u8> },
    Count { val: PortableVal, name: Vec<u8> },
    Mean { num: f64, name: Vec<u8> },
}

impl<'r> Val<'r> {
    /// Checks whether two types are equivalent.
    /// When one type is a more general or equally general representation of another, they are equivalent.
    pub fn equiv(&self, other: &Val<'r>) -> bool {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<Val<'r>>], name: &[u8]| -> Option<Val<'r>> {
            let mut out = None;
            for field in fields {
                if field.name.eq(name) {
                    out = Some(field.data.clone())
                }
            }
            out
        };
        let get_opt = |fields: &[(Vec<u8>, Val<'r>, Tm)], name: &[u8]| -> Option<(Val<'r>, Tm)> {
            let mut out = None;
            for (field_name, ty, val) in fields {
                if field_name.eq(&name) {
                    out = Some((ty.clone(), val.clone()))
                }
            }
            out
        };

        match (self, other) {
            // Neutrals we just let through; what else can we do?
            (Val::Neutral { .. }, _) | (_, Val::Neutral { .. }) => true,
            // Similar with awaiting
            (Val::FunReturnTyAwaiting { .. }, _) | (_, Val::FunReturnTyAwaiting { .. }) => true,
            // ErrorTy we also let through; it represents something that's already been caught
            (Val::ErrorTy, _) | (_, Val::ErrorTy) => true,

            // Any is equivalent to everything
            (Val::AnyTy, _) | (_, Val::AnyTy) => true,

            // trivially equivalent
            (Val::Univ, Val::Univ)
            | (Val::BoolTy, Val::BoolTy)
            | (Val::NumTy, Val::NumTy)
            | (Val::StrTy, Val::StrTy)
            | (Val::EffectTy, Val::EffectTy)
            | (Val::ErrorTy, Val::ErrorTy) => true,

            // list types must have equivalent inner types
            (Val::ListTy { ty: ty1 }, Val::ListTy { ty: ty2 }) => ty1.equiv(ty2),

            // equivalent if argument types are equivalent
            // and return types are equivalent
            (
                Val::FunTy {
                    args: args1,
                    opts: opts1,
                    body: body1,
                },
                Val::FunTy {
                    args: args2,
                    opts: opts2,
                    body: body2,
                },
            ) => {
                let mut names = opts1
                    .iter()
                    .chain(opts2)
                    .map(|(name, _, _)| name.clone())
                    .unique();

                args1.len().eq(&args2.len())
                    && args1.iter().zip(args2).all(|(arg1, arg2)| arg1.equiv(arg2))
                    && names.all(
                        |name| match (get_opt(opts1, &name), get_opt(opts2, &name)) {
                            // WARN for now do not verify that the values themselves are the same
                            // just check the types and the names of all the fields
                            (Some((ty1, val1)), Some((ty2, val2))) => ty1.equiv(&ty2),
                            // any optional arguments missing is a type error
                            _ => false,
                        },
                    )
                    && body1.equiv(body2)
            }

            // equivalent if all fields are the same
            (Val::RecTy { fields: fields1 }, Val::RecTy { fields: fields2 }) => {
                let mut names = fields1
                    .iter()
                    .chain(fields2)
                    .map(|a| a.name.clone())
                    .unique();

                fields1.len().eq(&fields2.len())
                    && names.all(|name| match (get(&fields1, &name), get(&fields2, &name)) {
                        // both recs must contain all fields, and the types must be equivalent
                        (Some(vty1), Some(vty2)) => vty1.equiv(&vty2),
                        // any fields missing is a type error
                        _ => false,
                    })
            }

            // if one record type contains equivalent fields to each one
            // required by a record type containing, they are equivalent
            (
                Val::RecTy { fields },
                Val::RecWithTy {
                    fields: with_fields,
                },
            )
            | (
                Val::RecWithTy {
                    fields: with_fields,
                },
                Val::RecTy { fields },
            ) => {
                let names = fields.iter().map(|CoreRecField { name, .. }| name);
                let with_names = with_fields.iter().map(|CoreRecField { name, .. }| name);

                // all the names in the RecWith should be present in the Rec
                with_names.clone().all(|name| names.clone().contains(name))
                    && fields
                        .iter()
                        .all(|field| match get(with_fields, &field.name) {
                            // if it contains it, they must be equiv
                            Some(ty) => field.data.equiv(&ty),
                            // if it doesn't contain it, that's fine
                            None => true,
                        })
            }

            // two RecTypeContaining are equivalent if their shared fields are equivalent
            // (any missing field could always be in the margin of flexibility)
            (Val::RecWithTy { fields: fields1 }, Val::RecWithTy { fields: fields2 }) => {
                let names1 = fields1.iter().map(|CoreRecField { name, .. }| name);
                let names2 = fields2.iter().map(|CoreRecField { name, .. }| name);
                let shared_names = names1.filter(|name| names2.clone().contains(name));

                shared_names
                    .clone()
                    .all(|name| match (get(fields1, name), get(fields2, name)) {
                        // if they both contain it, the types must be equivalent
                        (Some(ty1), Some(ty2)) => ty1.equiv(&ty2),
                        // if just one of them has it, it doesn't matter what the type is
                        (None, Some(_)) | (Some(_), None) => true,
                        // should never happen
                        (None, None) => panic!("neither contains the name?!"),
                    })
            }

            _ => false,
        }
    }

    /// Checks whether two values are equal.
    pub fn eq<'b>(&self, other: &Val<'b>) -> bool {
        match (self, other) {
            (Val::Univ, Val::Univ)
            | (Val::AnyTy, Val::AnyTy)
            | (Val::BoolTy, Val::BoolTy)
            | (Val::NumTy, Val::NumTy)
            | (Val::StrTy, Val::StrTy)
            | (Val::EffectTy, Val::EffectTy)
            | (Val::Error, Val::Error)
            | (Val::ErrorTy, Val::ErrorTy) => true,

            (Val::Bool { b: b1 }, Val::Bool { b: b2 }) => b1.eq(b2),
            (Val::Num { n: n1 }, Val::Num { n: n2 }) => n1.eq(n2),
            (Val::Str { s: s1 }, Val::Str { s: s2 }) => s1.as_bytes().eq(s2.as_bytes()),

            // check that all the fields of r1 and all the fields of r2 are the same
            (Val::Rec { rec: r1 }, Val::Rec { rec: r2 }) => {
                let fields1 = r1.all();
                let fields2 = r2.all();
                let mut names = fields1.iter().chain(fields2.iter()).map(|(name, _)| name);

                fields1.len().eq(&fields2.len())
                    && names.all(|name| match (fields1.get(name), fields2.get(name)) {
                        // both recs must contain all fields, and the values must be equal
                        (Some(val1), Some(val2)) => val1.eq(val2),
                        // any fields missing is unequal
                        _ => false,
                    })
            }

            (Val::Effect { effect: e1 }, Val::Effect { effect: e2 }) => e1.eq(e2),

            // really not sure how to check equivalence between functions, so false for now
            _ => false,
        }
    }

    /// Given another type, return the most precise type of the two.
    pub fn most_precise(&self, other: &Val<'r>) -> Val<'r> {
        // takes a field name, and looks it up in the list of fields,
        // returning the type if one is found
        let get = |fields: &[CoreRecField<Val<'r>>], name: &[u8]| -> Option<Val<'r>> {
            let mut out = None;
            for field in fields {
                if field.name.eq(name) {
                    out = Some(field.data.clone())
                }
            }
            out
        };

        match (self, other) {
            // if either type is Any, the other type is assumed more precise
            (Val::AnyTy, ty) | (ty, Val::AnyTy) => ty.clone(),

            // if both are Rec, take the most precise version of each field
            (Val::RecTy { fields: fields1 }, Val::RecTy { fields: fields2 }) => {
                Val::RecTy {
                    fields: fields1
                        .iter()
                        .chain(fields2)
                        .map(|CoreRecField { name, .. }| name)
                        .unique()
                        .map(|name| match (get(fields1, name), get(fields2, name)) {
                            // when both Recs contain the field name, take the most precise definition
                            (Some(ty1), Some(ty2)) => {
                                CoreRecField::new(name, ty1.most_precise(&ty2))
                            }

                            // this should never happen
                            _ => panic!(
                                "a record was missing a field?! both should have all fields?!"
                            ),
                        })
                        .collect::<Vec<_>>(),
                }
            }

            // all atomic combinations are equally precise (assuming they are equiv!)
            _ => self.clone(),
        }
    }

    /// Checks if a value is neutral.
    pub fn is_neutral(&self) -> bool {
        match self {
            Val::Neutral { neutral } => true,

            Val::ListTy { ty } => ty.is_neutral(),
            Val::List { v } => v.get_reference().iter().any(|val| val.is_neutral()),
            Val::RecTy { fields } => fields.iter().any(|field| field.data.is_neutral()),
            Val::RecWithTy { fields } => fields.iter().any(|field| field.data.is_neutral()),
            Val::Rec { rec } => rec.is_neutral(),
            Val::FunTy { args, opts, body } => {
                args.iter().any(|arg| arg.is_neutral()) || body.is_neutral()
            }

            _ => false,
        }
    }

    /// Convert a value to a portable value, so it owns its data and can be shared.
    pub fn to_portable(&self) -> PortableVal {
        match self {
            Val::Univ => PortableVal::Univ,

            Val::BoolTy => PortableVal::BoolTy,
            Val::Bool { b } => PortableVal::Bool { b: *b },

            Val::NumTy => PortableVal::NumTy,
            Val::Num { n } => PortableVal::Num { n: n.clone() },

            Val::StrTy => PortableVal::StrTy,
            Val::Str { s } => PortableVal::Str {
                s: s.as_bytes().to_vec(),
            },

            Val::ListTy { ty } => PortableVal::ListTy {
                ty: Arc::new(ty.to_portable()),
            },
            Val::List { v } => PortableVal::List {
                v: v.get_reference().iter().map(Val::to_portable).collect(),
            },

            Val::RecTy { fields } => PortableVal::RecTy {
                fields: fields
                    .iter()
                    .map(|field| (field.name.to_vec(), field.data.to_portable()))
                    .collect(),
            },
            Val::RecWithTy { fields } => PortableVal::RecWithTy {
                fields: fields
                    .iter()
                    .map(|field| (field.name.to_vec(), field.data.to_portable()))
                    .collect(),
            },
            Val::Rec { rec } => PortableVal::Rec {
                fields: rec
                    .all()
                    .iter()
                    .map(|(name, val)| (name.to_vec(), val.to_portable()))
                    .collect(),
            },

            Val::FunTy { args, opts, body } => PortableVal::FunTy {
                args: args.iter().map(Val::to_portable).collect(),
                opts: opts
                    .iter()
                    // WARN to make tms portable, for now just ignore them
                    .map(|(name, ty, tm)| (name.to_vec(), ty.to_portable(), tm.to_string()))
                    .collect(),
                body: Arc::new(body.to_portable()),
            },
            Val::Fun { .. } => PortableVal::Fun {
                s: "#fun".to_string(),
            },
            Val::FunForeign { body: f, .. } => PortableVal::Fun {
                s: "#fun-foreign".to_string(),
            },

            // not sure what to do with this.. just spit out a string?
            Val::FunReturnTyAwaiting { .. } => todo!(),
            Val::Neutral { .. } => todo!(),
            Val::AnyTy => todo!(),

            Val::EffectTy => PortableVal::EffectTy,
            Val::Effect { effect } => PortableVal::Effect {
                effect: Arc::new(effect.clone()),
            },
            Val::ErrorTy => PortableVal::ErrorTy,
            Val::Error => PortableVal::Error,
        }
    }
}

/// A neutral value, not able to be evaluated at compile time.
#[derive(Clone)]
pub enum Neutral<'a> {
    Var {
        level: usize,
    },
    //
    ListTy {
        ty: Arc<Val<'a>>,
    },
    ListLit {
        tms: Vec<Val<'a>>,
    },
    //
    FunTy {
        args: Vec<Val<'a>>,
        opts: Vec<(Vec<u8>, Val<'a>, Tm)>,
        body: Arc<Val<'a>>,
    },
    FunLit {
        body: Arc<Val<'a>>,
    },
    FunApp {
        head: Arc<Val<'a>>,
        args: Vec<Val<'a>>,
    },
    //
    RecTy {
        fields: Vec<CoreRecField<Val<'a>>>,
    },
    RecWithTy {
        fields: Vec<CoreRecField<Val<'a>>>,
    },
    RecLit {
        fields: Vec<CoreRecField<Val<'a>>>,
    },
    RecProj {
        tm: Arc<Val<'a>>,
        name: Vec<u8>,
    },
}

impl<'r> Neutral<'r> {
    pub fn reference<'a>(&'a self) -> Neutral<'a> {
        match self {
            Neutral::Var { level } => Neutral::Var { level: *level },
            Neutral::ListTy { ty } => Neutral::ListTy {
                ty: Arc::new(ty.reference()),
            },
            Neutral::ListLit { tms } => Neutral::ListLit {
                tms: tms.iter().map(Val::reference).collect(),
            },
            Neutral::FunTy { args, opts, body } => Neutral::FunTy {
                args: args.iter().map(Val::reference).collect(),
                opts: opts
                    .iter()
                    .map(|(name, ty, tm)| (name.clone(), ty.reference(), tm.clone()))
                    .collect(),
                body: Arc::new(body.reference()),
            },
            Neutral::FunLit { body } => Neutral::FunLit {
                body: Arc::new(body.reference()),
            },
            Neutral::FunApp { head, args } => Neutral::FunApp {
                head: Arc::new(head.reference()),
                args: args.iter().map(Val::reference).collect(),
            },
            Neutral::RecTy { fields } => Neutral::RecTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.reference()))
                    .collect(),
            },
            Neutral::RecWithTy { fields } => Neutral::RecWithTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.reference()))
                    .collect(),
            },
            Neutral::RecLit { fields } => Neutral::RecLit {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.reference()))
                    .collect(),
            },
            Neutral::RecProj { tm, name } => Neutral::RecProj {
                tm: Arc::new(tm.reference()),
                name: name.clone(),
            },
        }
    }

    pub fn concretise<'p>(&self) -> Neutral<'p> {
        match self {
            Neutral::Var { level } => Neutral::Var { level: *level },
            Neutral::ListTy { ty } => Neutral::ListTy {
                ty: Arc::new(ty.concretise()),
            },
            Neutral::ListLit { tms } => Neutral::ListLit {
                tms: tms.iter().map(Val::concretise).collect(),
            },
            Neutral::FunTy { args, opts, body } => Neutral::FunTy {
                args: args.iter().map(Val::concretise).collect(),
                opts: opts
                    .iter()
                    .map(|(name, ty, tm)| (name.clone(), ty.concretise(), tm.clone()))
                    .collect(),
                body: Arc::new(body.concretise()),
            },
            Neutral::FunLit { body } => Neutral::FunLit {
                body: Arc::new(body.concretise()),
            },
            Neutral::FunApp { head, args } => Neutral::FunApp {
                head: Arc::new(head.concretise()),
                args: args.iter().map(Val::concretise).collect(),
            },
            Neutral::RecTy { fields } => Neutral::RecTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.concretise()))
                    .collect(),
            },
            Neutral::RecWithTy { fields } => Neutral::RecWithTy {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.concretise()))
                    .collect(),
            },
            Neutral::RecLit { fields } => Neutral::RecLit {
                fields: fields
                    .iter()
                    .map(|c| CoreRecField::new(&c.name, c.data.concretise()))
                    .collect(),
            },
            Neutral::RecProj { tm, name } => Neutral::RecProj {
                tm: Arc::new(tm.concretise()),
                name: name.clone(),
            },
        }
    }
}

/// Represents an error INTERNAL to matchbox; expected to never appear to the user.
#[derive(Clone, Debug)]
pub struct InternalError {
    pub message: String,
}

impl InternalError {
    pub fn new(message: &str) -> InternalError {
        InternalError {
            message: message.to_string(),
        }
    }
}

/// A portable value which can be shared.
#[derive(Debug, Clone, PartialEq)]
pub enum PortableVal {
    /// Atomic values
    Univ,
    Any,

    BoolTy,
    Bool {
        b: bool,
    },

    NumTy,
    Num {
        n: f64,
    },

    StrTy,
    Str {
        s: Vec<u8>,
    },

    /// Functions just become description strings
    FunTy {
        args: Vec<PortableVal>,
        // WARN here, the term has been serialised as a string
        // because I have no idea what else to do
        opts: Vec<(Vec<u8>, PortableVal, String)>,
        body: Arc<PortableVal>,
    },
    Fun {
        s: String,
    },

    ListTy {
        ty: Arc<PortableVal>,
    },
    List {
        v: Vec<PortableVal>,
    },

    RecTy {
        fields: HashMap<Vec<u8>, PortableVal>,
    },
    RecWithTy {
        fields: HashMap<Vec<u8>, PortableVal>,
    },
    Rec {
        fields: HashMap<Vec<u8>, PortableVal>,
    },

    EffectTy,
    Effect {
        effect: Arc<Effect>,
    },

    ErrorTy,
    Error,
}

impl<'a> Display for Prog<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.data.stmt.fmt(f)
    }
}

impl<'a> Display for StmtData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StmtData::Let { tm, next } => format!("push {}; {}", tm, next).fmt(f),
            StmtData::Tm { tm, next } => format!("{}; {}", tm, next).fmt(f),
            StmtData::If { branches, next } => format!(
                "if {}; {}",
                branches.iter().map(|a| a.to_string()).join(", "),
                next
            )
            .fmt(f),
            StmtData::End => "".fmt(f),
        }
    }
}

impl<'a> Display for BranchData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchData::Bool { tm, stmt } => format!("{} => {}", tm, stmt).fmt(f),
            BranchData::Matches { tm, branches } => format!(
                "{} matches {}",
                tm,
                branches.iter().map(|b| b.to_string()).join(", ")
            )
            .fmt(f),
        }
    }
}

impl<'a> Display for PatternBranchData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("#matcher({}) => {}", self.matcher, self.stmt).fmt(f)
    }
}

impl Display for TmData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TmData::Global { index } => format!("#cached[{}]", index).fmt(f),
            TmData::Local { index } => format!("#[{}]", *index).fmt(f),
            TmData::Univ => "Univ".fmt(f),
            TmData::AnyTy => "Any".fmt(f),
            TmData::BoolTy => "Bool".fmt(f),
            TmData::BoolLit { b } => b.fmt(f),
            TmData::NumTy => "Num".fmt(f),
            TmData::NumLit { n } => n.fmt(f),
            TmData::StrTy => "Str".fmt(f),
            TmData::StrLit { s } => format!("'{}'", util::bytes_to_string(s).unwrap()).fmt(f),
            TmData::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|a| a.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            TmData::FunLit { body } => format!("#fun({})", body).fmt(f),
            TmData::FunForeignLit { body } => "#fun-foreign".fmt(f),
            TmData::FunApp { head, args } => format!(
                "({})({})",
                head,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::ListLit { tms } => format!("[{}]", tms.into_iter().join(", ")).fmt(f),
            TmData::ListTy { ty } => format!("[{}]", ty).fmt(f),
            TmData::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecLit { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            TmData::RecProj { tm, name } => {
                format!("{}.{}", tm, util::bytes_to_string(name).unwrap()).fmt(f)
            }
            TmData::EffectTy => "Effect".fmt(f),
            TmData::ErrorTy => "Error".fmt(f),
            TmData::Error => "#error".fmt(f),
        }
    }
}

impl<'a> std::fmt::Debug for Val<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.to_string(), f)
    }
}

impl<'a> Display for Val<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Univ => "Type".fmt(f),
            Val::AnyTy => "Any".fmt(f),
            Val::BoolTy => "Bool".fmt(f),
            Val::Bool { b } => b.fmt(f),
            Val::NumTy => "Num".fmt(f),
            Val::Num { n } => n.fmt(f),
            Val::StrTy => "Str".fmt(f),
            Val::Str { s } => util::bytes_to_string(s.as_bytes()).unwrap().fmt(f),
            Val::ListTy { ty } => format!("[{}]", ty).fmt(f),
            Val::List { v } => format!("[{}]", v.get_reference().into_iter().join(", ")).fmt(f),
            Val::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Val::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Val::Rec { rec } => rec.fmt(f),
            Val::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            Val::Fun { data } => format!("#fun({})", data).fmt(f),
            Val::FunForeign { body } => format!("#fun-foreign",).fmt(f),
            Val::FunReturnTyAwaiting { data } => format!("#awaiting({})", data).fmt(f),
            Val::Neutral { neutral } => format!("#neutral({})", neutral).fmt(f),
            Val::EffectTy => "Effect".fmt(f),
            Val::Effect { effect } => format!("#effect({})", effect).fmt(f),
            Val::ErrorTy => "ErrorTy".fmt(f),
            Val::Error => "Error".fmt(f),
        }
    }
}

impl<'a> Display for Neutral<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Neutral::Var { level } => format!("#[{}]", level).fmt(f),
            Neutral::FunApp { head, args } => format!(
                "({})({})",
                head,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecProj { tm, name } => {
                format!("{}.{}", tm, bytes_to_string(name).unwrap()).fmt(f)
            }
            Neutral::ListTy { ty } => format!("listty[{}]", ty).fmt(f),
            Neutral::ListLit { tms } => format!("[{}]", tms.into_iter().join(", ")).fmt(f),
            Neutral::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            Neutral::RecLit { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|field| &field.name)
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),

            Neutral::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|a| a.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            Neutral::FunLit { body } => format!("#fun({})", body).fmt(f),
        }
    }
}

impl<'a> Display for FunData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("{} :: {}", self.body, self.global).fmt(f)
    }
}

impl<'a> Display for Effect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Effect::Stdout { val } => format!("stdout!({})", val).fmt(f),
            Effect::Out { val, name } => {
                format!("out!({}, name = {})", val, bytes_to_string(name).unwrap()).fmt(f)
            }
            Effect::Count { val, name } => {
                format!("count!({}, name = {})", val, bytes_to_string(name).unwrap()).fmt(f)
            }
            Effect::Mean { num, name } => {
                format!("mean!({}, {})", num, bytes_to_string(name).unwrap()).fmt(f)
            }
        }
    }
}

impl<'a> Display for PortableVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortableVal::Univ => "Type".fmt(f),
            PortableVal::Any => "Any".fmt(f),
            PortableVal::BoolTy => "Bool".fmt(f),
            PortableVal::Bool { b } => b.fmt(f),
            PortableVal::NumTy => "Num".fmt(f),
            PortableVal::Num { n } => n.fmt(f),
            PortableVal::StrTy => "Str".fmt(f),
            PortableVal::Str { s } => util::bytes_to_string(s).unwrap().fmt(f),
            PortableVal::ListTy { ty } => format!("[{}]", ty).fmt(f),
            PortableVal::List { v } => format!("[{}]", v.into_iter().join(", ")).fmt(f),
            PortableVal::RecTy { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|(name, _)| *name)
                    .map(|(name, val)| format!(
                        "{} : {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::RecWithTy { fields } => format!(
                "{{ {} .. }}",
                fields
                    .iter()
                    .sorted_by_key(|(name, _)| *name)
                    .map(|(name, val)| format!(
                        "{} : {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::Rec { fields } => format!(
                "{{ {} }}",
                fields
                    .iter()
                    .sorted_by_key(|(name, _)| *name)
                    .map(|(name, val)| format!(
                        "{} = {}",
                        String::from_utf8(name.clone()).unwrap(),
                        val
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
            .fmt(f),
            PortableVal::FunTy { args, opts, body } => format!(
                "({}) -> {}",
                args.iter()
                    .map(|arg| arg.to_string())
                    .chain(opts.iter().map(|(name, ty, val)| format!(
                        "{}: {} = {}",
                        bytes_to_string(name).unwrap(),
                        ty,
                        val
                    )))
                    .collect::<Vec<_>>()
                    .join(", "),
                body
            )
            .fmt(f),
            PortableVal::Fun { s } => format!("#fun({})", s).fmt(f),
            PortableVal::EffectTy => "Effect".fmt(f),
            PortableVal::Effect { effect } => format!("#effect({})", effect).fmt(f),
            PortableVal::ErrorTy => "Error".fmt(f),
            PortableVal::Error => "#error".fmt(f),
        }
    }
}
