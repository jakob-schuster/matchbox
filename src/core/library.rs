use crate::{
    core,
    surface::Context,
    util::{self, bytes_to_string, Arena, CoreRecField, Location},
};
use std::{path::Path, sync::Arc};

use super::{rec::ConcreteRec, EvalError, Val};

pub fn standard_library<'a>(arena: &'a Arena, with_read: bool) -> Context<'a> {
    let entries = vec![
        ("Type", core::TmData::Univ, core::TmData::Univ),
        ("Any", core::TmData::Univ, core::TmData::AnyTy),
        ("Bool", core::TmData::Univ, core::TmData::BoolTy),
        ("Num", core::TmData::Univ, core::TmData::NumTy),
        ("Str", core::TmData::Univ, core::TmData::StrTy),
    ];

    let mut ctx = entries
        .iter()
        .try_fold(Context::default(), |ctx, (name, ty, tm)| {
            let ty1 = core::eval(
                arena,
                &ctx.tms,
                arena.alloc(core::Tm::new(Location::new(0, 0), ty.clone())),
            )?;
            let tm1 = core::eval(
                arena,
                &ctx.tms,
                arena.alloc(core::Tm::new(Location::new(0, 0), tm.clone())),
            )?;

            Ok::<Context, EvalError>(ctx.bind_def(name.to_string(), ty1, tm1))
        })
        .expect("could not evaluate standard library!");

    ctx = vec![
        (
            "binary_plus",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_plus)
                as Arc<
                    dyn Fn(
                            &'a Arena,
                            &Location,
                            &[&'a core::Val<'a>],
                        ) -> Result<&'a core::Val<'a>, EvalError>
                        + Send
                        + Sync,
                >,
        ),
        (
            "binary_times",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_times),
        ),
        (
            "binary_division",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_division),
        ),
        (
            "binary_modulo",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_modulo),
        ),
        (
            "binary_exponent",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_exponent),
        ),
        (
            "binary_minus",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::binary_minus),
        ),
        (
            "binary_equal",
            vec![core::TmData::AnyTy, core::TmData::AnyTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_equal),
        ),
        (
            "binary_not_equal",
            vec![core::TmData::AnyTy, core::TmData::AnyTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_not_equal),
        ),
        (
            "binary_less_than",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_less_than),
        ),
        (
            "binary_greater_than",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_greater_than),
        ),
        (
            "binary_less_than_or_equal",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_less_than_or_equal),
        ),
        (
            "binary_greater_than_or_equal",
            vec![core::TmData::NumTy, core::TmData::NumTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_greater_than_or_equal),
        ),
        (
            "binary_and",
            vec![core::TmData::BoolTy, core::TmData::BoolTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_and),
        ),
        (
            "binary_or",
            vec![core::TmData::BoolTy, core::TmData::BoolTy],
            core::TmData::BoolTy,
            Arc::new(core::library::binary_or),
        ),
        (
            "unary_minus",
            vec![core::TmData::NumTy],
            core::TmData::NumTy,
            Arc::new(core::library::unary_minus),
        ),
        (
            "unary_reverse_complement",
            vec![core::TmData::StrTy],
            core::TmData::StrTy,
            Arc::new(core::library::unary_reverse_complement),
        ),
    ]
    .into_iter()
    .try_fold(ctx.clone(), |ctx0, (name, args, return_ty, fun)| {
        let args = args
            .iter()
            .map(|arg| {
                core::eval(
                    arena,
                    &ctx0.tms,
                    arena.alloc(core::Tm::new(Location::new(0, 0), arg.clone())),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let body = core::eval(
            arena,
            &ctx0.tms,
            arena.alloc(core::Tm::new(Location::new(0, 0), return_ty.clone())),
        )?;

        Ok(ctx0.bind_def(
            name.to_string(),
            arena.alloc(core::Val::FunTy { args, body }),
            arena.alloc(core::Val::FunForeign { f: fun }),
        )) as Result<_, EvalError>
    })
    .expect("could not evaluate standard library!");

    // this is a bit insane, clean it up later
    if with_read {
        ctx = ctx.bind_param(
            "read".to_string(),
            core::eval(
                arena,
                &ctx.tms,
                &core::Tm::new(
                    Location::new(0, 0),
                    core::TmData::FunApp {
                        head: Arc::new(core::Tm::new(
                            Location::new(0, 0),
                            core::TmData::FunForeignLit {
                                args: vec![],
                                body: Arc::new(core::library::read_ty),
                            },
                        )),
                        args: vec![],
                    },
                ),
            )
            .expect("could not evaluate standard library!"),
            arena,
        );
    }

    ctx
}

pub fn foreign<'a>(
    location: &Location,
    name: &str,
) -> Result<
    Arc<
        dyn Fn(&'a Arena, &Location, &[&'a Val<'a>]) -> Result<&'a Val<'a>, EvalError>
            + Send
            + Sync,
    >,
    EvalError,
> {
    match name {
        "csv_ty" => Ok(Arc::new(csv_ty)),
        "csv_first" => Ok(Arc::new(csv_first)),

        "binary_plus" => Ok(Arc::new(binary_plus)),
        "binary_times" => Ok(Arc::new(binary_times)),
        "binary_division" => Ok(Arc::new(binary_division)),
        "binary_modulo" => Ok(Arc::new(binary_modulo)),
        "binary_exponent" => Ok(Arc::new(binary_exponent)),
        "binary_minus" => Ok(Arc::new(binary_minus)),
        "binary_equal" => Ok(Arc::new(binary_equal)),
        "binary_not_equal" => Ok(Arc::new(binary_not_equal)),
        "binary_less_than" => Ok(Arc::new(binary_less_than)),
        "binary_greater_than" => Ok(Arc::new(binary_greater_than)),
        "binary_less_than_or_equal" => Ok(Arc::new(binary_less_than_or_equal)),
        "binary_greater_than_or_equal" => Ok(Arc::new(binary_greater_than_or_equal)),
        "binary_and" => Ok(Arc::new(binary_and)),
        "binary_or" => Ok(Arc::new(binary_or)),

        "unary_minus" => Ok(Arc::new(unary_minus)),
        "unary_reverse_complement" => Ok(Arc::new(unary_reverse_complement)),

        _ => Err(EvalError::new(location, "foreign function does not exist")),
    }
}

pub fn binary_plus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0 + n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_times<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0 * n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_division<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0 / n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_modulo<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0 % n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_exponent<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0.powf(*n1) })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_minus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Num { n: n0 - n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(arena.alloc(Val::Bool {
            b: v0.eq(arena, v1),
        })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_not_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(arena.alloc(Val::Bool {
            b: !v0.eq(arena, v1),
        })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_less_than<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Bool { b: n0 < n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_greater_than<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Bool { b: n0 > n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_less_than_or_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Bool { b: n0 <= n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_greater_than_or_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(arena.alloc(Val::Bool { b: n0 >= n1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_and<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(arena.alloc(Val::Bool { b: *b0 && *b1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_or<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(arena.alloc(Val::Bool { b: *b0 || *b1 })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_minus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(arena.alloc(Val::Num { n: -n })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_reverse_complement<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(arena.alloc(Val::Str {
            s: arena.alloc(util::rev_comp(s)),
        })),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn csv_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => {
            // open the file
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(
                    &bytes_to_string(s)
                        .map_err(|e| EvalError::from_internal(e, location.clone()))?,
                ))
                .map_err(|_| EvalError {
                    location: location.clone(),
                    message: "could not load file".to_string(),
                })?;

            // get the type from the headers
            // Ok(arena.alloc(Val::RecTy {
            //     fields: rdr
            //         .headers()
            //         .map_err(|_| EvalError {
            //             location: location.clone(),
            //             message: "CSV file had no headers".to_string(),
            //         })?
            //         .iter()
            //         .map(|header| {
            //             crate::util::CoreRecField::new(
            //                 arena.alloc(header.clone()).as_bytes() as &[u8],
            //                 arena.alloc(Val::StrTy) as &Val<'a>,
            //             )
            //         })
            //         .collect(),
            // }))

            todo!()
        }
        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn csv_first<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => {
            // open the file
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(
                    &bytes_to_string(s)
                        .map_err(|e| EvalError::from_internal(e, location.clone()))?,
                ))
                .map_err(|_| EvalError::new(location, "could not load file"))?;

            // get the type from the headers
            // Ok(&Val::Rec(ConcreteRec { map:  }))

            todo!()
        }
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn read_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[&'a Val<'a>],
) -> Result<&'a Val<'a>, EvalError> {
    match vtms {
        [] => Ok(arena.alloc(Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"seq",
                    data: arena.alloc(Val::StrTy),
                },
                CoreRecField {
                    name: b"id",
                    data: arena.alloc(Val::StrTy),
                },
                CoreRecField {
                    name: b"desc",
                    data: arena.alloc(Val::StrTy),
                },
            ],
        })),
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}
