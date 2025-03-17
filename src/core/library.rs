use crate::util::{self, bytes_to_string, Arena, Location};
use std::{path::Path, sync::Arc};

use super::{rec::ConcreteRec, EvalError, Val};

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
