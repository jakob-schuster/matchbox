use itertools::Itertools;

use crate::{
    core,
    myers::VarMyers,
    read::{get_filetype_and_buffer, FileType},
    surface::Context,
    util::{self, bytes_to_string, Arena, Cache, CoreRecField, Env, Location},
};
use std::{collections::HashMap, io::Read, path::Path, sync::Arc};

use super::{
    make_portable,
    rec::{self, ConcreteRec},
    Effect, EvalError, PortableVal, Val,
};

pub fn standard_library<'a>(arena: &'a Arena) -> Context<'a> {
    let entries = vec![
        ("Type", core::TmData::Univ, core::TmData::Univ),
        ("Any", core::TmData::Univ, core::TmData::AnyTy),
        ("Bool", core::TmData::Univ, core::TmData::BoolTy),
        ("Num", core::TmData::Univ, core::TmData::NumTy),
        ("Str", core::TmData::Univ, core::TmData::StrTy),
        ("Effect", core::TmData::Univ, core::TmData::EffectTy),
    ];

    let ctx = entries
        .iter()
        .try_fold(Context::default(), |ctx, (name, ty, tm)| {
            // WARN cache can be empty because this is pre-caching

            let ty1 = arena
                .alloc(core::Tm::new(Location::new(0, 0), ty.clone()))
                .eval(arena, &ctx.tms, &Cache::default(), &Env::default())?;
            let tm1 = arena
                .alloc(core::Tm::new(Location::new(0, 0), tm.clone()))
                .eval(arena, &ctx.tms, &Cache::default(), &Env::default())?;

            Ok::<Context, EvalError>(ctx.bind_def(name.to_string(), ty1, tm1))
        })
        .expect("could not evaluate standard library!");

    ctx
}

pub fn foreign<'a>(
    location: &Location,
    name: &str,
) -> Result<
    Arc<
        dyn for<'b> Fn(&'b Arena, &Location, &[Val<'b>]) -> Result<Val<'b>, EvalError>
            + Send
            + Sync,
    >,
    EvalError,
> {
    match name {
        "binary_plus" => Ok(Arc::new(binary_plus)),
        "binary_times" => Ok(Arc::new(binary_times)),
        "binary_minus" => Ok(Arc::new(binary_minus)),
        "binary_division" => Ok(Arc::new(binary_division)),
        "binary_modulo" => Ok(Arc::new(binary_modulo)),
        "binary_exponent" => Ok(Arc::new(binary_exponent)),

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
        "unary_read_reverse_complement" => Ok(Arc::new(unary_read_reverse_complement)),

        "read_ty" => Ok(Arc::new(read_ty)),

        "len" => Ok(Arc::new(len)),
        "slice" => Ok(Arc::new(slice)),
        "tag" => Ok(Arc::new(tag)),
        "translate" => Ok(Arc::new(translate)),
        "str_concat" => Ok(Arc::new(str_concat)),
        "concat" => Ok(Arc::new(concat)),
        "csv_ty" => Ok(Arc::new(csv_ty)),
        "csv" => Ok(Arc::new(csv)),
        "tsv_ty" => Ok(Arc::new(tsv_ty)),
        "tsv" => Ok(Arc::new(tsv)),
        "fasta" => Ok(Arc::new(fasta)),
        "find_first" => Ok(Arc::new(find_first)),
        "find_last" => Ok(Arc::new(find_last)),
        "to_upper" => Ok(Arc::new(to_upper)),
        "to_lower" => Ok(Arc::new(to_lower)),
        "describe" => Ok(Arc::new(describe)),
        "contains" => Ok(Arc::new(contains)),
        "distance" => Ok(Arc::new(distance)),
        "to_str" => Ok(Arc::new(to_str)),

        "stdout!" => Ok(Arc::new(stdout_eff)),
        "count!" => Ok(Arc::new(count_eff)),
        "out!" => Ok(Arc::new(out_eff)),
        "average!" => Ok(Arc::new(average_eff)),

        _ => Err(EvalError::new(location, "foreign function does not exist")),
    }
}

pub fn binary_plus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 + n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_times<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 * n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_division<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 / n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_modulo<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 % n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_exponent<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0.powf(*n1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_minus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 - n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(Val::Bool { b: v0.eq(v1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_not_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(Val::Bool { b: !v0.eq(v1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_less_than<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Bool { b: n0 < n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_greater_than<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Bool { b: n0 > n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_less_than_or_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Bool { b: n0 <= n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_greater_than_or_equal<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Bool { b: n0 >= n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_and<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(Val::Bool { b: *b0 && *b1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_or<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(Val::Bool { b: *b0 || *b1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_minus<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Num { n: -n }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_reverse_complement<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: arena.alloc(util::rev_comp(s)),
        }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_read_reverse_complement<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }] => {
            if let Ok(Val::Str { s: seq }) = rec.get(b"seq") {
                if let Ok(Val::Str { s: qual }) = rec.get(b"qual") {
                    let mut rev_qual = qual.to_vec();
                    rev_qual.reverse();

                    Ok(Val::Rec {
                        rec: Arc::new(rec.with_all(&[
                            (
                                b"seq",
                                Val::Str {
                                    s: arena.alloc(util::rev_comp(seq)),
                                },
                            ),
                            (
                                b"qual",
                                Val::Str {
                                    s: arena.alloc(rev_qual),
                                },
                            ),
                        ])),
                    })
                } else {
                    Ok(Val::Rec {
                        rec: Arc::new(rec.with(
                            b"seq",
                            Val::Str {
                                s: arena.alloc(util::rev_comp(seq)),
                            },
                        )),
                    })
                }
            } else {
                Err(EvalError {
                    location: location.clone(),
                    message: format!(
                        "bad arguments given to function {}?!",
                        vtms.iter().map(|v| v.to_string()).join(", ")
                    ),
                })
            }
        }

        _ => Err(EvalError {
            location: location.clone(),
            message: format!(
                "bad arguments given to function {}?!",
                vtms.iter().map(|v| v.to_string()).join(", ")
            ),
        }),
    }
}

pub fn paired_read_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v1, v2] => Ok(Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: read_ty(arena, location, &[v1.clone()])?,
                },
                CoreRecField {
                    name: b"r2",
                    data: read_ty(arena, location, &[v2.clone()])?,
                },
            ],
        }),
        _ => Err(EvalError::new(
            &location,
            &format!(
                "bad arguments given to function?! {}",
                vtms.iter().join(",")
            ),
        )),
    }
}

pub fn read_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let (filetype, _) = get_filetype_and_buffer(
                bytes_to_string(filename).unwrap().as_str(),
            )
            .map_err(|e| EvalError::new(location, "reads were of an unfamiliar file type!"))?;

            match filetype {
                FileType::Fasta => Ok(Val::RecTy {
                    fields: vec![
                        CoreRecField {
                            name: b"seq",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"desc",
                            data: Val::StrTy,
                        },
                    ],
                }),
                FileType::Fastq => Ok(Val::RecTy {
                    fields: vec![
                        CoreRecField {
                            name: b"seq",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"desc",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"qual",
                            data: Val::StrTy,
                        },
                    ],
                }),

                FileType::Sam | FileType::Bam => Ok(Val::RecTy {
                    // in the order as described in the spec
                    fields: vec![
                        // Query template name
                        CoreRecField {
                            name: b"qname",
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id",
                            data: Val::StrTy,
                        },
                        // Bitwise flag
                        CoreRecField {
                            name: b"flag",
                            data: Val::NumTy,
                        },
                        CoreRecField {
                            name: b"flag_rec",
                            data: Val::RecTy {
                                fields: vec![
                                    CoreRecField {
                                        name: b"paired",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"mapped_in_proper_pair",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"unmapped",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"mate_unmapped",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"reverse_strand",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"mate_reverse_strand",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"first_in_pair",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"second_in_pair",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"not_primary_alignment",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"fails_platform_quality_checks",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"pcr_or_optical_duplicate",
                                        data: Val::BoolTy,
                                    },
                                    CoreRecField {
                                        name: b"supplementary_alignment",
                                        data: Val::BoolTy,
                                    },
                                ],
                            },
                        },
                        // Reference sequence name
                        CoreRecField {
                            name: b"rname",
                            data: Val::StrTy,
                        },
                        // 1-based leftmost mapping position
                        CoreRecField {
                            name: b"pos",
                            data: Val::NumTy,
                        },
                        // Mapping quality
                        CoreRecField {
                            name: b"mapq",
                            data: Val::NumTy,
                        },
                        // The CIGAR string
                        CoreRecField {
                            name: b"cigar",
                            data: Val::StrTy,
                        },
                        // Reference name of the mate / next read
                        CoreRecField {
                            name: b"rnext",
                            data: Val::StrTy,
                        },
                        // Position of the mate / next read
                        CoreRecField {
                            name: b"pnext",
                            data: Val::NumTy,
                        },
                        // Observed template length
                        CoreRecField {
                            name: b"tlen",
                            data: Val::NumTy,
                        },
                        // The actual sequence of the alignment
                        CoreRecField {
                            name: b"seq",
                            data: Val::StrTy,
                        },
                        // The quality string of the alignment
                        CoreRecField {
                            name: b"qual",
                            data: Val::StrTy,
                        },
                        // The optional tags associated with the alignment
                        CoreRecField {
                            name: b"tags",
                            data: Val::StrTy,
                            // data: Val::ListTy {
                            //     ty: Arc::new(Val::StrTy),
                            // },
                        },
                        // The optional tags associated with the alignment
                        CoreRecField {
                            name: b"desc",
                            data: Val::StrTy,
                        },
                    ],
                }),

                // CSV and TSV types can be inferred too
                FileType::CSV => csv_ty(arena, location, vtms),
                FileType::TSV => tsv_ty(arena, location, vtms),

                _ => Err(EvalError::new(
                    &location,
                    &format!(
                        "unknown file type '{}'",
                        String::from_utf8(filename.to_vec()).unwrap()
                    ),
                )),
            }
        }
        _ => Err(EvalError::new(
            &location,
            &format!(
                "bad arguments given to function?! {}",
                vtms.iter().join(",")
            ),
        )),
    }
}

pub fn len<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Num { n: s.len() as f32 }),
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn slice<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }, Val::Num { n: start }, Val::Num { n: end }] => Ok(Val::Str {
            s: &s[start.round() as usize..end.round() as usize],
        }),
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tag<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }, Val::Str { s }, Val::Str { s: prefix }] => {
            let desc = rec
                .get(b"desc")
                .expect("record with no desc given to tag?!");

            match desc {
                Val::Str { s: old_s } => Ok(Val::Rec {
                    rec: Arc::new(
                        rec.with(
                            b"desc",
                            Val::Str {
                                s: arena
                                    .alloc(format!(
                                        "{}{}{}",
                                        util::bytes_to_string(old_s).unwrap(),
                                        util::bytes_to_string(prefix).unwrap(),
                                        util::bytes_to_string(s).unwrap()
                                    ))
                                    .as_bytes(),
                            },
                        ),
                    ),
                }),
                _ => panic!("record desc was not string type?!"),
            }
        }
        _ => Err(EvalError::new(
            &location,
            &format!(
                "bad arguments given to function {}?!",
                vtms.iter().map(|v| v.to_string()).join(", ")
            ),
        )),
    }
}

pub fn translate<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: arena.alloc(util::translate(s)).as_bytes(),
        }),
        _ => Err(EvalError::new(
            location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn concat<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec: read0 }, Val::Rec { rec: read1 }] => {
            // Get sequences from both structs
            let seq0 = match read0
                .get(b"seq")
                .map_err(|e| EvalError::new(location, "read did not have a sequence field"))?
            {
                Val::Str { s } => Ok(s),
                _ => Err(EvalError::new(
                    location,
                    "read's sequence field was of the wrong type",
                )),
            }?;
            let seq1 = match read1
                .get(b"seq")
                .map_err(|e| EvalError::new(location, "read did not have a sequence field"))?
            {
                Val::Str { s } => Ok(s),
                _ => Err(EvalError::new(
                    location,
                    "read's sequence field was of the wrong type",
                )),
            }?;

            // Combine sequences
            let new_seq = Val::Str {
                s: arena.alloc(seq0.iter().chain(seq1).cloned().collect::<Vec<_>>()),
            };

            match (read0.get(b"qual"), read1.get(b"qual")) {
                (Ok(Val::Str { s: qual0 }), Ok(Val::Str { s: qual1 })) => {
                    let new_qual = Val::Str {
                        s: arena.alloc(qual0.iter().chain(qual1).cloned().collect::<Vec<_>>()),
                    };

                    Ok(Val::Rec {
                        rec: Arc::new(read1.with_all(&[(b"seq", new_seq), (b"qual", new_qual)])),
                    })
                }
                _ => Ok(Val::Rec {
                    rec: Arc::new(read1.with(b"seq", new_seq)),
                }),
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}
pub fn str_concat<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s0 }, Val::Str { s: s1 }] => Ok(Val::Str {
            s: &arena.alloc((*s0).iter().chain(*s1).cloned().collect::<Vec<_>>())[..],
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn csv_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(&String::from_utf8(filename.to_vec()).unwrap()))
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| {
                    a.iter()
                        .map(String::from)
                        .map(|s| arena.alloc(s).as_bytes())
                })
                .collect::<Vec<_>>();

            let fields = field_names
                .iter()
                .map(|name| CoreRecField::new(*name, Val::StrTy))
                .collect::<Vec<_>>();

            Ok(Val::RecTy { fields })
        }

        _ => Err(EvalError::new(
            &location,
            &format!(
                "bad arguments given to function?! got ({})",
                vtms.iter().join(", ")
            ),
        )),
    }
}

pub fn csv<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(&String::from_utf8(filename.to_vec()).unwrap()))
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| {
                    a.iter()
                        .map(String::from)
                        .map(|s| arena.alloc(s).as_bytes())
                })
                .collect::<Vec<_>>();

            let structs: Vec<_> = rdr
                .records()
                .map(|r| {
                    r.map(|sr| -> Val<'_> {
                        Val::Rec {
                            rec: Arc::new(ConcreteRec {
                                map: HashMap::from_iter(sr.into_iter().enumerate().map(
                                    |(i, s)| {
                                        let field: &[u8] = field_names.get(i).unwrap();
                                        let val: Val = Val::Str {
                                            s: arena.alloc(s.to_string()).as_bytes(),
                                        };

                                        (field, val)
                                    },
                                )),
                            }),
                        }
                    })
                })
                .try_collect()
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            Ok(Val::List { v: structs })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tsv_ty<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(&String::from_utf8(filename.to_vec()).unwrap()))
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| {
                    a.iter()
                        .map(String::from)
                        .map(|s| arena.alloc(s).as_bytes())
                })
                .collect::<Vec<_>>();

            let fields = field_names
                .iter()
                .map(|name| CoreRecField::new(*name, Val::StrTy))
                .collect::<Vec<_>>();

            Ok(Val::RecTy { fields })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tsv<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(&String::from_utf8(filename.to_vec()).unwrap()))
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| {
                    a.iter()
                        .map(String::from)
                        .map(|s| arena.alloc(s).as_bytes())
                })
                .collect::<Vec<_>>();

            let structs: Vec<_> = rdr
                .records()
                .map(|r| {
                    r.map(|sr| -> Val<'_> {
                        Val::Rec {
                            rec: Arc::new(ConcreteRec {
                                map: HashMap::from_iter(sr.into_iter().enumerate().map(
                                    |(i, s)| {
                                        let field: &[u8] = field_names.get(i).unwrap();
                                        let val: Val = Val::Str {
                                            s: arena.alloc(s.to_string()).as_bytes(),
                                        };

                                        (field, val)
                                    },
                                )),
                            }),
                        }
                    })
                })
                .try_collect()
                .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            Ok(Val::List { v: structs })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn fasta<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let f =
                bio::io::fasta::Reader::from_file(String::from_utf8(filename.to_vec()).unwrap())
                    .map_err(|_| EvalError::new(location, "couldn't open file"))?;

            let mut v = vec![];

            let id_str = arena.alloc(b"id".to_vec());
            let seq_str = arena.alloc(b"seq".to_vec());

            for rec in f.records() {
                match rec {
                    Ok(read) => {
                        let seq = arena.alloc(read.seq().to_vec());
                        let name = arena.alloc(read.id().as_bytes().to_vec());

                        let map: HashMap<&[u8], Val<'_>> = HashMap::from([
                            (id_str as &[u8], Val::Str { s: name }),
                            (seq_str as &[u8], Val::Str { s: seq }),
                        ]);

                        v.push(Val::Rec {
                            rec: Arc::new(ConcreteRec { map }),
                        });
                    }
                    Err(_) => panic!("bad read in fasta"),
                }
            }

            Ok(Val::List { v })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn find_first<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s1 }, Val::Str { s: s2 }] => Ok(Val::Num {
            n: match String::from_utf8((*s1).to_vec())
                .unwrap()
                .find(&String::from_utf8((*s2).to_vec()).unwrap())
            {
                Some(i) => i as f32,
                None => -1.0,
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn find_last<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s1 }, Val::Str { s: s2 }] => Ok(Val::Num {
            n: match String::from_utf8((*s1).to_vec())
                .unwrap()
                .rfind(&String::from_utf8((*s2).to_vec()).unwrap())
            {
                Some(i) => i as f32,
                None => -1.0,
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_upper<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: arena.alloc(s.to_ascii_uppercase().to_vec()),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_lower<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: arena.alloc(s.to_ascii_lowercase().to_vec()),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn describe<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec: read }, Val::Rec { rec: search_terms }, Val::Bool {
            b: reverse_complement,
        }, Val::Num { n: error_rate }] => {
            if let Val::Str { s: read_seq } = read
                .get(b"seq")
                .map_err(|e| EvalError::from_internal(e, location.clone()))?
            {
                // parse the struct, making sure its structure is good
                let mut map = HashMap::new();

                for (name, val) in search_terms.all() {
                    if let Val::Str { s: seq } = val {
                        map.insert(name, seq);
                    } else {
                        return Err(EvalError::new(
                            location,
                            "only sequences are permitted in the describe struct!",
                        ));
                    }
                }

                // if necessary, add the reverse complements
                let little_arena = Arena::new();
                if *reverse_complement {
                    let mut new_map = map.clone();
                    for (id, seq) in &map {
                        new_map.insert(
                            format!("-{}", util::bytes_to_string(id).unwrap())
                                .as_bytes()
                                .to_vec(),
                            little_arena.alloc(util::rev_comp(seq)),
                        );
                    }
                    map = new_map;
                }

                // describe the read
                let description = map
                    .iter()
                    .flat_map(|(id, seq)| {
                        VarMyers::new(seq)
                            .find_all_disjoint(
                                read_seq,
                                (error_rate * read_seq.len() as f32).round() as usize,
                            )
                            .iter()
                            .map(|matches| (id, *matches))
                            .collect_vec()
                    })
                    .sorted_by_key(|(_, (start, _, _))| *start)
                    .map(|(id, _)| util::bytes_to_string(id).unwrap())
                    .join(" _ ");

                // allocate the description
                Ok(Val::Str {
                    s: arena
                        .alloc(if description.is_empty() {
                            String::from("[ _ ]")
                        } else {
                            format!("[ _ {description} _ ]")
                        })
                        .as_bytes(),
                })
            } else {
                Err(EvalError::new(
                    location,
                    "first argument of describe must have the seq field",
                ))
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn contains<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::List { v }, v0] => {
            // look for the query value in the list
            for v1 in v {
                if v1.eq(v0) {
                    // if you find it, return early
                    return Ok(Val::Bool { b: true });
                }
            }
            Ok(Val::Bool { b: false })
        }

        _ => Err(EvalError::new(
            &location,
            &format!(
                "bad arguments given to function: '{}'?!",
                vtms.iter().map(|a| a.to_string()).join(",")
            ),
        )),
    }
}

pub fn distance<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s0 }, Val::Str { s: s1 }] => {
            let distance = bio::alignment::distance::levenshtein(s0, s1);
            Ok(Val::Num { n: distance as f32 })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_str<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v @ Val::Str { .. }] => Ok(v.clone()),
        // all else is turned into a string and then allocated
        [v] => Ok(Val::Str {
            s: arena.alloc(v.to_string()).as_bytes(),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn stdout_eff<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v] => Ok(Val::Effect {
            val: make_portable(arena, v),
            handler: PortableVal::Rec {
                fields: HashMap::from([(
                    b"output".to_vec(),
                    PortableVal::Str {
                        s: b"stdout".to_vec(),
                    },
                )]),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn count_eff<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v, Val::Str { s: name }] => Ok(Val::Effect {
            val: PortableVal::Rec {
                fields: HashMap::from([
                    (b"name".to_vec(), PortableVal::Str { s: name.to_vec() }),
                    (b"val".to_vec(), make_portable(arena, v)),
                ]),
            },
            handler: PortableVal::Rec {
                fields: HashMap::from([(
                    b"output".to_vec(),
                    PortableVal::Str {
                        s: b"counts".to_vec(),
                    },
                )]),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn out_eff<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v, filename @ Val::Str { .. }] => Ok(Val::Effect {
            val: make_portable(arena, v),
            handler: PortableVal::Rec {
                fields: HashMap::from([
                    (
                        b"output".to_vec(),
                        PortableVal::Str {
                            s: b"file".to_vec(),
                        },
                    ),
                    (b"filename".to_vec(), make_portable(arena, filename)),
                ]),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn average_eff<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v, Val::Str { s: name }] => Ok(Val::Effect {
            val: PortableVal::Rec {
                fields: HashMap::from([
                    (b"name".to_vec(), PortableVal::Str { s: name.to_vec() }),
                    (b"val".to_vec(), make_portable(arena, v)),
                ]),
            },
            handler: PortableVal::Rec {
                fields: HashMap::from([(
                    b"output".to_vec(),
                    PortableVal::Str {
                        s: b"average".to_vec(),
                    },
                )]),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn slice_read<'a>(
    arena: &'a Arena,
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }] => {
            let a = todo!();

            todo!()
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}
