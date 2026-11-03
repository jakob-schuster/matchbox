use itertools::Itertools;

use crate::{
    core::{
        self,
        rec::{FullyConcreteRec, Rec2},
        ListVal, StrVal,
    },
    input::{get_filetype_and_buffer, FileType},
    myers::VarMyers,
    surface::elab2::context::Context,
    util::{
        self, bytes_to_string, cache::Cache, env::Env, get_bit, location::Location,
        recfield::CoreRecField, Arena,
    },
};
use std::{collections::HashMap, io::Read, path::Path, sync::Arc};

use super::{
    rec::{self, ConcreteRec},
    Effect, EvalError, PortableVal, Val,
};

pub fn standard_library<'a>() -> Context<'a> {
    Context::new()
        .bind("Type", Val::Univ, Val::Univ)
        .bind("Any", Val::Univ, Val::AnyTy)
        .bind("Bool", Val::Univ, Val::BoolTy)
        .bind("Num", Val::Univ, Val::NumTy)
        .bind("Str", Val::Univ, Val::StrTy)
        .bind("Effect", Val::Univ, Val::EffectTy)
}

pub fn foreign<'a>(
    location: &Location,
    name: &str,
) -> Result<
    Arc<dyn for<'b> Fn(&Location, &[Val<'b>]) -> Result<Val<'b>, EvalError> + Send + Sync>,
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

        "unary_not" => Ok(Arc::new(unary_not)),

        "read_ty" => Ok(Arc::new(read_ty)),

        "len" => Ok(Arc::new(len)),
        "slice" => Ok(Arc::new(slice)),
        "tag" => Ok(Arc::new(tag)),
        "rename" => Ok(Arc::new(rename)),
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

        "lookup" => Ok(Arc::new(lookup)),
        "to_upper" => Ok(Arc::new(to_upper)),
        "to_lower" => Ok(Arc::new(to_lower)),
        "describe" => Ok(Arc::new(describe)),
        "count_matches" => Ok(Arc::new(count_matches)),
        "contains" => Ok(Arc::new(contains)),
        "distance" => Ok(Arc::new(distance)),
        "to_str" => Ok(Arc::new(to_str)),

        "to_num" => Ok(Arc::new(to_num)),

        "stdout!" => Ok(Arc::new(stdout_eff)),
        "count!" => Ok(Arc::new(count_eff)),
        "out!" => Ok(Arc::new(out_eff)),
        "average!" => Ok(Arc::new(average_eff)),

        "min" => Ok(Arc::new(min)),
        "max" => Ok(Arc::new(max)),
        "mean" => Ok(Arc::new(mean)),

        "to_qscores" => Ok(Arc::new(to_qscores)),

        "flag_paired" => Ok(Arc::new(flag_paired)),
        "flag_mapped_in_proper_pair" => Ok(Arc::new(flag_mapped_in_proper_pair)),
        "flag_unmapped" => Ok(Arc::new(flag_unmapped)),
        "flag_mate_unmapped" => Ok(Arc::new(flag_mate_unmapped)),
        "flag_reverse_strand" => Ok(Arc::new(flag_reverse_strand)),
        "flag_mate_reverse_strand" => Ok(Arc::new(flag_mate_reverse_strand)),
        "flag_first_in_pair" => Ok(Arc::new(flag_first_in_pair)),
        "flag_second_in_pair" => Ok(Arc::new(flag_second_in_pair)),
        "flag_not_primary_alignment" => Ok(Arc::new(flag_not_primary_alignment)),
        "flag_fails_platform_quality_checks" => Ok(Arc::new(flag_fails_platform_quality_checks)),
        "flag_pcr_or_optical_duplicate" => Ok(Arc::new(flag_pcr_or_optical_duplicate)),
        "flag_supplementary_alignment" => Ok(Arc::new(flag_supplementary_alignment)),

        "round" => Ok(Arc::new(round)),
        "floor" => Ok(Arc::new(floor)),
        "ceil" => Ok(Arc::new(ceil)),

        _ => Err(EvalError::new(location, "foreign function does not exist")),
    }
}

pub fn binary_plus<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 + n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_times<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 * n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_division<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 / n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_modulo<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 % n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_exponent<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0.powf(*n1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_minus<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Num { n: n0 - n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_equal<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(Val::Bool { b: v0.eq(v1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_not_equal<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v0, v1] => Ok(Val::Bool { b: !v0.eq(v1) }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_less_than<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n: n0 }, Val::Num { n: n1 }] => Ok(Val::Bool { b: n0 < n1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_greater_than<'a>(
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

pub fn binary_and<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(Val::Bool { b: *b0 && *b1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn binary_or<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }, Val::Bool { b: b1 }] => Ok(Val::Bool { b: *b0 || *b1 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_minus<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Num { n: -n }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_reverse_complement<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: StrVal::new_concrete(util::rev_comp(s.as_bytes())),
        }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn unary_read_reverse_complement<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }] => {
            if let Ok(Val::Str { s: seq }) = rec.get(b"seq") {
                if let Ok(Val::Str { s: qual }) = rec.get(b"qual") {
                    let mut rev_qual = qual.as_bytes().to_vec();
                    rev_qual.reverse();

                    Ok(Val::Rec {
                        rec: rec.with_all(&[
                            (
                                b"seq",
                                Val::Str {
                                    s: StrVal::new_concrete(util::rev_comp(seq.as_bytes())),
                                },
                            ),
                            (
                                b"qual",
                                Val::Str {
                                    s: StrVal::new_concrete(rev_qual),
                                },
                            ),
                        ]),
                    })
                } else {
                    Ok(Val::Rec {
                        rec: rec.with(
                            b"seq",
                            Val::Str {
                                s: StrVal::new_concrete(util::rev_comp(seq.as_bytes())),
                            },
                        ),
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

pub fn unary_not<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Bool { b: b0 }] => Ok(Val::Bool { b: !b0 }),

        _ => Err(EvalError {
            location: location.clone(),
            message: "bad arguments given to function?!".to_string(),
        }),
    }
}

pub fn paired_read_ty<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v1, v2] => Ok(Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1".to_vec(),
                    data: read_ty(location, &[v1.clone()])?,
                },
                CoreRecField {
                    name: b"r2".to_vec(),
                    data: read_ty(location, &[v2.clone()])?,
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

pub fn read_ty<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let (filetype, _) =
                get_filetype_and_buffer(bytes_to_string(filename.as_bytes()).unwrap().as_str())
                    .map_err(|e| {
                        EvalError::new(location, "reads were of an unfamiliar file type!")
                    })?;

            match filetype {
                FileType::Fasta => Ok(Val::RecTy {
                    fields: vec![
                        CoreRecField {
                            name: b"seq".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"desc".to_vec(),
                            data: Val::StrTy,
                        },
                    ],
                }),
                FileType::Fastq => Ok(Val::RecTy {
                    fields: vec![
                        CoreRecField {
                            name: b"seq".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"desc".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"qual".to_vec(),
                            data: Val::StrTy,
                        },
                    ],
                }),

                FileType::Sam | FileType::Bam => Ok(Val::RecTy {
                    // in the order as described in the spec
                    fields: vec![
                        // Query template name
                        CoreRecField {
                            name: b"qname".to_vec(),
                            data: Val::StrTy,
                        },
                        CoreRecField {
                            name: b"id".to_vec(),
                            data: Val::StrTy,
                        },
                        // Bitwise flag
                        CoreRecField {
                            name: b"flag".to_vec(),
                            data: Val::NumTy,
                        },
                        // Reference sequence name
                        CoreRecField {
                            name: b"rname".to_vec(),
                            data: Val::StrTy,
                        },
                        // 1-based leftmost mapping position
                        CoreRecField {
                            name: b"pos".to_vec(),
                            data: Val::NumTy,
                        },
                        // Mapping quality
                        CoreRecField {
                            name: b"mapq".to_vec(),
                            data: Val::NumTy,
                        },
                        // The CIGAR string
                        CoreRecField {
                            name: b"cigar".to_vec(),
                            data: Val::StrTy,
                        },
                        // Reference name of the mate / next read
                        CoreRecField {
                            name: b"rnext".to_vec(),
                            data: Val::StrTy,
                        },
                        // Position of the mate / next read
                        CoreRecField {
                            name: b"pnext".to_vec(),
                            data: Val::NumTy,
                        },
                        // Observed template length
                        CoreRecField {
                            name: b"tlen".to_vec(),
                            data: Val::NumTy,
                        },
                        // The actual sequence of the alignment
                        CoreRecField {
                            name: b"seq".to_vec(),
                            data: Val::StrTy,
                        },
                        // The quality string of the alignment
                        CoreRecField {
                            name: b"qual".to_vec(),
                            data: Val::StrTy,
                        },
                        // The optional tags associated with the alignment
                        CoreRecField {
                            name: b"tags".to_vec(),
                            data: Val::StrTy,
                            // data: Val::ListTy {
                            //     ty: Arc::new(Val::StrTy),
                            // },
                        },
                        // The optional tags associated with the alignment
                        CoreRecField {
                            name: b"desc".to_vec(),
                            data: Val::StrTy,
                        },
                    ],
                }),

                // CSV and TSV types can be inferred too
                FileType::CSV => csv_ty(location, vtms),
                FileType::TSV => tsv_ty(location, vtms),

                _ => Err(EvalError::new(
                    &location,
                    &format!(
                        "unknown file type '{}'",
                        String::from_utf8(filename.as_bytes().to_vec()).unwrap()
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

pub fn len<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Num {
            n: s.as_bytes().len() as f64,
        }),
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn slice<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }, Val::Num { n: start }, Val::Num { n: end }] => {
            if start.round() >= 0.0 && (end.round() as usize) <= s.as_bytes().len() {
                Ok(Val::Str {
                    s: s.slice(start.round() as usize, end.round() as usize),
                })
            } else {
                Err(EvalError::new(
                    location,
                    &format!(
                        "couldn't slice string '{}' from position {} to {}",
                        bytes_to_string(s.as_bytes()).unwrap(),
                        start,
                        end
                    ),
                ))
            }
        }
        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tag<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }, Val::Str { s }, Val::Str { s: prefix }] => {
            let desc = rec
                .get(b"desc")
                .expect("record with no desc given to tag?!");

            match desc {
                Val::Str { s: old_s } => {
                    let new_desc = old_s
                        .as_bytes()
                        .iter()
                        .chain(prefix.as_bytes())
                        .chain(s.as_bytes())
                        .cloned()
                        .collect_vec();

                    Ok(Val::Rec {
                        rec: if rec.get(b"tags").is_ok() {
                            rec.with_all(&[
                                (
                                    b"desc",
                                    Val::Str {
                                        s: StrVal::new_concrete(new_desc.clone()),
                                    },
                                ),
                                (
                                    b"tags",
                                    Val::Str {
                                        s: StrVal::new_concrete(new_desc.clone()),
                                    },
                                ),
                            ])
                        } else {
                            rec.with(
                                b"desc",
                                Val::Str {
                                    s: StrVal::new_concrete(new_desc),
                                },
                            )
                        },
                    })
                }
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

pub fn rename<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec }, Val::Str { s: new_id }] => {
            let id = rec.get(b"id").expect("record with no id given to rename?!");

            match id {
                Val::Str { s: _ } => Ok(Val::Rec {
                    rec: if rec.get(b"qname").is_ok() {
                        rec.with_all(&[
                            (
                                b"id",
                                Val::Str {
                                    s: new_id.concretise(),
                                },
                            ),
                            (
                                b"qname",
                                Val::Str {
                                    s: new_id.concretise(),
                                },
                            ),
                        ])
                    } else {
                        rec.with(
                            b"id",
                            Val::Str {
                                s: new_id.concretise(),
                            },
                        )
                    },
                }),
                _ => panic!("record id was not string type?!"),
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

pub fn translate<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }, Val::Str { s: stop_codon_char }, Val::Str {
            s: illegal_codon_char,
        }] => match (stop_codon_char.as_bytes(), illegal_codon_char.as_bytes()) {
            ([c1], [c2]) => Ok(Val::Str {
                s: StrVal::new_concrete(util::translate(s.as_bytes(), c1, c2).as_bytes().to_vec()),
            }),
            _ => Err(EvalError::new(
                location,
                "stop codon and illegal codon can only be represented by one character",
            )),
        },
        _ => Err(EvalError::new(
            location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn translate2<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }, Val::Str { s: stop_codon_char }, Val::Str {
            s: illegal_codon_char,
        }] => match (stop_codon_char.as_bytes(), illegal_codon_char.as_bytes()) {
            ([c1], [c2]) => Ok(Val::Str {
                s: core::StrVal::Concrete {
                    s: util::translate(s.as_bytes(), c1, c2).as_bytes().to_vec(),
                },
            }),
            _ => Err(EvalError::new(
                location,
                "stop codon and illegal codon can only be represented by one character",
            )),
        },
        _ => Err(EvalError::new(
            location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn concat<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
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
                s: StrVal::new_concrete(
                    seq0.as_bytes()
                        .iter()
                        .chain(seq1.as_bytes())
                        .cloned()
                        .collect::<Vec<_>>(),
                ),
            };

            match (read0.get(b"qual"), read1.get(b"qual")) {
                (Ok(Val::Str { s: qual0 }), Ok(Val::Str { s: qual1 })) => {
                    let new_qual = Val::Str {
                        s: StrVal::new_concrete(
                            qual0
                                .as_bytes()
                                .iter()
                                .chain(qual1.as_bytes())
                                .cloned()
                                .collect::<Vec<_>>(),
                        ),
                    };

                    Ok(Val::Rec {
                        rec: read1.with_all(&[(b"seq", new_seq), (b"qual", new_qual)]),
                    })
                }
                _ => Ok(Val::Rec {
                    rec: read1.with(b"seq", new_seq),
                }),
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}
pub fn str_concat<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s0 }, Val::Str { s: s1 }] => Ok(Val::Str {
            s: StrVal::new_concrete(
                s0.as_bytes()
                    .iter()
                    .chain(s1.as_bytes())
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn csv_ty<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(
                    &String::from_utf8(filename.as_bytes().to_vec()).unwrap(),
                ))
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| a.iter().map(String::from))
                .collect::<Vec<_>>();

            let fields = field_names
                .iter()
                .map(|name| CoreRecField::new(name.as_bytes(), Val::StrTy))
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

pub fn csv<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .from_path(Path::new(
                    &String::from_utf8(filename.as_bytes().to_vec()).unwrap(),
                ))
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| a.iter().map(String::from))
                .collect::<Vec<_>>();

            let structs: Vec<_> = rdr
                .records()
                .map(|r| {
                    r.map(|sr| -> Val<'_> {
                        Val::Rec {
                            rec: Rec2::FullyConcrete {
                                r: FullyConcreteRec {
                                    map: HashMap::from_iter(sr.into_iter().enumerate().map(
                                        |(i, s)| {
                                            let field = match field_names.get(i) {
                                                Some(a) => a.as_bytes().to_vec(),
                                                None => todo!(),
                                            };

                                            let val: Val = Val::Str {
                                                s: StrVal::new_concrete(
                                                    s.to_string().as_bytes().to_vec(),
                                                ),
                                            };

                                            (field, val)
                                        },
                                    )),
                                },
                            },
                        }
                    })
                })
                .try_collect()
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            Ok(Val::List {
                v: ListVal::new_concrete(structs),
            })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tsv_ty<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(
                    &String::from_utf8(filename.as_bytes().to_vec()).unwrap(),
                ))
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| a.iter().map(String::from))
                .collect::<Vec<_>>();

            let fields = field_names
                .iter()
                .map(|name| CoreRecField::new(name.as_bytes(), Val::StrTy))
                .collect::<Vec<_>>();

            Ok(Val::RecTy { fields })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn tsv<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(Path::new(
                    &String::from_utf8(filename.as_bytes().to_vec()).unwrap(),
                ))
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            let field_names = rdr
                .headers()
                .iter()
                .flat_map(|a| a.iter().map(String::from))
                .collect::<Vec<_>>();

            let structs: Vec<_> = rdr
                .records()
                .map(|r| {
                    r.map(|sr| -> Val<'_> {
                        Val::Rec {
                            rec: Rec2::FullyConcrete {
                                r: FullyConcreteRec {
                                    map: HashMap::from_iter(sr.into_iter().enumerate().map(
                                        |(i, s)| {
                                            let field = match field_names.get(i) {
                                                Some(a) => a.as_bytes().to_vec(),
                                                None => todo!(),
                                            };

                                            let val: Val = Val::Str {
                                                s: StrVal::new_concrete(
                                                    s.to_string().as_bytes().to_vec(),
                                                ),
                                            };

                                            (field, val)
                                        },
                                    )),
                                },
                            },
                        }
                    })
                })
                .try_collect()
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't open file '{}'",
                            bytes_to_string(filename.as_bytes()).unwrap()
                        ),
                    )
                })?;

            Ok(Val::List {
                v: ListVal::new_concrete(structs),
            })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn fasta<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: filename }] => {
            let f = bio::io::fasta::Reader::from_file(
                String::from_utf8(filename.as_bytes().to_vec()).unwrap(),
            )
            .map_err(|_| {
                EvalError::new(
                    location,
                    &format!(
                        "couldn't open file '{}'",
                        bytes_to_string(filename.as_bytes()).unwrap()
                    ),
                )
            })?;

            let mut v = vec![];

            for rec in f.records() {
                match rec {
                    Ok(read) => {
                        let seq = read.seq().to_vec();
                        let name = read.id().as_bytes().to_vec();

                        let map: HashMap<Vec<u8>, Val<'_>> = HashMap::from([
                            (
                                b"id".to_vec(),
                                Val::Str {
                                    s: StrVal::new_concrete(name),
                                },
                            ),
                            (
                                b"seq".to_vec(),
                                Val::Str {
                                    s: StrVal::new_concrete(seq),
                                },
                            ),
                        ]);

                        v.push(Val::Rec {
                            rec: Rec2::FullyConcrete {
                                r: FullyConcreteRec { map },
                            },
                        });
                    }
                    Err(_) => {
                        return Err(EvalError::new(
                            location,
                            &format!(
                                "bad read in FASTA file '{}'",
                                bytes_to_string(filename.as_bytes()).unwrap()
                            ),
                        ))
                    }
                }
            }

            Ok(Val::List {
                v: ListVal::new_concrete(v),
            })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn find_first<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s1 }, Val::Str { s: s2 }] => Ok(Val::Num {
            n: match String::from_utf8(s1.as_bytes().to_vec())
                .unwrap()
                .find(&String::from_utf8(s2.as_bytes().to_vec()).unwrap())
            {
                Some(i) => i as f64,
                None => -1.0,
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn find_last<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s1 }, Val::Str { s: s2 }] => Ok(Val::Num {
            n: match String::from_utf8(s1.as_bytes().to_vec())
                .unwrap()
                .rfind(&String::from_utf8(s2.as_bytes().to_vec()).unwrap())
            {
                Some(i) => i as f64,
                None => -1.0,
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn find_matches<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s1 }, Val::Str { s: s2 }, Val::Num { n }] => {
            let myers = VarMyers::new(s2.as_bytes());
            let edit_dist = (s2.as_bytes().len() as f64 * n).floor() as usize;

            myers.find_all_disjoint(s1.as_bytes(), edit_dist);

            Ok(Val::Num {
                n: match String::from_utf8(s1.as_bytes().to_vec())
                    .unwrap()
                    .rfind(&String::from_utf8(s2.as_bytes().to_vec()).unwrap())
                {
                    Some(i) => i as f64,
                    None => -1.0,
                },
            })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn lookup<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }, Val::Str { s: key }, Val::Str { s: kv_sep }, Val::Str { s: sep }] => {
            let s1 = String::from_utf8(s.as_bytes().to_vec()).unwrap();
            let key = String::from_utf8(
                key.as_bytes()
                    .iter()
                    .chain(kv_sep.as_bytes())
                    .cloned()
                    .collect_vec(),
            )
            .unwrap();
            let sep = String::from_utf8(sep.as_bytes().to_vec()).unwrap();

            if let Some(start) = s1.find(&key) {
                if let Some(end) = s1[start + key.len()..].find(&sep) {
                    Ok(Val::Str {
                        s: s.slice(start + key.len(), end - start + key.len()),
                    })
                } else {
                    Ok(Val::Str {
                        // WARN check this
                        s: s.slice(start + key.len(), s.as_bytes().len()),
                    })
                }
            } else {
                // WARN this looks like weird behaviour
                Ok(Val::Str { s: s.slice(0, 0) })
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_upper<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: StrVal::new_concrete(s.as_bytes().to_ascii_uppercase().to_vec()),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_lower<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Str {
            s: StrVal::new_concrete(s.as_bytes().to_ascii_lowercase().to_vec()),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn describe<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
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
                if *reverse_complement {
                    let map_reverse_complement = map
                        .iter()
                        .map(|(key, val)| {
                            (
                                format!("-{}", util::bytes_to_string(key).unwrap())
                                    .as_bytes()
                                    .to_vec(),
                                StrVal::new_concrete(util::rev_comp(val.as_bytes())),
                            )
                        })
                        .collect::<HashMap<_, _>>();

                    map = map
                        .into_iter()
                        .chain(map_reverse_complement)
                        .collect::<HashMap<_, _>>();
                }

                // describe the read
                let description = map
                    .iter()
                    .flat_map(|(id, seq)| {
                        VarMyers::new(seq.as_bytes())
                            .find_all_disjoint(
                                read_seq.as_bytes(),
                                (error_rate * seq.as_bytes().len() as f64).round() as usize,
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
                    s: StrVal::new_concrete(if description.is_empty() {
                        String::from("[ _ ]").as_bytes().to_vec()
                    } else {
                        format!("[ _ {description} _ ]").as_bytes().to_vec()
                    }),
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

pub fn count_matches<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Rec { rec: read }, Val::Str { s: seq }, Val::Num { n: error_rate }] => {
            if let Val::Str { s: read_seq } = read
                .get(b"seq")
                .map_err(|e| EvalError::from_internal(e, location.clone()))?
            {
                let matches = VarMyers::new(seq.as_bytes())
                    .find_all_disjoint(
                        read_seq.as_bytes(),
                        (error_rate * seq.as_bytes().len() as f64).round() as usize,
                    )
                    .len();

                // allocate the description
                Ok(Val::Num { n: matches as f64 })
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

pub fn contains<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::List { v }, v0] => {
            // look for the query value in the list
            for v1 in v.get_reference() {
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

pub fn distance<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s: s0 }, Val::Str { s: s1 }] => {
            let distance = bio::alignment::distance::levenshtein(s0.as_bytes(), s1.as_bytes());
            Ok(Val::Num { n: distance as f64 })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_str<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v @ Val::Str { .. }] => Ok(v.clone()),
        // all else is turned into a string and then allocated
        [v] => Ok(Val::Str {
            s: StrVal::new_concrete(v.to_string().as_bytes().to_vec()),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_num<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => Ok(Val::Num {
            n: String::from_utf8(s.as_bytes().to_vec())
                .unwrap()
                .parse::<f64>()
                .map_err(|_| {
                    EvalError::new(
                        location,
                        &format!(
                            "couldn't convert string '{}' to a Num!",
                            bytes_to_string(s.as_bytes()).unwrap()
                        ),
                    )
                })?,
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn stdout_eff<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v] => Ok(Val::Effect {
            effect: Effect::Stdout {
                val: v.to_portable(),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn count_eff<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        // string types pass straight through to reduce allocations
        [v, Val::Str { s: name }] => Ok(Val::Effect {
            effect: Effect::Count {
                val: v.to_portable(),
                name: name.as_bytes().to_vec(),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn out_eff<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [v, Val::Str { s: filename }] => Ok(Val::Effect {
            effect: Effect::Out {
                val: v.to_portable(),
                name: filename.as_bytes().to_vec(),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn average_eff<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }, Val::Str { s: name }] => Ok(Val::Effect {
            effect: Effect::Mean {
                num: *n,
                name: name.as_bytes().to_vec(),
            },
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn min<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::List { v }] => {
            let mut min = None;
            for val in v.get_reference() {
                match val {
                    Val::Num { n } => match min {
                        Some(m) => min = Some(f64::min(*n, m)),
                        None => min = Some(*n),
                    },
                    _ => panic!("applying `min` on a non-Num val!?"),
                }
            }

            match min {
                Some(n) => Ok(Val::Num { n }),
                None => Err(EvalError::new(location, "can't apply `min` to empty list")),
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn max<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::List { v }] => {
            let mut max = None;
            for val in v.get_reference() {
                match val {
                    Val::Num { n } => match max {
                        Some(m) => max = Some(f64::max(*n, m)),
                        None => max = Some(*n),
                    },
                    _ => panic!("applying `max` on a non-Num val!?"),
                }
            }

            match max {
                Some(n) => Ok(Val::Num { n }),
                None => Err(EvalError::new(location, "can't apply `max` to empty list")),
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn mean<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::List { v }] => {
            let mut total = None;
            for val in v.get_reference() {
                match val {
                    Val::Num { n } => match total {
                        Some(t) => total = Some(t + *n),
                        None => total = Some(*n),
                    },
                    _ => panic!("applying `min` on a non-Num val!?"),
                }
            }

            match total {
                Some(t) => Ok(Val::Num {
                    n: t / v.get_reference().len() as f64,
                }),
                None => Err(EvalError::new(location, "can't apply `min` to empty list")),
            }
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn to_qscores<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Str { s }] => {
            let mut v = vec![];
            for c in s.as_bytes() {
                // check that c is a valid Phred character
                if *c < 33 {
                    return Err(EvalError::new(
                        location,
                        &format!("couldn't interpret '{}' as a quality score; not in the Phred character set", *c as char),
                    ));
                }

                let qscore = c - 33;

                v.push(Val::Num { n: qscore as f64 });
            }

            Ok(Val::List {
                v: ListVal::new_concrete(v),
            })
        }

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_paired<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 0),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_mapped_in_proper_pair<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 1),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_unmapped<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 2),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_mate_unmapped<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 3),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_reverse_strand<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 4),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_mate_reverse_strand<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 5),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_first_in_pair<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 6),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_second_in_pair<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 7),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_not_primary_alignment<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 8),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_fails_platform_quality_checks<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 9),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_pcr_or_optical_duplicate<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 10),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn flag_supplementary_alignment<'a>(
    location: &Location,
    vtms: &[Val<'a>],
) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Bool {
            b: get_bit(n.round() as u16, 11),
        }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn round<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Num { n: n.round() }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn floor<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Num { n: n.floor() }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}

pub fn ceil<'a>(location: &Location, vtms: &[Val<'a>]) -> Result<Val<'a>, EvalError> {
    match vtms {
        [Val::Num { n }] => Ok(Val::Num { n: n.ceil() }),

        _ => Err(EvalError::new(
            &location,
            "bad arguments given to function?!",
        )),
    }
}
