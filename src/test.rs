//! Snapshot tests using insta.

use std::collections::HashMap;

use crate::parse::parse;
use crate::{
    core::{self, rec::ConcreteRec, Val},
    generic_error::GenericError,
    global_config::GlobalConfig,
};

/// Parse some given code,
/// printing the surface AST as a string.
#[cfg(test)]
fn parse_test(code: &str) -> Result<String, GenericError> {
    use crate::generic_error::GenericError;

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    Ok(format!("{:?}", prog))
}

/// Elaborate some given code,
/// printing the resulting core AST as a string.
#[cfg(test)]
fn elab_test(code: &str) -> Result<(String, String), GenericError> {
    use crate::{
        core::library::standard_library, generic_error::GenericError, read_code_from_script,
    };

    let global_config = GlobalConfig::default();

    // should never unwrap, because program terminates
    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    let c0 = standard_library();

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, &global_config).map_err(GenericError::from)?;

    // elaborate to generate the context
    let (_, c1) = library_prog.elab(&c0);

    // elaborate to a core program
    let (core_prog, c2) = prog.elab(&c1);

    Ok((core_prog.to_string(), c2.to_string()))
}

/// Execute some given code on one given sequence,
/// printing the resulting effects as a string.
#[cfg(test)]
fn eval_one_fasta_read_test(code: &str, seq: &[u8]) -> Result<String, Vec<GenericError>> {
    use crate::core::{
        rec::{FullyConcreteRec, RecVal},
        StrVal,
    };

    // create a toy read
    let read = core::Val::Rec {
        rec: RecVal::FullyConcrete {
            r: FullyConcreteRec {
                map: [
                    (
                        b"seq".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(seq.to_vec()),
                        } as Val,
                    ),
                    (
                        b"id".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(b"read1".to_vec()),
                        } as Val,
                    ),
                    (
                        b"desc".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(b"".to_vec()),
                        } as Val,
                    ),
                ]
                .into_iter()
                .collect(),
            },
        },
    };

    eval_one_read_test(code, &read)
}
/// Execute some given code on one given sequence,
/// printing the resulting effects as a string.
#[cfg(test)]
fn eval_one_fastq_read_test(
    code: &str,
    seq: &[u8],
    qual: &[u8],
) -> Result<String, Vec<GenericError>> {
    use crate::core::{
        rec::{FullyConcreteRec, RecVal},
        StrVal,
    };

    // create a toy read
    let read = core::Val::Rec {
        rec: RecVal::FullyConcrete {
            r: FullyConcreteRec {
                map: [
                    (
                        b"seq".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(seq.to_vec()),
                        } as Val,
                    ),
                    (
                        b"id".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(b"read1".to_vec()),
                        } as Val,
                    ),
                    (
                        b"desc".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(b"".to_vec()),
                        } as Val,
                    ),
                    (
                        b"qual".to_vec(),
                        core::Val::Str {
                            s: StrVal::new_concrete(qual.to_vec()),
                        } as Val,
                    ),
                ]
                .into_iter()
                .collect(),
            },
        },
    };

    eval_one_read_test(code, &read)
}

/// Given some code and a read, which exists within a given arena,
/// printing the resulting effects as a string
fn eval_one_read_test(code: &str, read: &core::Val) -> Result<String, Vec<GenericError>> {
    use crate::{core::library::standard_library, input::ReaderWithBar};

    let global_config = GlobalConfig::default();

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let c0 = standard_library();

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = match parse(&library_code, &global_config).map_err(GenericError::from) {
        Ok(prog) => prog,
        Err(e) => return Err(vec![e]),
    };

    // elaborate to generate the context
    let (_, c1) = library_prog.elab(&c0);

    // parse the args program
    let args_prog = match parse(
        &format!("args = {{{}}}", global_config.args),
        &global_config,
    )
    .map_err(GenericError::from)
    {
        Ok(prog) => prog,
        Err(e) => return Err(vec![e]),
    };

    let (_, c2) = args_prog.elab(&c1);

    // parse the program
    let prog = match parse(code, &global_config).map_err(GenericError::from) {
        Ok(prog) => prog,
        Err(e) => return Err(vec![e]),
    };

    // load in the input reads
    let reader_with_bar =
        match ReaderWithBar::new(&global_config.input_reads).map_err(GenericError::from) {
            Ok(reader) => reader,
            Err(e) => return Err(vec![e]),
        };

    // elaborate to a core program
    let (core_prog, c3) = prog.elab(&c2.bind_read_from_reader(reader_with_bar.get_ty()));

    // drop the read parameter from the context
    let c4 = c2.with_global_from(c3);

    // at this point, check for elab errors
    if !c4.get_errors().is_empty() {
        return Err(c4
            .get_errors()
            .into_iter()
            .map(GenericError::from)
            .collect::<Vec<_>>());
    }

    // evaluate the program
    Ok(core_prog
        .eval2(&c4.get_global_env(), &c4.get_local_env(), read.reference())
        .unwrap()
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(","))
}

#[test]
fn parse_assignment() {
    insta::assert_snapshot!(format!("{:?}", parse_test(r#"a = 1"#)), @r#"Ok("Located { location: Location { start: 0, end: 5 }, data: ProgData { stmts: [Located { location: Location { start: 0, end: 5 }, data: Let { name: \"a\", tm: Located { location: Location { start: 4, end: 5 }, data: NumLit { n: Int(1) } } } }] } }")"#)
}

#[test]
fn parse_multiple_assignment() {
    insta::assert_snapshot!(format!("{:?}", parse_test(r#"
        a = 1
        b = 2; c = 'hello'
    "#)), @r#"Ok("Located { location: Location { start: 0, end: 46 }, data: ProgData { stmts: [Located { location: Location { start: 9, end: 14 }, data: Let { name: \"a\", tm: Located { location: Location { start: 13, end: 14 }, data: NumLit { n: Int(1) } } } }, Located { location: Location { start: 23, end: 28 }, data: Let { name: \"b\", tm: Located { location: Location { start: 27, end: 28 }, data: NumLit { n: Int(2) } } } }, Located { location: Location { start: 30, end: 41 }, data: Let { name: \"c\", tm: Located { location: Location { start: 34, end: 41 }, data: StrLit { regs: [Located { location: Location { start: 35, end: 40 }, data: Str { s: [104, 101, 108, 108, 111] } }] } } } }] } }")"#)
}

#[test]
fn elab_assignment_pass1() {
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1")), @r#"Ok(("", "global = {Type: 0, Any: 1, Bool: 2, Num: 3, Str: 4, Effect: 5, binary_plus: 6, binary_times: 7, binary_minus: 8, binary_division: 9, binary_modulo: 10, binary_exponent: 11, binary_equal: 12, binary_not_equal: 13, binary_less_than: 14, binary_greater_than: 16, binary_less_than_or_equal: 17, binary_greater_than_or_equal: 18, binary_and: 19, binary_or: 20, unary_minus: 21, unary_reverse_complement: 22, unary_read_reverse_complement: 23, unary_not: 24, read_ty: 25, len: 26, slice: 27, tag: 28, translate: 29, str_concat: 30, concat: 31, csv_ty: 32, csv: 33, tsv_ty: 34, tsv: 35, fasta: 36, find_first: 37, find_last: 38, lookup: 39, to_upper: 40, to_lower: 41, describe: 42, count_matches: 43, contains: 44, distance: 45, to_str: 46, to_num: 47, stdout!: 48, count!: 49, out!: 50, mean!: 51, min: 52, max: 53, mean: 54, to_qscores: 55, paired: 56, mapped_in_proper_pair: 57, unmapped: 58, mate_unmapped: 59, reverse_strand: 60, mate_reverse_strand: 61, first_in_pair: 62, second_in_pair: 63, not_primary_alignment: 64, fails_platform_quality_checks: 65, pcr_or_optical_duplicate: 66, supplementary_alignment: 67, round: 68, floor: 69, ceil: 70, a: 71}\nglobal_tys = {0: Type, 1: Type, 2: Type, 3: Type, 4: Type, 5: Type, 6: (Num, Num) -> Num, 7: (Num, Num) -> Num, 8: (Num, Num) -> Num, 9: (Num, Num) -> Num, 10: (Num, Num) -> Num, 11: (Num, Num) -> Num, 12: (Any, Any) -> Bool, 13: (Any, Any) -> Bool, 14: (Num, Num) -> Bool, 15: (Num, Num) -> Bool, 16: (Num, Num) -> Bool, 17: (Num, Num) -> Bool, 18: (Num, Num) -> Bool, 19: (Bool, Bool) -> Bool, 20: (Bool, Bool) -> Bool, 21: (Num) -> Num, 22: (Str) -> Str, 23: ({ seq = Str .. }) -> { seq = Str .. }, 24: (Bool) -> Bool, 25: (Str) -> Type, 26: (Str) -> Num, 27: (Str, Num, Num) -> Str, 28: ({ desc = Str .. }, Str, prefix: Str = ' ') -> { desc = Str .. }, 29: (Str, stop_codon: Str = '-', illegal_codon: Str = '?') -> Str, 30: (Str, Str) -> Str, 31: ({ seq = Str .. }, { seq = Str .. }) -> { seq = Str .. }, 32: (Str) -> Type, 33: (Str) -> #awaiting([(#cached[32])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 34: (Str) -> Type, 35: (Str) -> #awaiting([(#cached[34])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 36: (Str) -> [{ desc = Str, id = Str, seq = Str }], 37: (Str, Str) -> Num, 38: (Str, Str) -> Num, 39: (Str, Str, kv_sep: Str = '=', sep: Str = ' ') -> Str, 40: (Str) -> Str, 41: (Str) -> Str, 42: ({ seq = Str .. }, {  .. }, reverse_complement: Bool = false, error_rate: Num = 0) -> Str, 43: ({ seq = Str .. }, Str, error_rate: Num = 0) -> Str, 44: ([Any], Any) -> Bool, 45: (Str, Str) -> Num, 46: (Any) -> Str, 47: (Str) -> Num, 48: (Any) -> Effect, 49: (Any, name: Str = 'count') -> Effect, 50: (Any, Str) -> Effect, 51: (Num, name: Str = 'mean') -> Effect, 52: ([Num]) -> Num, 53: ([Num]) -> Num, 54: ([Num]) -> Num, 55: (Str) -> [Num], 56: (Num) -> Bool, 57: (Num) -> Bool, 58: (Num) -> Bool, 59: (Num) -> Bool, 60: (Num) -> Bool, 61: (Num) -> Bool, 62: (Num) -> Bool, 63: (Num) -> Bool, 64: (Num) -> Bool, 65: (Num) -> Bool, 66: (Num) -> Bool, 67: (Num) -> Bool, 68: (Num) -> Num, 69: (Num) -> Num, 70: (Num) -> Num, 71: Num}\nglobal_tms = {0: Type, 1: Any, 2: Bool, 3: Num, 4: Str, 5: Effect, 6: #fun-foreign, 7: #fun-foreign, 8: #fun-foreign, 9: #fun-foreign, 10: #fun-foreign, 11: #fun-foreign, 12: #fun-foreign, 13: #fun-foreign, 14: #fun-foreign, 15: #fun-foreign, 16: #fun-foreign, 17: #fun-foreign, 18: #fun-foreign, 19: #fun-foreign, 20: #fun-foreign, 21: #fun-foreign, 22: #fun-foreign, 23: #fun-foreign, 24: #fun-foreign, 25: #fun-foreign, 26: #fun-foreign, 27: #fun-foreign, 28: #fun-foreign, 29: #fun-foreign, 30: #fun-foreign, 31: #fun-foreign, 32: #fun-foreign, 33: #fun-foreign, 34: #fun-foreign, 35: #fun-foreign, 36: #fun-foreign, 37: #fun-foreign, 38: #fun-foreign, 39: #fun-foreign, 40: #fun-foreign, 41: #fun-foreign, 42: #fun-foreign, 43: #fun-foreign, 44: #fun-foreign, 45: #fun-foreign, 46: #fun-foreign, 47: #fun-foreign, 48: #fun-foreign, 49: #fun-foreign, 50: #fun-foreign, 51: #fun-foreign, 52: #fun-foreign, 53: #fun-foreign, 54: #fun-foreign, 55: #fun-foreign, 56: #fun-foreign, 57: #fun-foreign, 58: #fun-foreign, 59: #fun-foreign, 60: #fun-foreign, 61: #fun-foreign, 62: #fun-foreign, 63: #fun-foreign, 64: #fun-foreign, 65: #fun-foreign, 66: #fun-foreign, 67: #fun-foreign, 68: #fun-foreign, 69: #fun-foreign, 70: #fun-foreign, 71: 1}\nlocal = {}\nlocal_tys = {}\nlocal_tms = {}\nerrors = {}"))"#)
}

#[test]
fn elab_assignment_pass2() {
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1; b = a")), @r#"Ok(("", "global = {Type: 0, Any: 1, Bool: 2, Num: 3, Str: 4, Effect: 5, binary_plus: 6, binary_times: 7, binary_minus: 8, binary_division: 9, binary_modulo: 10, binary_exponent: 11, binary_equal: 12, binary_not_equal: 13, binary_less_than: 14, binary_greater_than: 16, binary_less_than_or_equal: 17, binary_greater_than_or_equal: 18, binary_and: 19, binary_or: 20, unary_minus: 21, unary_reverse_complement: 22, unary_read_reverse_complement: 23, unary_not: 24, read_ty: 25, len: 26, slice: 27, tag: 28, translate: 29, str_concat: 30, concat: 31, csv_ty: 32, csv: 33, tsv_ty: 34, tsv: 35, fasta: 36, find_first: 37, find_last: 38, lookup: 39, to_upper: 40, to_lower: 41, describe: 42, count_matches: 43, contains: 44, distance: 45, to_str: 46, to_num: 47, stdout!: 48, count!: 49, out!: 50, mean!: 51, min: 52, max: 53, mean: 54, to_qscores: 55, paired: 56, mapped_in_proper_pair: 57, unmapped: 58, mate_unmapped: 59, reverse_strand: 60, mate_reverse_strand: 61, first_in_pair: 62, second_in_pair: 63, not_primary_alignment: 64, fails_platform_quality_checks: 65, pcr_or_optical_duplicate: 66, supplementary_alignment: 67, round: 68, floor: 69, ceil: 70, a: 71, b: 72}\nglobal_tys = {0: Type, 1: Type, 2: Type, 3: Type, 4: Type, 5: Type, 6: (Num, Num) -> Num, 7: (Num, Num) -> Num, 8: (Num, Num) -> Num, 9: (Num, Num) -> Num, 10: (Num, Num) -> Num, 11: (Num, Num) -> Num, 12: (Any, Any) -> Bool, 13: (Any, Any) -> Bool, 14: (Num, Num) -> Bool, 15: (Num, Num) -> Bool, 16: (Num, Num) -> Bool, 17: (Num, Num) -> Bool, 18: (Num, Num) -> Bool, 19: (Bool, Bool) -> Bool, 20: (Bool, Bool) -> Bool, 21: (Num) -> Num, 22: (Str) -> Str, 23: ({ seq = Str .. }) -> { seq = Str .. }, 24: (Bool) -> Bool, 25: (Str) -> Type, 26: (Str) -> Num, 27: (Str, Num, Num) -> Str, 28: ({ desc = Str .. }, Str, prefix: Str = ' ') -> { desc = Str .. }, 29: (Str, stop_codon: Str = '-', illegal_codon: Str = '?') -> Str, 30: (Str, Str) -> Str, 31: ({ seq = Str .. }, { seq = Str .. }) -> { seq = Str .. }, 32: (Str) -> Type, 33: (Str) -> #awaiting([(#cached[32])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 34: (Str) -> Type, 35: (Str) -> #awaiting([(#cached[34])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 36: (Str) -> [{ desc = Str, id = Str, seq = Str }], 37: (Str, Str) -> Num, 38: (Str, Str) -> Num, 39: (Str, Str, kv_sep: Str = '=', sep: Str = ' ') -> Str, 40: (Str) -> Str, 41: (Str) -> Str, 42: ({ seq = Str .. }, {  .. }, reverse_complement: Bool = false, error_rate: Num = 0) -> Str, 43: ({ seq = Str .. }, Str, error_rate: Num = 0) -> Str, 44: ([Any], Any) -> Bool, 45: (Str, Str) -> Num, 46: (Any) -> Str, 47: (Str) -> Num, 48: (Any) -> Effect, 49: (Any, name: Str = 'count') -> Effect, 50: (Any, Str) -> Effect, 51: (Num, name: Str = 'mean') -> Effect, 52: ([Num]) -> Num, 53: ([Num]) -> Num, 54: ([Num]) -> Num, 55: (Str) -> [Num], 56: (Num) -> Bool, 57: (Num) -> Bool, 58: (Num) -> Bool, 59: (Num) -> Bool, 60: (Num) -> Bool, 61: (Num) -> Bool, 62: (Num) -> Bool, 63: (Num) -> Bool, 64: (Num) -> Bool, 65: (Num) -> Bool, 66: (Num) -> Bool, 67: (Num) -> Bool, 68: (Num) -> Num, 69: (Num) -> Num, 70: (Num) -> Num, 71: Num, 72: Num}\nglobal_tms = {0: Type, 1: Any, 2: Bool, 3: Num, 4: Str, 5: Effect, 6: #fun-foreign, 7: #fun-foreign, 8: #fun-foreign, 9: #fun-foreign, 10: #fun-foreign, 11: #fun-foreign, 12: #fun-foreign, 13: #fun-foreign, 14: #fun-foreign, 15: #fun-foreign, 16: #fun-foreign, 17: #fun-foreign, 18: #fun-foreign, 19: #fun-foreign, 20: #fun-foreign, 21: #fun-foreign, 22: #fun-foreign, 23: #fun-foreign, 24: #fun-foreign, 25: #fun-foreign, 26: #fun-foreign, 27: #fun-foreign, 28: #fun-foreign, 29: #fun-foreign, 30: #fun-foreign, 31: #fun-foreign, 32: #fun-foreign, 33: #fun-foreign, 34: #fun-foreign, 35: #fun-foreign, 36: #fun-foreign, 37: #fun-foreign, 38: #fun-foreign, 39: #fun-foreign, 40: #fun-foreign, 41: #fun-foreign, 42: #fun-foreign, 43: #fun-foreign, 44: #fun-foreign, 45: #fun-foreign, 46: #fun-foreign, 47: #fun-foreign, 48: #fun-foreign, 49: #fun-foreign, 50: #fun-foreign, 51: #fun-foreign, 52: #fun-foreign, 53: #fun-foreign, 54: #fun-foreign, 55: #fun-foreign, 56: #fun-foreign, 57: #fun-foreign, 58: #fun-foreign, 59: #fun-foreign, 60: #fun-foreign, 61: #fun-foreign, 62: #fun-foreign, 63: #fun-foreign, 64: #fun-foreign, 65: #fun-foreign, 66: #fun-foreign, 67: #fun-foreign, 68: #fun-foreign, 69: #fun-foreign, 70: #fun-foreign, 71: 1, 72: 1}\nlocal = {}\nlocal_tys = {}\nlocal_tms = {}\nerrors = {}"))"#)
}

#[test]
fn elab_assignment_pass3() {
    insta::assert_snapshot!(format!("{:?}", elab_test(
        r"a = 1; b = a; c = 'hello'; d = { f1 = a, f2 = c }"
    )), @r#"Ok(("", "global = {Type: 0, Any: 1, Bool: 2, Num: 3, Str: 4, Effect: 5, binary_plus: 6, binary_times: 7, binary_minus: 8, binary_division: 9, binary_modulo: 10, binary_exponent: 11, binary_equal: 12, binary_not_equal: 13, binary_less_than: 14, binary_greater_than: 16, binary_less_than_or_equal: 17, binary_greater_than_or_equal: 18, binary_and: 19, binary_or: 20, unary_minus: 21, unary_reverse_complement: 22, unary_read_reverse_complement: 23, unary_not: 24, read_ty: 25, len: 26, slice: 27, tag: 28, translate: 29, str_concat: 30, concat: 31, csv_ty: 32, csv: 33, tsv_ty: 34, tsv: 35, fasta: 36, find_first: 37, find_last: 38, lookup: 39, to_upper: 40, to_lower: 41, describe: 42, count_matches: 43, contains: 44, distance: 45, to_str: 46, to_num: 47, stdout!: 48, count!: 49, out!: 50, mean!: 51, min: 52, max: 53, mean: 54, to_qscores: 55, paired: 56, mapped_in_proper_pair: 57, unmapped: 58, mate_unmapped: 59, reverse_strand: 60, mate_reverse_strand: 61, first_in_pair: 62, second_in_pair: 63, not_primary_alignment: 64, fails_platform_quality_checks: 65, pcr_or_optical_duplicate: 66, supplementary_alignment: 67, round: 68, floor: 69, ceil: 70, a: 71, b: 72, c: 73, d: 74}\nglobal_tys = {0: Type, 1: Type, 2: Type, 3: Type, 4: Type, 5: Type, 6: (Num, Num) -> Num, 7: (Num, Num) -> Num, 8: (Num, Num) -> Num, 9: (Num, Num) -> Num, 10: (Num, Num) -> Num, 11: (Num, Num) -> Num, 12: (Any, Any) -> Bool, 13: (Any, Any) -> Bool, 14: (Num, Num) -> Bool, 15: (Num, Num) -> Bool, 16: (Num, Num) -> Bool, 17: (Num, Num) -> Bool, 18: (Num, Num) -> Bool, 19: (Bool, Bool) -> Bool, 20: (Bool, Bool) -> Bool, 21: (Num) -> Num, 22: (Str) -> Str, 23: ({ seq = Str .. }) -> { seq = Str .. }, 24: (Bool) -> Bool, 25: (Str) -> Type, 26: (Str) -> Num, 27: (Str, Num, Num) -> Str, 28: ({ desc = Str .. }, Str, prefix: Str = ' ') -> { desc = Str .. }, 29: (Str, stop_codon: Str = '-', illegal_codon: Str = '?') -> Str, 30: (Str, Str) -> Str, 31: ({ seq = Str .. }, { seq = Str .. }) -> { seq = Str .. }, 32: (Str) -> Type, 33: (Str) -> #awaiting([(#cached[32])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 34: (Str) -> Type, 35: (Str) -> #awaiting([(#cached[34])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 36: (Str) -> [{ desc = Str, id = Str, seq = Str }], 37: (Str, Str) -> Num, 38: (Str, Str) -> Num, 39: (Str, Str, kv_sep: Str = '=', sep: Str = ' ') -> Str, 40: (Str) -> Str, 41: (Str) -> Str, 42: ({ seq = Str .. }, {  .. }, reverse_complement: Bool = false, error_rate: Num = 0) -> Str, 43: ({ seq = Str .. }, Str, error_rate: Num = 0) -> Str, 44: ([Any], Any) -> Bool, 45: (Str, Str) -> Num, 46: (Any) -> Str, 47: (Str) -> Num, 48: (Any) -> Effect, 49: (Any, name: Str = 'count') -> Effect, 50: (Any, Str) -> Effect, 51: (Num, name: Str = 'mean') -> Effect, 52: ([Num]) -> Num, 53: ([Num]) -> Num, 54: ([Num]) -> Num, 55: (Str) -> [Num], 56: (Num) -> Bool, 57: (Num) -> Bool, 58: (Num) -> Bool, 59: (Num) -> Bool, 60: (Num) -> Bool, 61: (Num) -> Bool, 62: (Num) -> Bool, 63: (Num) -> Bool, 64: (Num) -> Bool, 65: (Num) -> Bool, 66: (Num) -> Bool, 67: (Num) -> Bool, 68: (Num) -> Num, 69: (Num) -> Num, 70: (Num) -> Num, 71: Num, 72: Num, 73: Str, 74: { f1 = Num, f2 = Str }}\nglobal_tms = {0: Type, 1: Any, 2: Bool, 3: Num, 4: Str, 5: Effect, 6: #fun-foreign, 7: #fun-foreign, 8: #fun-foreign, 9: #fun-foreign, 10: #fun-foreign, 11: #fun-foreign, 12: #fun-foreign, 13: #fun-foreign, 14: #fun-foreign, 15: #fun-foreign, 16: #fun-foreign, 17: #fun-foreign, 18: #fun-foreign, 19: #fun-foreign, 20: #fun-foreign, 21: #fun-foreign, 22: #fun-foreign, 23: #fun-foreign, 24: #fun-foreign, 25: #fun-foreign, 26: #fun-foreign, 27: #fun-foreign, 28: #fun-foreign, 29: #fun-foreign, 30: #fun-foreign, 31: #fun-foreign, 32: #fun-foreign, 33: #fun-foreign, 34: #fun-foreign, 35: #fun-foreign, 36: #fun-foreign, 37: #fun-foreign, 38: #fun-foreign, 39: #fun-foreign, 40: #fun-foreign, 41: #fun-foreign, 42: #fun-foreign, 43: #fun-foreign, 44: #fun-foreign, 45: #fun-foreign, 46: #fun-foreign, 47: #fun-foreign, 48: #fun-foreign, 49: #fun-foreign, 50: #fun-foreign, 51: #fun-foreign, 52: #fun-foreign, 53: #fun-foreign, 54: #fun-foreign, 55: #fun-foreign, 56: #fun-foreign, 57: #fun-foreign, 58: #fun-foreign, 59: #fun-foreign, 60: #fun-foreign, 61: #fun-foreign, 62: #fun-foreign, 63: #fun-foreign, 64: #fun-foreign, 65: #fun-foreign, 66: #fun-foreign, 67: #fun-foreign, 68: #fun-foreign, 69: #fun-foreign, 70: #fun-foreign, 71: 1, 72: 1, 73: hello, 74: { f1 = 1, f2 = hello }}\nlocal = {}\nlocal_tys = {}\nlocal_tms = {}\nerrors = {}"))"#)
}

#[test]
fn elab_assignment_pass4() {
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1; b = a + 10 * 10")), @r#"Ok(("", "global = {Type: 0, Any: 1, Bool: 2, Num: 3, Str: 4, Effect: 5, binary_plus: 6, binary_times: 7, binary_minus: 8, binary_division: 9, binary_modulo: 10, binary_exponent: 11, binary_equal: 12, binary_not_equal: 13, binary_less_than: 14, binary_greater_than: 16, binary_less_than_or_equal: 17, binary_greater_than_or_equal: 18, binary_and: 19, binary_or: 20, unary_minus: 21, unary_reverse_complement: 22, unary_read_reverse_complement: 23, unary_not: 24, read_ty: 25, len: 26, slice: 27, tag: 28, translate: 29, str_concat: 30, concat: 31, csv_ty: 32, csv: 33, tsv_ty: 34, tsv: 35, fasta: 36, find_first: 37, find_last: 38, lookup: 39, to_upper: 40, to_lower: 41, describe: 42, count_matches: 43, contains: 44, distance: 45, to_str: 46, to_num: 47, stdout!: 48, count!: 49, out!: 50, mean!: 51, min: 52, max: 53, mean: 54, to_qscores: 55, paired: 56, mapped_in_proper_pair: 57, unmapped: 58, mate_unmapped: 59, reverse_strand: 60, mate_reverse_strand: 61, first_in_pair: 62, second_in_pair: 63, not_primary_alignment: 64, fails_platform_quality_checks: 65, pcr_or_optical_duplicate: 66, supplementary_alignment: 67, round: 68, floor: 69, ceil: 70, a: 71, b: 72}\nglobal_tys = {0: Type, 1: Type, 2: Type, 3: Type, 4: Type, 5: Type, 6: (Num, Num) -> Num, 7: (Num, Num) -> Num, 8: (Num, Num) -> Num, 9: (Num, Num) -> Num, 10: (Num, Num) -> Num, 11: (Num, Num) -> Num, 12: (Any, Any) -> Bool, 13: (Any, Any) -> Bool, 14: (Num, Num) -> Bool, 15: (Num, Num) -> Bool, 16: (Num, Num) -> Bool, 17: (Num, Num) -> Bool, 18: (Num, Num) -> Bool, 19: (Bool, Bool) -> Bool, 20: (Bool, Bool) -> Bool, 21: (Num) -> Num, 22: (Str) -> Str, 23: ({ seq = Str .. }) -> { seq = Str .. }, 24: (Bool) -> Bool, 25: (Str) -> Type, 26: (Str) -> Num, 27: (Str, Num, Num) -> Str, 28: ({ desc = Str .. }, Str, prefix: Str = ' ') -> { desc = Str .. }, 29: (Str, stop_codon: Str = '-', illegal_codon: Str = '?') -> Str, 30: (Str, Str) -> Str, 31: ({ seq = Str .. }, { seq = Str .. }) -> { seq = Str .. }, 32: (Str) -> Type, 33: (Str) -> #awaiting([(#cached[32])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 34: (Str) -> Type, 35: (Str) -> #awaiting([(#cached[34])(#[0])] :: [Type, Any, Bool, Num, Str, Effect, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign, #fun-foreign]), 36: (Str) -> [{ desc = Str, id = Str, seq = Str }], 37: (Str, Str) -> Num, 38: (Str, Str) -> Num, 39: (Str, Str, kv_sep: Str = '=', sep: Str = ' ') -> Str, 40: (Str) -> Str, 41: (Str) -> Str, 42: ({ seq = Str .. }, {  .. }, reverse_complement: Bool = false, error_rate: Num = 0) -> Str, 43: ({ seq = Str .. }, Str, error_rate: Num = 0) -> Str, 44: ([Any], Any) -> Bool, 45: (Str, Str) -> Num, 46: (Any) -> Str, 47: (Str) -> Num, 48: (Any) -> Effect, 49: (Any, name: Str = 'count') -> Effect, 50: (Any, Str) -> Effect, 51: (Num, name: Str = 'mean') -> Effect, 52: ([Num]) -> Num, 53: ([Num]) -> Num, 54: ([Num]) -> Num, 55: (Str) -> [Num], 56: (Num) -> Bool, 57: (Num) -> Bool, 58: (Num) -> Bool, 59: (Num) -> Bool, 60: (Num) -> Bool, 61: (Num) -> Bool, 62: (Num) -> Bool, 63: (Num) -> Bool, 64: (Num) -> Bool, 65: (Num) -> Bool, 66: (Num) -> Bool, 67: (Num) -> Bool, 68: (Num) -> Num, 69: (Num) -> Num, 70: (Num) -> Num, 71: Num, 72: Num}\nglobal_tms = {0: Type, 1: Any, 2: Bool, 3: Num, 4: Str, 5: Effect, 6: #fun-foreign, 7: #fun-foreign, 8: #fun-foreign, 9: #fun-foreign, 10: #fun-foreign, 11: #fun-foreign, 12: #fun-foreign, 13: #fun-foreign, 14: #fun-foreign, 15: #fun-foreign, 16: #fun-foreign, 17: #fun-foreign, 18: #fun-foreign, 19: #fun-foreign, 20: #fun-foreign, 21: #fun-foreign, 22: #fun-foreign, 23: #fun-foreign, 24: #fun-foreign, 25: #fun-foreign, 26: #fun-foreign, 27: #fun-foreign, 28: #fun-foreign, 29: #fun-foreign, 30: #fun-foreign, 31: #fun-foreign, 32: #fun-foreign, 33: #fun-foreign, 34: #fun-foreign, 35: #fun-foreign, 36: #fun-foreign, 37: #fun-foreign, 38: #fun-foreign, 39: #fun-foreign, 40: #fun-foreign, 41: #fun-foreign, 42: #fun-foreign, 43: #fun-foreign, 44: #fun-foreign, 45: #fun-foreign, 46: #fun-foreign, 47: #fun-foreign, 48: #fun-foreign, 49: #fun-foreign, 50: #fun-foreign, 51: #fun-foreign, 52: #fun-foreign, 53: #fun-foreign, 54: #fun-foreign, 55: #fun-foreign, 56: #fun-foreign, 57: #fun-foreign, 58: #fun-foreign, 59: #fun-foreign, 60: #fun-foreign, 61: #fun-foreign, 62: #fun-foreign, 63: #fun-foreign, 64: #fun-foreign, 65: #fun-foreign, 66: #fun-foreign, 67: #fun-foreign, 68: #fun-foreign, 69: #fun-foreign, 70: #fun-foreign, 71: 1, 72: 101}\nlocal = {}\nlocal_tys = {}\nlocal_tms = {}\nerrors = {}"))"#)
}

#[test]
fn eval_read_name_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"read.id |> stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(read1)")"#)
}

#[test]
fn eval_assignment_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"a = 'hello'; b = 10; c = '{a} and {b}'; c |> stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(hello and 10)")"#)
}

#[test]
fn eval_pattern_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read matches [_ GGGG rest:_] => {rest.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(CCCCCCCCCCCC)")"#)
}

#[test]
fn eval_pattern_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read matches [_ GGGG rest:|3| _] => {rest.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(CCC)")"#)
}

#[test]
fn eval_pattern_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read matches [first:|3| _] => {first.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(AAA)")"#)
}

#[test]
fn eval_pattern_pass4() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read matches [_ last:|3|] => {last.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(CCC)")"#)
}

#[test]
fn eval_rec_lit_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.id }; 'hello'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(hello)")"#)
}

#[test]
fn eval_rec_lit_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.id }; rec.a.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(read1)")"#)
}
#[test]
fn eval_rec_lit_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.seq.len() }; if rec.a > 10 { 'long'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(long)")"#)
}

#[test]
fn eval_fastq_trim_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fastq_read_test(r"if read matches [start:|10| _] => start.out!('trimmed.fq')", b"AAAAAAAAAGGGGCCCCCCCCCCCC", b"!@#$%^&*&^%$#@#$%^&*^%^&*")), @r#"Ok("out!({ desc = , id = read1, qual = !@#$%^&*&^, seq = AAAAAAAAAG }, name = trimmed.fq)")"#)
}
#[test]
fn eval_fastq_trim_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fastq_read_test(r"if read matches [start:|10| _] => (-start).out!('trimmed.fq')", b"AAAAAAAAAGGGGCCCCCCCCCCCC", b"!@#$%^&*&^%$#@#$%^&*^%^&*")), @r#"Ok("out!({ desc = , id = read1, qual = ^&*&^%$#@!, seq = CTTTTTTTTT }, name = trimmed.fq)")"#)
}

#[test]
fn eval_opt_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 7) => n; if f() > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(greater)")"#)
}
#[test]
fn eval_opt_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 7) => n; if f(n = 1) > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("")"#)
}
#[test]
fn eval_opt_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f() > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("")"#)
}
#[test]
fn eval_opt_fail1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f(m = 1) > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err([GenericError { location: Some(Location { start: 26, end: 34 }), message: "found optional argument named 'm'; only expected optional arguments { n: Num }" }])"#)
}
#[test]
fn eval_opt_fail2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f(n = true) > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err([GenericError { location: Some(Location { start: 32, end: 36 }), message: "mismatched types: expected Num, found Bool" }])"#)
}
#[test]
fn eval_opt_fail3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = true) => n; if f() > 5 { 'greater'.stdout!() }", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err([GenericError { location: Some(Location { start: 14, end: 18 }), message: "mismatched types: expected Num, found Bool" }])"#)
}

#[test]
fn eval_stmt_after_conditional_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"
        if read matches [_] => 'a' |> stdout!()
        'b' |> stdout!()
        ", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("stdout!(a),stdout!(b)")"#)
}
