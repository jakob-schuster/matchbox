use std::collections::HashMap;

use crate::parse::parse;
use crate::{
    core::{self, rec::ConcreteRec, Val},
    surface::elab_prog,
    util::Arena,
    GenericError, GlobalConfig,
};

/// Parse some given code,
/// printing the surface AST as a string.
#[cfg(test)]
fn parse_test(code: &str) -> Result<String, GenericError> {
    use crate::GenericError;

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    Ok(format!("{:?}", prog))
}

/// Elaborate some given code,
/// printing the resulting core AST as a string.
#[cfg(test)]
fn elab_test(code: &str) -> Result<String, GenericError> {
    use crate::{
        core::library::standard_library, read_code_from_script, surface::elab_prog_for_ctx,
        GenericError,
    };

    let global_config = GlobalConfig::default();

    // should never unwrap, because program terminates
    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = standard_library(&arena);

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, &global_config).map_err(GenericError::from)?;

    // elaborate to generate the context
    let ctx =
        elab_prog_for_ctx(&arena, &ctx, arena.alloc(library_prog)).map_err(GenericError::from)?;

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx, &prog).map_err(GenericError::from)?;

    Ok(core_prog.to_string())
}

/// Execute some given code on one given sequence,
/// printing the resulting effects as a string.
#[cfg(test)]
fn eval_one_fasta_read_test(code: &str, seq: &[u8]) -> Result<String, GenericError> {
    use std::sync::Arc;

    use crate::{
        core::{library::standard_library, rec::FullyConcreteRec},
        read::FileType,
        read_code_from_script,
        surface::elab_prog_for_ctx,
        GenericError,
    };

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = standard_library(&arena);

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, &global_config).map_err(GenericError::from)?;

    // elaborate to generate the context
    let ctx =
        elab_prog_for_ctx(&arena, &ctx, arena.alloc(library_prog)).map_err(GenericError::from)?;

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx.bind_read(&arena, &FileType::Fasta), &prog)
        .map_err(GenericError::from)?;
    let (core_prog, cache) =
        core_prog.cache(&arena, &ctx.bind_read(&arena, &FileType::Fasta).tms)?;

    // create a toy read
    let read = core::Val::Rec {
        rec: Arc::new(FullyConcreteRec {
            map: [
                (
                    b"seq".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(seq.to_vec()),
                    } as Val,
                ),
                (
                    b"id".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(b"read1".to_vec()),
                    } as Val,
                ),
                (
                    b"desc".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(b"".to_vec()),
                    } as Val,
                ),
            ]
            .into_iter()
            .collect(),
        }),
    };

    // evaluate the program
    Ok(core_prog
        .eval(&arena, &ctx.tms, &cache, read)
        .unwrap()
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(","))
}
/// Execute some given code on one given sequence,
/// printing the resulting effects as a string.
#[cfg(test)]
fn eval_one_fastq_read_test(code: &str, seq: &[u8], qual: &[u8]) -> Result<String, GenericError> {
    use std::sync::Arc;

    use crate::{
        core::{library::standard_library, rec::FullyConcreteRec},
        read::FileType,
        read_code_from_script,
        surface::elab_prog_for_ctx,
        GenericError,
    };

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config).map_err(GenericError::from)?;

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = standard_library(&arena);

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, &global_config).map_err(GenericError::from)?;

    // elaborate to generate the context
    let ctx =
        elab_prog_for_ctx(&arena, &ctx, arena.alloc(library_prog)).map_err(GenericError::from)?;

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx.bind_read(&arena, &FileType::Fasta), &prog)
        .map_err(GenericError::from)?;
    let (core_prog, cache) =
        core_prog.cache(&arena, &ctx.bind_read(&arena, &FileType::Fasta).tms)?;

    // create a toy read
    let read = core::Val::Rec {
        rec: Arc::new(FullyConcreteRec {
            map: [
                (
                    b"seq".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(seq.to_vec()),
                    } as Val,
                ),
                (
                    b"id".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(b"read1".to_vec()),
                    } as Val,
                ),
                (
                    b"desc".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(b"".to_vec()),
                    } as Val,
                ),
                (
                    b"qual".to_vec(),
                    core::Val::Str {
                        s: arena.alloc(qual.to_vec()),
                    } as Val,
                ),
            ]
            .into_iter()
            .collect(),
        }),
    };

    // evaluate the program
    Ok(core_prog
        .eval(&arena, &ctx.tms, &cache, read)
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
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1")), @r#"Ok("push 1; ")"#)
}

#[test]
fn elab_assignment_pass2() {
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1; b = a")), @r#"Ok("push 1; push #[0]; ")"#)
}

#[test]
fn elab_assignment_pass3() {
    insta::assert_snapshot!(format!("{:?}", elab_test(
        r"a = 1; b = a; c = 'hello'; d = { f1 = a, f2 = c }"
    )), @r#"Ok("push 1; push #[0]; push 'hello'; push { f1 = #[2], f2 = #[0] }; ")"#)
}

#[test]
fn elab_assignment_pass4() {
    insta::assert_snapshot!(format!("{:?}", elab_test(r"a = 1; b = a + 10 * 10")), @r#"Ok("push 1; push (#[42])(#[0], (#[41])(10, 10)); ")"#)
}

#[test]
fn eval_read_name_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"read.id |> stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("read1 |> { output = stdout }")"#)
}

#[test]
fn eval_assignment_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"a = 'hello'; b = 10; c = '{a} and {b}'; c |> stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("hello and 10 |> { output = stdout }")"#)
}

#[test]
fn eval_pattern_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read is [_ GGGG rest:_] => {rest.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("CCCCCCCCCCCC |> { output = stdout }")"#)
}

#[test]
fn eval_pattern_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read is [_ GGGG rest:|3| _] => {rest.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("CCC |> { output = stdout }")"#)
}

#[test]
fn eval_pattern_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read is [first:|3| _] => {first.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("AAA |> { output = stdout }")"#)
}

#[test]
fn eval_pattern_pass4() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"if read is [_ last:|3|] => {last.seq |> stdout!()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("CCC |> { output = stdout }")"#)
}

#[test]
fn eval_rec_lit_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.id }; 'hello'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("hello |> { output = stdout }")"#)
}

#[test]
fn eval_rec_lit_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.id }; rec.a.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("read1 |> { output = stdout }")"#)
}
#[test]
fn eval_rec_lit_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"rec = { a = read.seq.len() }; if rec.a > 10 => 'long'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("long |> { output = stdout }")"#)
}

#[test]
fn eval_fastq_trim_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fastq_read_test(r"if read is [start:|10| _] => start.out!('trimmed.fq')", b"AAAAAAAAAGGGGCCCCCCCCCCCC", b"!@#$%^&*&^%$#@#$%^&*^%^&*")), @r#"Ok("{ desc = , id = read1, qual = !@#$%^&*&^, seq = AAAAAAAAAG } |> { filename = trimmed.fq, output = file }")"#)
}
#[test]
fn eval_fastq_trim_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fastq_read_test(r"if read is [start:|10| _] => (-start).out!('trimmed.fq')", b"AAAAAAAAAGGGGCCCCCCCCCCCC", b"!@#$%^&*&^%$#@#$%^&*^%^&*")), @r#"Ok("{ desc = , id = read1, qual = ^&*&^%$#@!, seq = CTTTTTTTTT } |> { filename = trimmed.fq, output = file }")"#)
}

#[test]
fn eval_opt_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 7) => n; if f() > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("greater |> { output = stdout }")"#)
}
#[test]
fn eval_opt_pass2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 7) => n; if f(n = 1) > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("")"#)
}
#[test]
fn eval_opt_pass3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f() > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("")"#)
}
#[test]
fn eval_opt_fail1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f(m = 1) > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err(GenericError { location: Some(Location { start: 26, end: 34 }), message: "found optional argument named 'm'; only expected optional arguments { n: Num }" })"#)
}
#[test]
fn eval_opt_fail2() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = 3) => n; if f(n = true) > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err(GenericError { location: Some(Location { start: 32, end: 36 }), message: "mismatched types: expected Num, found Bool" })"#)
}
#[test]
fn eval_opt_fail3() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"f = (n: Num = true) => n; if f() > 5 => 'greater'.stdout!()", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Err(GenericError { location: Some(Location { start: 14, end: 18 }), message: "mismatched types: expected Num, found Bool" })"#)
}

#[test]
fn eval_stmt_after_conditional_pass1() {
    insta::assert_snapshot!(format!("{:?}", eval_one_fasta_read_test(r"
        if read is [_] => 'a' |> stdout!()
        'b' |> stdout!()
        ", b"AAAAAAAAAGGGGCCCCCCCCCCCC")), @r#"Ok("a |> { output = stdout },b |> { output = stdout }")"#)
}
