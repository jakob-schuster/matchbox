use std::collections::HashMap;

use crate::{
    core::{
        self,
        rec::{ConcreteRec, FastaRead},
        Val,
    },
    parse::parse,
    surface::elab_prog,
    util::{Arena, Located},
    GlobalConfig,
};

/// Parse some given code,
/// printing the surface AST as a string.
#[cfg(test)]
fn parse_test(code: &str) -> String {
    use crate::GenericError;

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(&global_config))
        // should never unwrap, because program terminates
        .unwrap();

    format!("{:?}", prog)
}

/// Elaborate some given code,
/// printing the resulting core AST as a string.
#[cfg(test)]
fn elab_test(code: &str) -> String {
    use crate::GenericError;

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(&global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = core::library::standard_library(&arena, true);

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(&global_config))
        // should never unwrap, because program terminates
        .unwrap();

    core_prog.to_string()
}

/// Execute some given code on one given sequence,
/// printing the resulting effects as a string.
#[cfg(test)]
fn eval_one_fasta_read_test(code: &str, seq: &[u8]) -> String {
    use crate::GenericError;

    let global_config = GlobalConfig::default();

    let prog = parse(code, &global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(&global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = core::library::standard_library(&arena, true);

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(&global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // create a toy read
    let read = core::Val::Rec(
        arena.alloc(ConcreteRec {
            map: [
                (
                    b"seq" as &[u8],
                    arena.alloc(core::Val::Str {
                        s: arena.alloc(seq.to_vec()),
                    }) as &Val,
                ),
                (
                    b"id" as &[u8],
                    arena.alloc(core::Val::Str {
                        s: arena.alloc(b"read1".to_vec()),
                    }) as &Val,
                ),
                (
                    b"desc" as &[u8],
                    arena.alloc(core::Val::Str {
                        s: arena.alloc(b"".to_vec()),
                    }) as &Val,
                ),
            ]
            .into_iter()
            .collect(),
        }),
    );

    // evaluate the program
    core_prog
        .eval(&arena, &ctx.tms, &read)
        .unwrap()
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn parse_assignment() {
    insta::assert_snapshot!(parse_test(r#"a = 1"#), @r#"Located { location: Location { start: 0, end: 5 }, data: ProgData { stmts: [Located { location: Location { start: 0, end: 5 }, data: Let { name: "a", tm: Located { location: Location { start: 4, end: 5 }, data: NumLit { n: Int(1) } } } }] } }"#)
}

#[test]
fn parse_multiple_assignment() {
    insta::assert_snapshot!(parse_test(r#"
        a = 1
        b = 2; c = 'hello'
    "#), @r#"Located { location: Location { start: 0, end: 46 }, data: ProgData { stmts: [Located { location: Location { start: 9, end: 14 }, data: Let { name: "a", tm: Located { location: Location { start: 13, end: 14 }, data: NumLit { n: Int(1) } } } }, Located { location: Location { start: 23, end: 28 }, data: Let { name: "b", tm: Located { location: Location { start: 27, end: 28 }, data: NumLit { n: Int(2) } } } }, Located { location: Location { start: 30, end: 41 }, data: Let { name: "c", tm: Located { location: Location { start: 34, end: 41 }, data: StrLit { regs: [Located { location: Location { start: 35, end: 40 }, data: Str { s: [104, 101, 108, 108, 111] } }] } } } }] } }"#)
}

#[test]
fn elab_assignment_pass1() {
    insta::assert_snapshot!(elab_test(r"a = 1"), @"push 1;")
}

#[test]
fn elab_assignment_pass2() {
    insta::assert_snapshot!(elab_test(r"a = 1; b = a"), @"push 1; push #[0];")
}

#[test]
fn elab_assignment_pass3() {
    insta::assert_snapshot!(elab_test(
        r"a = 1; b = a; c = 'hello'; d = { f1 = a, f2 = c }"
    ), @"push 1; push #[0]; push 'hello'; push { f1 = #[2], f2 = #[0] };")
}

#[test]
fn elab_assignment_pass4() {
    insta::assert_snapshot!(elab_test(r"a = 1; b = a + 10 * 10"), @"push 1; push (#[42])(#[0], (#[41])(10, 10));")
}

#[test]
fn eval_read_name_pass1() {
    insta::assert_snapshot!(eval_one_fasta_read_test(r"read.id |> stdout()", b"AAAAAAAAAGGGGCCCCCCCCCCCC"), @"read1 |> { output = stats }")
}

#[test]
fn eval_pattern_pass1() {
    insta::assert_snapshot!(eval_one_fasta_read_test(r"if read is [_ GGGG rest:_] => {rest.seq |> stdout()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC"), @"CCCCCCCCCCCC |> { output = stats }")
}

#[test]
fn eval_pattern_pass2() {
    insta::assert_snapshot!(eval_one_fasta_read_test(r"if read is [_ GGGG rest:|3| _] => {rest.seq |> stdout()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC"), @"CCC |> { output = stats }")
}

#[test]
fn eval_pattern_pass3() {
    insta::assert_snapshot!(eval_one_fasta_read_test(r"if read is [first:|3| _] => {first.seq |> stdout()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC"), @"AAA |> { output = stats }")
}

#[test]
fn eval_pattern_pass4() {
    insta::assert_snapshot!(eval_one_fasta_read_test(r"if read is [_ last:|3|] => {last.seq |> stdout()}", b"AAAAAAAAAGGGGCCCCCCCCCCCC"), @"CCC |> { output = stats }")
}
