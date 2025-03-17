use core::{eval, eval_prog, make_portable};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::{Error, SimpleFile},
    term::{self, termcolor::StandardStream},
};
use parse::parse;
use read::Reader;
use surface::elab_prog;
use util::{Arena, Env};

mod core;
mod myers;
mod parse;
mod read;
mod surface;
mod util;
mod visit;

use surface::Context;

fn main() {
    let header = r"
        binary_plus = #(n0: Num, n1: Num): Num => binary_plus
        binary_times = #(n0: Num, n1: Num): Num => binary_times
        binary_division = #(n0: Num, n1: Num): Num => binary_division
        binary_modulo = #(n0: Num, n1: Num): Num => binary_modulo
        binary_exponent = #(n0: Num, n1: Num): Num => binary_exponent
        binary_minus = #(n0: Num, n1: Num): Num => binary_minus

        binary_equal = #(a: Any, b: Any): Bool => binary_equal
        binary_not_equal = #(a: Any, b: Any): Bool => binary_not_equal

        binary_less_than = #(n0: Num, n1: Num): Bool => binary_less_than
        binary_greater_than = #(n0: Num, n1: Num): Bool => binary_greater_than
        binary_less_than_or_equal = #(n0: Num, n1: Num): Bool => binary_less_than_or_equal
        binary_greater_than_or_equal = #(n0: Num, n1: Num): Bool => binary_greater_than_or_equal

        binary_and = #(a: Bool, b: Bool): Bool => binary_and
        binary_or = #(a: Bool, b: Bool): Bool => binary_or

        unary_minus = #(n: Num): Num => unary_minus
        unary_reverse_complement = #(s: Str): Str => unary_reverse_complement
    ";

    let code = r"
        read = { seq = ACGATGCTTTTTTTTATGTCGA }

        b = 100

        if read is [before:_ TTTTTTTT after:_] => {after.seq |> before.seq}

    ";

    let appended = format!(
        "{header}
        {code}"
    );

    let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
    let config = codespan_reporting::term::Config::default();

    let prog = parse(&appended, &GlobalConfig { error: 0.2 })
        .map_err(|e| {
            let file = SimpleFile::new("<code>", appended.clone());
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.start..e.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);
        })
        .unwrap();

    let arena = Arena::new();
    let ctx = Context::standard_library(&arena);

    let cprog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| {
            let file = SimpleFile::new("<code>", appended.clone());
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.location.start..e.location.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);
        })
        .unwrap();

    let reader = Reader::new("input.fa", 10000, None).unwrap();

    reader.map_single_threaded(
        |val| {
            let quick_arena = Arena::new();
            // let rec = core::Val::Rec(quick_arena.alloc(val));
            let a = cprog.eval(quick_arena.alloc(val), &quick_arena);

            // make_portable(&quick_arena, quick_arena.alloc(val))
        },
        |effects, _| todo!(),
        &mut (),
    );

    // println!("{}", cprog);
    // let effects = eval_prog(&arena, &ctx.tms, arena.alloc(cprog))
    //     .map_err(|e| {
    //         let file = SimpleFile::new("<code>", appended.clone());
    //         let diagnostic = Diagnostic::error()
    //             .with_message(e.clone().message)
    //             .with_labels(vec![Label::primary((), e.location.start..e.location.end)])
    //             .with_notes(vec![]);

    //         term::emit(&mut writer, &config, &file, &diagnostic);
    //     })
    //     .unwrap();

    // println!(
    //     "{}",
    //     effects
    //         .iter()
    //         .map(|a| a.to_string())
    //         .collect::<Vec<_>>()
    //         .join(", ")
    // )
}

pub struct GlobalConfig {
    error: f32,
}
