use core::{eval, eval_prog, make_portable};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::{Error, SimpleFile},
    term::{self, termcolor::StandardStream},
};
use parse::parse;
use read::{read_fa_new, Reader};
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
    let code = r"
        b = 10

        if read is [_ AAA rest:_] => {rest.seq |> 'found'}
    ";

    let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
    let config = codespan_reporting::term::Config::default();

    let prog = parse(&code, &GlobalConfig { error: 0.2 })
        .map_err(|e| {
            let file = SimpleFile::new("<code>", code);
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.start..e.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);
        })
        .unwrap();

    println!("{:?}", prog);
    let arena = Arena::new();
    let ctx = core::library::standard_library(&arena, true);

    let cprog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| {
            let file = SimpleFile::new("<code>", code);
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.location.start..e.location.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);
        })
        .unwrap();

    read_fa_new("local/input_upper.fa", &cprog, &arena);
}

pub struct GlobalConfig {
    error: f32,
}
