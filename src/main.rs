use core::{eval, make_portable};
use std::{fmt::Debug, fs::File, io::Read, process::exit};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::{Error, SimpleFile},
    term::{self, termcolor::StandardStream},
};
use output::OutputHandler;
use parse::parse;
use read::{
    get_extensions, get_filetype_and_buffer, read_any, read_fa_new, FileType, FileTypeError,
    InputError, Reader,
};
use surface::elab_prog;
use util::{Arena, Env};

mod core;
mod myers;
mod output;
mod parse;
mod read;
mod surface;
mod util;
mod visit;

use clap::Parser;
use surface::Context;

fn main() {
    let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
    let config = codespan_reporting::term::Config::default();

    let global_config = GlobalConfig::parse();

    rayon::ThreadPoolBuilder::new()
        .num_threads(global_config.threads)
        .build_global()
        .unwrap();

    let code = read_code_from_script(&global_config.script)
        .map_err(|e| {
            let file = SimpleFile::new(format!("<{}>", global_config.script), "");
            let diagnostic = Diagnostic::error().with_message(e.to_string());
            term::emit(&mut writer, &config, &file, &diagnostic);
            exit(1)
        })
        // should never unwrap, because program terminates
        .unwrap();

    let prog = parse(&code, &global_config)
        .map_err(|e| {
            let file = SimpleFile::new(format!("<{}>", global_config.script), code.clone());
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.start..e.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);
            exit(1)
        })
        // should never unwrap, because program terminates
        .unwrap();

    let arena = Arena::new();
    let ctx = core::library::standard_library(&arena, true);

    let cprog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| {
            let file = SimpleFile::new(format!("<{}>", global_config.script), code.clone());
            let diagnostic = Diagnostic::error()
                .with_message(e.clone().message)
                .with_labels(vec![Label::primary((), e.location.start..e.location.end)])
                .with_notes(vec![]);

            term::emit(&mut writer, &config, &file, &diagnostic);

            exit(1)
        })
        // should never unwrap, because program terminates
        .unwrap();

    // println!("{}", cprog);
    let mut output_handler = OutputHandler::new();

    let env = core::library::standard_library(&arena, false).tms;
    if let Some(reads_filename) = global_config.reads {
        read_any(&reads_filename, &cprog, &arena, &env, &mut output_handler);
    } else {
        panic!("can't handle stdin reads yet!")
    }
}

#[derive(Parser)]
pub struct GlobalConfig {
    #[arg(short, long, default_value_t = 0.2)]
    error: f32,

    #[arg(short, long, default_value_t = 1)]
    threads: usize,

    #[arg(short, long)]
    script: String,

    #[arg()]
    reads: Option<String>,

    #[arg(short, long)]
    paired_with: Option<String>,
}

fn read_code_from_script(script_filename: &str) -> Result<String, InputError> {
    let mut code = String::new();

    match &get_extensions(script_filename)[..] {
        [.., last] => match FileType::try_from(*last) {
            Ok(filetype) => match filetype {
                FileType::Matchbox => {}
                _ => {
                    return Err(InputError::FileTypeError(FileTypeError {
                        extension: last.to_string(),
                        expected: Some("mb".to_string()),
                    }))
                }
            },
            Err(_) => {
                return Err(InputError::FileTypeError(FileTypeError {
                    extension: last.to_string(),
                    expected: Some("mb".to_string()),
                }))
            }
        },
        [] => {
            return Err(InputError::FileTypeError(FileTypeError {
                extension: "".to_string(),
                expected: Some("mb".to_string()),
            }))
        }
    }

    File::open(script_filename)
        .map_err(|_| InputError::FileOpenError {
            filename: script_filename.to_string(),
        })?
        .read_to_string(&mut code)
        .unwrap();

    Ok(code)
}
