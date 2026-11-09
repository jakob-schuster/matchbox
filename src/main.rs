//! The compiler for the *matchbox* language, a bioinformatics DSL for processing sequencing reads.
//!
//! The flow of execution in *matchbox* is:
//! - Arguments are parsed from the command line.
//! - The *matchbox* script (provided as either a file `-s` or as a direct argument `-r`) is parsed into a surface-level AST (`parse.rs`).
//! - The type of the input read value, `read`, is inferred from either the `-f` option or the file extension of the reads file (`input.rs`).
//! - The surface-level AST is elaborated into a core AST (`surface.rs`). Bidirectional type checking is performed; where types are given, they are checked, and where they are not given, they are inferred.
//! - The input reads are processed.
//!   - This involves reading in a chunk of reads, then evaluating the program in parallel across the chunk of reads (`input.rs`).
//!   - "Evaluating the program" involves creating an environment which includes the current read and evaluating the core AST in the context of this environment, which results in a vector of output effects (e.g. incrementing a global count, appending to a file, writing to stdout).
//!   - The output effects for the chunk of reads are applied (`output.rs`).

use core::library::standard_library;
use std::{fmt::Debug, fs::File, io::Read, process::exit};

use clap::Parser;
use codespan_reporting::{
    diagnostic::Label,
    files::SimpleFile,
    term::{self, termcolor::StandardStream},
};
use input::{get_extensions, FileType, FileTypeError, InputError, ReaderWithBar};
use output::OutputHandler;
use parse::parse;
use util::location::Location;

mod core;
mod generic_error;
mod global_config;
mod input;
mod myers;
mod output;
mod parse;
mod surface;
mod test;
mod ui;
mod util;

fn main() {
    // parse in the command line config
    let global_config = global_config::GlobalConfig::parse();

    // execute the matchbox script
    run_script(&global_config)
}

/// Given the global config,
/// load the matchbox script and run matchbox on input reads.
/// Panic on evaluation errors or internal errors.
fn run_script(global_config: &global_config::GlobalConfig) {
    let code = if let Some(code) = &global_config.input_code.run {
        code.to_string()
    } else if let Some(filename) = &global_config.input_code.script_file {
        read_code_from_script(filename)
            .map_err(|e| {
                generic_error::GenericError::from(e).codespan_print_and_exit(global_config)
            })
            // should never unwrap, because program terminates
            .unwrap()
    } else {
        // can't get here
        panic!()
    };

    run(&code, global_config)
}

/// Given matchbox code as a string, and the global config,
/// run matchbox on input reads.
/// Panic on evaluation errors or internal errors.
fn run(code: &str, global_config: &global_config::GlobalConfig) {
    // set up the thread pool
    if !global_config.input_reads.debug_single_threaded_run {
        rayon::ThreadPoolBuilder::new()
            .num_threads(global_config.threads)
            .build_global()
            .unwrap();
    }

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let c0 = standard_library();

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, global_config)
        .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // elaborate to generate the context
    let (_, c1) = library_prog.elab(&c0);

    // parse the args program
    let args_prog = parse(&format!("args = {{{}}}", global_config.args), global_config)
        .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
        .unwrap();

    // elaborate to cache the args value into the context
    let (_, c2) = args_prog.elab(&c1);

    // parse the program
    let prog = parse(code, global_config)
        .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // load in the input reads
    let mut reader_with_bar = ReaderWithBar::new(&global_config.input_reads)
        .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
        .unwrap();

    // elaborate to a core program
    let (core_prog, c3) =
        prog.elab(&c2.bind_read_from_reader(reader_with_bar.get_ty().concretise()));

    // drop the read parameter from the context
    let c4 = c2.with_global_from(c3);

    // if we are printing compilation debug info, print it now
    if global_config.input_reads.debug_compilation {
        eprintln!("{}", core_prog);
    }

    // at this point, check for elab errors
    if !c4.get_errors().is_empty() {
        for error in c4.get_errors() {
            // WARN currently errors in standard library or arg struct will be printed wrong
            generic_error::GenericError::from(error).codespan_print(global_config);
        }

        // terminate execution
        exit(1);
    }

    // create an output handler, to receive output effects
    let mut output_handler = OutputHandler::new(
        global_config.output_directory.clone(),
        reader_with_bar.get_aux_data(),
    )
    .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
    // should never unwrap
    .unwrap();

    let global = c4.get_global_env();

    // process the reads
    reader_with_bar
        .map(&core_prog, &global, &mut output_handler)
        .map_err(|e| generic_error::GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap
        .unwrap();
}

/// Given the name of a matchbox script,
/// opens the file and returns the code from inside, or an input error.
/// Returns errors when the filetype is not '.mb',
/// or when the file can't be opened.
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
