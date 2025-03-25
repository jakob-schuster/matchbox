use core::{make_portable, EvalError};
use std::{fmt::Debug, fs::File, io::Read, process::exit};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::{Error, SimpleFile},
    term::{self, termcolor::StandardStream},
};
use output::OutputHandler;
use parse::{parse, ParseError};
use read::{
    get_extensions, get_filetype_and_buffer, read_any, read_fa, FileType, FileTypeError, InputError,
};
use surface::{elab_prog, ElabError};
use util::{Arena, Env, Location};

mod core;
mod myers;
mod output;
mod parse;
mod read;
mod surface;
mod util;
mod visit;

use clap::Parser;

fn main() {
    // parse in the command line config
    let global_config = GlobalConfig::parse();

    // execute the matchbox script
    run_script(&global_config)
}

/// Given the global config,
/// load the matchbox script and run matchbox on input reads.
/// Panic on evaluation errors or internal errors.
fn run_script(global_config: &GlobalConfig) {
    let code = read_code_from_script(&global_config.script)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    run(&code, global_config)
}

/// Given matchbox code as a string, and the global config,
/// run matchbox on input reads.
/// Panic on evaluation errors or internal errors.
fn run(code: &str, global_config: &GlobalConfig) {
    // set up the thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(global_config.threads)
        .build_global()
        .unwrap();

    // parse the program
    let prog = parse(code, global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = core::library::standard_library(&arena, true);

    // elaborate to a core program
    let core_prog = elab_prog(&arena, &ctx, &prog)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // create an output handler, to receive output effects
    let mut output_handler = OutputHandler::new();

    // create the standard library
    let env = core::library::standard_library(&arena, false).tms;

    // process the reads
    if let Some(reads_filename) = &global_config.reads {
        read_any(reads_filename, &core_prog, &env, &mut output_handler);
    } else {
        panic!("can't handle stdin reads yet!")
    }
}

/// The global configuration options, accessible as command line parameters.
#[derive(Parser)]
pub struct GlobalConfig {
    /// Default error rate permitted when searching for sequences. Given as a proportion of total search sequence length.
    #[arg(short, long, default_value_t = 0.2)]
    error: f32,

    /// Number of threads to use when processing reads.
    #[arg(short, long, default_value_t = 1)]
    threads: usize,

    /// Matchbox script to execute.
    #[arg(short, long)]
    script: String,

    /// Reads. Accepts FASTA/FASTQ files.
    #[arg()]
    reads: Option<String>,

    /// Paired reads. Accepts FASTA/FASTQ files.
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

struct GenericError {
    pub location: Option<Location>,
    pub message: String,
}

impl GenericError {
    /// Pretty prints the error with codespan, and terminates execution
    fn codespan_print_and_exit(&self, global_config: &GlobalConfig) {
        let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
        let config = codespan_reporting::term::Config::default();

        let file = SimpleFile::new(format!("<{}>", global_config.script), "");

        // attach a location if one exists
        let diagnostic = match self.location.clone() {
            Some(location) => Diagnostic::error()
                .with_message(self.message.clone())
                .with_labels(vec![Label::primary((), location.start..location.end)]),
            None => Diagnostic::error().with_message(&self.message),
        };

        term::emit(&mut writer, &config, &file, &diagnostic);
        exit(1)
    }
}

impl From<InputError> for GenericError {
    fn from(value: InputError) -> Self {
        GenericError {
            location: None,
            message: value.to_string(),
        }
    }
}

impl From<ParseError> for GenericError {
    fn from(value: ParseError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

impl From<ElabError> for GenericError {
    fn from(value: ElabError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

impl From<EvalError> for GenericError {
    fn from(value: EvalError) -> Self {
        GenericError {
            location: Some(value.location),
            message: value.message,
        }
    }
}

#[cfg(test)]
mod test {}
