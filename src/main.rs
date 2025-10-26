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

use core::{library::standard_library, make_portable, EvalError};
use std::{fmt::Debug, fs::File, io::Read, path::Path, process::exit};

use codespan_reporting::{
    diagnostic::{Diagnostic, Label},
    files::{Error, SimpleFile},
    term::{self, termcolor::StandardStream},
};
use input::{
    get_extensions, get_filetype_and_buffer, FileType, FileTypeError, InputError, ReaderWithBar,
};
use output::{OutputError, OutputHandler};
use parse::{parse, ParseError};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use surface::{elab_prog, elab_prog_for_ctx, Context, ElabError};
use util::{Arena, Cache, Env, Location};

mod core;
mod input;
mod myers;
mod output;
mod parse;
mod surface;
mod test;
mod ui;
mod util;
mod visit;

use clap::{Args, Parser};

use crate::{
    input::{open, reader_from_input, BarProgress, CLIFileType, ExecError},
    surface::MatchMode,
};

fn main() {
    // parse in the command line config
    let global_config = GlobalConfig::parse();

    // execute the matchbox script
    run_script(&global_config)
}

/// The global configuration options, accessible as command line parameters.
#[derive(Parser)]
struct GlobalConfig {
    #[command(flatten)]
    input_reads: InputReads,

    #[command(flatten)]
    input_code: InputCode,

    /// Default error rate permitted when searching for sequences. Given as a proportion of total search sequence length.
    #[arg(short, long, default_value_t = 0.0)]
    error: f32,

    /// Number of threads to use when processing reads.
    #[arg(short, long, default_value_t = 1)]
    threads: usize,

    /// Values passed into the matchbox script, via the built-in `args` variable.
    #[arg(short, long, default_value = "")]
    args: String,

    /// Directory to produce CSVs generated from `count!` and `mean!` functions.
    #[arg(short, long, default_value = ".")]
    output_directory: String,

    /// Whether to operate on all matches of a pattern within a read, or just the first one.
    #[arg(short, long, default_value_t = MatchMode::All)]
    match_mode: MatchMode,
}

#[derive(Args)]
struct InputReads {
    /// The format for parsing stdin. To be used when piping input into matchbox
    #[arg(long, short = 'f', conflicts_with_all(["reads"]), required_unless_present_any(["reads"]))]
    stdin_format: Option<CLIFileType>,

    /// A read file to process.
    #[arg(conflicts_with_all(["stdin_format", "debug"]), required_unless_present_any(["stdin_format", "debug"]))]
    reads: Option<String>,

    /// Paired reads. Accepts FASTA/FASTQ/SAM/BAM files. File type must match primary read file.
    #[arg(short, long, requires("reads"))]
    paired_with: Option<String>,

    /// Compile the script and output debug information
    #[arg(long, requires("stdin_format"), conflicts_with_all(["reads"]), conflicts_with_all(["reads"]))]
    debug: bool,
}

#[derive(Args, Clone)]
struct InputReadsFile {
    /// A read file to process.
    #[arg()]
    reads: String,

    /// Paired reads. Accepts FASTA/FASTQ/SAM/BAM files. File type must match primary read file.
    #[arg(short, long)]
    paired_with: Option<String>,
}

#[derive(Args)]
#[group(required = true, multiple = false)]
struct InputCode {
    /// A matchbox script to execute
    #[arg(long, short = 's')]
    script_file: Option<String>,

    /// Some matchbox code to execute
    #[arg(long, short = 'r')]
    run: Option<String>,
}

impl InputCode {
    fn name(&self) -> String {
        if let Some(script_file) = &self.script_file {
            script_file.to_string()
        } else if let Some(run) = &self.run {
            "command line code".to_string()
        } else {
            panic!("no script file or run code?!")
        }
    }

    fn code(&self) -> Result<String, InputError> {
        if let Some(script_file) = &self.script_file {
            read_code_from_script(&script_file)
        } else if let Some(run) = &self.run {
            Ok(run.clone())
        } else {
            panic!("no script file or run code?!")
        }
    }
}

impl GlobalConfig {
    fn default() -> GlobalConfig {
        GlobalConfig {
            input_reads: InputReads {
                stdin_format: Some(CLIFileType::Fastq),
                reads: None,
                debug: false,
                paired_with: None,
            },
            input_code: InputCode {
                script_file: Some("".to_string()),
                run: None,
            },
            output_directory: ".".to_string(),

            error: 0.0,
            threads: 1,

            args: "".to_string(),
            match_mode: MatchMode::All,
        }
    }
}

/// Given the global config,
/// load the matchbox script and run matchbox on input reads.
/// Panic on evaluation errors or internal errors.
fn run_script(global_config: &GlobalConfig) {
    let code = if let Some(code) = &global_config.input_code.run {
        code.to_string()
    } else if let Some(filename) = &global_config.input_code.script_file {
        read_code_from_script(&filename)
            .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
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
fn run(code: &str, global_config: &GlobalConfig) {
    // set up the thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(global_config.threads)
        .build_global()
        .unwrap();

    // establish a global-level arena and context,
    // for values allocated during elaboration
    let arena = Arena::new();
    let ctx = standard_library(&arena);

    // parse the standard library
    let library_code = String::from_utf8(include_bytes!("standard_library.mb").to_vec()).unwrap();
    let library_prog = parse(&library_code, global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // elaborate to generate the context
    let ctx = elab_prog_for_ctx(&arena, &ctx, arena.alloc(library_prog))
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // parse the args program
    let args_prog = parse(&format!("args = {{{}}}", global_config.args), global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        .unwrap();

    let ctx = elab_prog_for_ctx(&arena, &ctx, arena.alloc(args_prog))
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        .unwrap();

    // parse the program
    let prog = parse(code, global_config)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();

    // load in the input reads
    let mut reader_with_bar = ReaderWithBar::new(&global_config.input_reads)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        .unwrap();

    // elaborate to a core program
    let core_prog = elab_prog(
        &arena,
        &ctx.bind_read_from_reader(&arena, reader_with_bar.get_ty(&arena)),
        arena.alloc(prog),
    )
    .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
    // should never unwrap, because program terminates
    .unwrap();

    // cache the values
    let (core_prog, cache) = core_prog
        .cache(
            &arena,
            &ctx.bind_read_from_reader(&arena, reader_with_bar.get_ty(&arena))
                .tms,
        )
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap, because program terminates
        .unwrap();
    // let cache = Cache::default();

    if global_config.input_reads.debug {
        eprintln!("{}", core_prog);
    }

    // create an output handler, to receive output effects
    let mut output_handler = OutputHandler::new(
        global_config.output_directory.clone(),
        reader_with_bar.get_aux_data(),
    )
    .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
    // should never unwrap
    .unwrap();
    // create the standard library
    let env = ctx.tms;

    // process the reads
    reader_with_bar
        .map(&core_prog, &env, &cache, &mut output_handler)
        .map_err(|e| GenericError::from(e).codespan_print_and_exit(global_config))
        // should never unwrap
        .unwrap()
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

/// A simple wrapper around the various error types,
/// so that they can all be universally printed with codespan.
#[derive(Debug)]
struct GenericError {
    location: Option<Location>,
    message: String,
}

impl GenericError {
    /// Pretty prints the error with codespan, and terminates execution
    fn codespan_print_and_exit(&self, global_config: &GlobalConfig) {
        let mut writer = StandardStream::stderr(term::termcolor::ColorChoice::Always);
        let config = codespan_reporting::term::Config::default();

        let file = SimpleFile::new(
            format!("<{}>", global_config.input_code.name()),
            global_config.input_code.code().unwrap(),
        );

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

impl From<ExecError> for GenericError {
    fn from(value: ExecError) -> Self {
        match value {
            ExecError::Eval(eval_error) => GenericError::from(eval_error),
            ExecError::Input(input_error) => GenericError::from(input_error),
            ExecError::Output(output_error) => GenericError::from(output_error),
        }
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

impl From<OutputError> for GenericError {
    fn from(value: OutputError) -> Self {
        GenericError {
            location: None,
            message: value.to_string(),
        }
    }
}
