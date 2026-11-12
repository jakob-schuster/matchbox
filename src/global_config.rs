use super::read_code_from_script;

use clap::{
    builder::styling::{self},
    crate_version, Args, Parser,
};

use input::InputError;

use crate::{
    input::{self, CLIFileType},
    surface::MatchMode,
};

use clap::builder::styling::RgbColor;

pub(crate) const MATCHBOX_YELLOW: RgbColor = RgbColor(239, 233, 64);

pub(crate) const MATCHBOX_ORANGE: RgbColor = RgbColor(247, 146, 38);

pub(crate) const MATCHBOX_RED: RgbColor = RgbColor(196, 39, 54);

pub(crate) const STYLES: styling::Styles = styling::Styles::styled()
    .header(MATCHBOX_ORANGE.on_default().bold())
    .usage(MATCHBOX_ORANGE.on_default().bold())
    .literal(MATCHBOX_YELLOW.on_default().bold())
    .placeholder(MATCHBOX_YELLOW.on_default());

// The global configuration options, accessible as command line parameters.
#[derive(Parser)]
#[command(version = crate_version!(), after_help = "For a guide to the matchbox scripting language, check out the docs at https://jakob-schuster.github.io/matchbox-docs/", styles = STYLES)]
pub(crate) struct GlobalConfig {
    #[command(flatten)]
    pub(crate) input_reads: InputReads,

    #[command(flatten)]
    pub(crate) input_code: InputCode,

    /// Default error rate permitted when searching for sequences. Given as a proportion of total search sequence length.
    #[arg(short, long, default_value_t = 0.0)]
    pub(crate) error: f64,

    /// Number of threads to use when processing reads.
    #[arg(short, long, default_value_t = 1)]
    pub(crate) threads: usize,

    /// Values passed into the matchbox script, via the built-in `args` variable.
    #[arg(short, long, default_value = "")]
    pub(crate) args: String,

    /// Directory to produce CSVs generated from `count!` and `mean!` functions.
    #[arg(short, long, default_value = ".")]
    pub(crate) output_directory: String,

    /// Whether to operate on all matches of a pattern within a read, or just the first one.
    #[arg(short, long, default_value_t = MatchMode::All)]
    pub(crate) match_mode: MatchMode,
}

// Global configuration options related to inputting reads.
#[derive(Args)]
pub(crate) struct InputReads {
    /// The format for parsing stdin. To be used when piping input into matchbox
    #[arg(long, short = 'f', conflicts_with_all(["reads"]), required_unless_present_any(["reads"]))]
    pub(crate) stdin_format: Option<CLIFileType>,

    /// A read file to process.
    #[arg(conflicts_with_all(["stdin_format"]), required_unless_present_any(["stdin_format", "debug"]))]
    pub(crate) reads: Option<String>,

    /// Paired reads. Accepts FASTA/FASTQ/SAM/BAM files. File type must match primary read file.
    #[arg(short, long, requires("reads"))]
    pub(crate) paired_with: Option<String>,

    /// Also process the reverse complement of each input read. Only works for single-end FASTA/FASTQ/SAM/BAM.
    #[arg(long, conflicts_with_all(["paired_with"]))]
    pub(crate) with_reverse_complement: bool,

    /// Compile the script and output debug information
    #[arg(long)]
    pub(crate) debug_compilation: bool,

    /// Compile the script and output parsing information
    #[arg(long)]
    pub(crate) debug_parsing: bool,

    /// Run in a single-threaded mode, for easier profiling with flamegraph / samply
    #[arg(long, conflicts_with_all(["debug_compilation", "threads", "with_reverse_complement"]))]
    pub(crate) debug_single_threaded_run: bool,
}

// An optionally-paired reads file.
#[derive(Args, Clone)]
pub(crate) struct InputReadsFile {
    /// A read file to process.
    #[arg()]
    pub(crate) reads: String,

    /// Paired reads. Accepts FASTA/FASTQ/SAM/BAM files. File type must match primary read file.
    #[arg(short, long)]
    pub(crate) paired_with: Option<String>,
}

// Global configuration options related to inputting matchbox code.
#[derive(Args)]
#[group(required = true, multiple = false)]
pub(crate) struct InputCode {
    /// A matchbox script to execute
    #[arg(long, short = 's')]
    pub(crate) script_file: Option<String>,

    /// Some matchbox code to execute
    #[arg(long, short = 'r')]
    pub(crate) run: Option<String>,
}

impl InputCode {
    /// Get the name of the code file, for displaying in error messages.
    pub(crate) fn name(&self) -> String {
        if let Some(script_file) = &self.script_file {
            script_file.to_string()
        } else if let Some(run) = &self.run {
            "command line code".to_string()
        } else {
            panic!("no script file or run code?!")
        }
    }

    /// Get the content of the code file.
    pub(crate) fn code(&self) -> Result<String, InputError> {
        if let Some(script_file) = &self.script_file {
            read_code_from_script(script_file)
        } else if let Some(run) = &self.run {
            Ok(run.clone())
        } else {
            panic!("no script file or run code?!")
        }
    }
}

impl GlobalConfig {
    pub(crate) fn default() -> GlobalConfig {
        GlobalConfig {
            input_reads: InputReads {
                stdin_format: Some(CLIFileType::Fastq),
                reads: None,
                debug_compilation: false,
                debug_parsing: false,
                paired_with: None,
                with_reverse_complement: false,
                debug_single_threaded_run: false,
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
