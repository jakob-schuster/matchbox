//! Handle input from various file formats.

use std::{
    fmt::Display,
    fs::File,
    io::{stdin, BufRead, BufReader, Read},
    path::Path,
    sync::Arc,
};

use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;

use crate::{
    core::{self, EvalError, Val},
    input::{
        bam::{BamReader, PairedBamReader, RevCompBamReader},
        dsv::DSVReader,
        fasta::{FastaReader, PairedFastaReader, RevCompFastaReader},
        fastq::{FastqReader, PairedFastqReader, RevCompFastqReader},
        sam::{PairedSamReader, RevCompSamReader, SamReader},
    },
    output::{OutputError, OutputHandler, OutputHandlerSummary},
    ui::Interface,
    util::{Arena, Cache, Env},
    InputReads,
};

mod bam;
mod dsv;
mod fasta;
mod fastq;
mod sam;

#[derive(Debug, Clone)]
pub enum ExecError {
    Eval(EvalError),
    Input(InputError),
    Output(OutputError),
}

/// An error raised if there was a problem with input files.
/// This is normal, and should be rendered nicely.
#[derive(Debug, Clone)]
pub enum InputError {
    FileNameError { filename: String },
    FileTypeError(FileTypeError),
    FileOpenError { filename: String },
    NoInputError,
    PairedFiletypes { r1: String, r2: String },
    // A problem with a read
    Read,

    // A SAM/BAM header issue
    Header,

    // A SAM/BAM read referenced a reference sequence that was not in the header
    ReferenceID,

    // A SAM/BAM read had an error with a tag
    BAMTag,
    // A SAM/BAM read had an error with a tag
    BAMArrayTag,
}

impl Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputError::FileNameError { filename } => {
                format!("could not determine type of file '{}'", filename).fmt(f)
            }
            InputError::FileTypeError(file_type_error) => file_type_error.fmt(f),
            InputError::FileOpenError { filename } => {
                format!("could not open file '{}'", filename).fmt(f)
            }
            InputError::NoInputError => "no input provided".fmt(f),
            InputError::PairedFiletypes { r1, r2 } => {
                format!("couldn't pair reads of type '{}' with '{}'", r1, r2).fmt(f)
            }
            InputError::Read => "error parsing an input read".fmt(f),
            InputError::Header => "error parsing a SAM/BAM header".fmt(f),
            InputError::ReferenceID => {
                "a BAM read's rname ID was not present in the header; check for issues in header"
            }
            .fmt(f),
            InputError::BAMTag => "error parsing BAM tag".fmt(f),
            InputError::BAMArrayTag => "error parsing BAM array tag".fmt(f),
        }
    }
}

/// An error raised if the filetype did not match what was expected.
#[derive(Debug, Clone)]
pub struct FileTypeError {
    pub extension: String,
    pub expected: Option<String>,
}

impl Display for FileTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.expected {
            Some(expected) => format!(
                "inappropriate file extension '{}', expected '{}'",
                self.extension, expected
            )
            .fmt(f),

            None => format!("inappropriate file extension '{}'", self.extension).fmt(f),
        }
    }
}

/// A file type which can be specified in the CLI as an input format.
#[derive(Clone, ValueEnum)]
pub enum CLIFileType {
    Fa,
    Fasta,
    Fq,
    Fastq,
    Sam,
    Bam,
    CSV,
    TSV,
    List,
    Txt,
}

/// A file type. Collapses alternative representations of the same type (e.g. fq/fastq).
#[derive(Clone)]
pub enum FileType {
    Fasta,
    Fastq,
    Sam,
    Bam,
    Matchbox,
    List,
    CSV,
    TSV,
}

impl From<CLIFileType> for FileType {
    fn from(value: CLIFileType) -> Self {
        match value {
            CLIFileType::Fa | CLIFileType::Fasta => FileType::Fasta,
            CLIFileType::Fq | CLIFileType::Fastq => FileType::Fastq,
            CLIFileType::Sam => FileType::Sam,
            CLIFileType::Bam => FileType::Bam,
            CLIFileType::CSV => FileType::CSV,
            CLIFileType::TSV => FileType::TSV,
            CLIFileType::List => FileType::List,
            CLIFileType::Txt => FileType::List,
        }
    }
}

impl TryFrom<&str> for FileType {
    type Error = FileTypeError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "fa" => Ok(FileType::Fasta),
            "fasta" => Ok(FileType::Fasta),

            "fq" => Ok(FileType::Fastq),
            "fastq" => Ok(FileType::Fastq),

            "list" => Ok(FileType::List),
            "txt" => Ok(FileType::List),

            "sam" => Ok(FileType::Sam),
            "bam" => Ok(FileType::Bam),

            "mb" => Ok(FileType::Matchbox),

            "csv" => Ok(FileType::CSV),
            "tsv" => Ok(FileType::TSV),

            _ => Err(FileTypeError {
                extension: value.to_string(),
                expected: None,
            }),
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Fasta => "fa",
            FileType::Fastq => "fq",
            FileType::Sam => "sam",
            FileType::Bam => "bam",
            FileType::Matchbox => "mb",
            FileType::List => "list",
            FileType::CSV => "csv",
            FileType::TSV => "tsv",
        }
        .fmt(f)
    }
}

/// Given a filename, gets the list of file extensions.
pub fn get_extensions(input_file: &str) -> Vec<&str> {
    input_file.split('.').collect_vec()
}

/// Checks the arguments, and either opens a file or reads from stdin.
/// Also returns the filetype.
pub fn get_filetype_and_buffer(
    input_file: &str,
) -> Result<(FileType, Box<dyn BufRead>), InputError> {
    if input_file.eq("stdin") {
        // just read straight from stdin
        Ok((FileType::Fasta, Box::new(BufReader::new(stdin()))))
    } else {
        let path = Path::new(&input_file);

        let file = File::open(path).map_err(|_| InputError::FileOpenError {
            filename: input_file.to_string(),
        })?;

        match &get_extensions(input_file)[..] {
            [.., ext, "gz"] => match FileType::try_from(*ext) {
                Ok(filetype) => Ok((
                    filetype,
                    Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(file))),
                )),
                Err(err) => Err(InputError::FileTypeError(err)),
            },

            [.., ext] => match FileType::try_from(*ext) {
                Ok(filetype) => Ok((filetype, Box::new(BufReader::new(file)))),
                Err(err) => Err(InputError::FileTypeError(err)),
            },

            _ => Err(InputError::FileNameError {
                filename: input_file.to_string(),
            }),
        }
    }
}

/// A progress summary report to the progress bar.
pub struct ProgressSummary {
    read_increment: usize,
    output_handler_summary: OutputHandlerSummary,
}

impl ProgressSummary {
    fn new(read_increment: usize, output_handler_summary: OutputHandlerSummary) -> ProgressSummary {
        ProgressSummary {
            read_increment,
            output_handler_summary,
        }
    }
}

/// An interface which can display progress updates.
pub trait Progress {
    fn update(&mut self, progress_summary: &ProgressSummary);

    fn finish(&mut self);
}

pub struct UIProgress {
    interface: Interface,
}

impl UIProgress {
    fn new(interface: Interface) -> UIProgress {
        UIProgress { interface }
    }
}

impl Progress for UIProgress {
    fn update(&mut self, progress_summary: &ProgressSummary) {
        self.interface
            .update(&progress_summary.output_handler_summary);
    }

    fn finish(&mut self) {
        self.interface.finish();
    }
}

/// A bar which can display progress updates.
pub struct BarProgress {
    bar: ProgressBar,
}
impl BarProgress {
    fn new(bar: ProgressBar) -> BarProgress {
        BarProgress { bar }
    }
}

impl Progress for BarProgress {
    fn update(&mut self, progress_summary: &ProgressSummary) {
        self.bar.inc(progress_summary.read_increment as u64);
    }

    fn finish(&mut self) {
        self.bar.finish_with_message("Done!");
    }
}

// pub struct ReaderWithUI {
//     interface: UIProgress,
//     reader: Box<dyn Reader>,
// }

// impl ReaderWithUI {
//     pub fn new(input_reads: &InputReads) -> ReaderWithUI {
//         let estimate = estimate_reads(input, 1000000).unwrap();

//         // make the bar
//         let style = ProgressStyle::with_template(
//             "{prefix} {elapsed_precise} [{bar:40.red/yellow}] {percent}% {msg}",
//         )
//         .unwrap()
//         .progress_chars(" @=");
//         let bar = ProgressBar::new(estimate)
//             .with_style(style)
//             .with_prefix(String::from(filename));
//         bar.set_draw_target(ProgressDrawTarget::stderr());

//         ReaderWithUI {
//             interface: UIProgress::new(Interface::new(bar)),
//             reader: reader_from_filename(filename, paired_filename_opt),
//         }
//     }

//     pub fn map<'p>(
//         &mut self,
//         prog: &core::Prog<'p>,
//         env: &Env<Val<'p>>,
//         cache: &Cache<Val<'p>>,
//         output_handler: &mut OutputHandler,
//     ) -> Result<(), EvalError> {
//         self.reader
//             .map(prog, env, cache, output_handler, &mut self.interface)
//     }
// }

/// A reader with a progress bar.
pub struct ReaderWithBar {
    bar: BarProgress,
    reader: Box<dyn Reader>,
}

impl ReaderWithBar {
    pub fn new(input_reads: &InputReads) -> Result<ReaderWithBar, InputError> {
        let input = open(input_reads)?;
        let filename = input.name().to_string();

        let reader = reader_from_input(input)?;

        let estimate = estimate(&input_reads, 1000000).unwrap();

        match estimate {
            Some(estimate) => {
                // make the bar
                let style = ProgressStyle::with_template(
                    "{prefix} {elapsed_precise} [{bar:40.red/yellow}] {percent}% {msg}",
                )
                .unwrap()
                .progress_chars(" @=");
                let bar = ProgressBar::new(estimate)
                    .with_style(style)
                    .with_prefix(filename);
                bar.set_draw_target(ProgressDrawTarget::stderr());

                Ok(ReaderWithBar {
                    bar: BarProgress::new(bar),
                    reader,
                })
            }
            None => {
                // reads are from stdin - can't estimate size!
                // just use spinners!
                let style =
                    ProgressStyle::with_template("{spinner} {elapsed_precise} {prefix} {msg}")
                        .unwrap();

                let bar = ProgressBar::new_spinner()
                    .with_style(style)
                    .with_prefix(filename);

                Ok(ReaderWithBar {
                    bar: BarProgress::new(bar),
                    reader,
                })
            }
        }
    }

    pub fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
    ) -> Result<(), ExecError> {
        self.reader
            .map(prog, env, cache, output_handler, &mut self.bar)
    }

    pub fn get_ty<'a>(&mut self, arena: &'a Arena) -> Val<'a> {
        self.reader.get_ty(arena)
    }

    pub fn get_aux_data(&self) -> AuxiliaryInputData {
        self.reader.get_aux_data()
    }
}

pub enum AuxiliaryInputData {
    None,
    SAMHeader { header: noodles::sam::Header },
}

pub trait Reader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError>;

    fn count(&mut self) -> usize;

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a>;

    fn get_aux_data(&self) -> AuxiliaryInputData {
        AuxiliaryInputData::None
    }
}

/// A source of reads, with a name, a filetype and a buffer.
/// An intermediate representation between InputReads and Reader.
struct ReadSource {
    name: String,
    filetype: FileType,
    buffer: Box<dyn BufRead>,
}

/// An input configuration. An intermediate representation between InputReads and Reader.
pub enum Input {
    Single {
        source: ReadSource,
        with_reverse_complement: bool,
    },
    Paired {
        r1: ReadSource,
        r2: ReadSource,
    },
}

impl Input {
    fn name(&self) -> &str {
        match self {
            Input::Single { source, .. } => &source.name,
            Input::Paired { r1, r2 } => &r1.name,
        }
    }
}

/// Estimate the number of reads in a file.
fn estimate(input_reads: &InputReads, buffer_len: u64) -> Result<Option<u64>, InputError> {
    if let Some(filetype) = &input_reads.stdin_format {
        // no estimate, reading from stdin
        Ok(None)
    } else if let Some(input_reads_file) = &input_reads.reads {
        // reads are from a file
        let filename = input_reads_file.clone();

        // first, open the main read file
        let (filetype, _) = get_filetype_and_buffer(&filename)?;

        let f = File::open(&filename).map_err(|_| InputError::FileOpenError {
            filename: filename.to_string(),
        })?;

        // get the length of the file in bytes
        let file_len = f
            .metadata()
            .map_err(|_| InputError::FileOpenError {
                filename: filename.to_string(),
            })?
            .len();

        // establish a buffer of buffer_len bytes
        let short_buffer = BufReader::new(f).take(buffer_len);
        let short_source = ReadSource {
            name: "".to_string(),
            filetype,
            buffer: Box::new(short_buffer),
        };
        let short_input = Input::Single {
            source: short_source,
            with_reverse_complement: false,
        };
        let mut short_reader = reader_from_input(short_input)?;

        // read in that buffer and count the records you find
        let count = short_reader.count();

        // calculate the estimate
        let estimate = (count as f32 * file_len as f32 / buffer_len as f32) as u64;

        Ok(Some(estimate))
    } else {
        Err(InputError::NoInputError)
    }
}

/// Open the inputs specified in InputReads, returning an Input.
pub fn open(input_reads: &InputReads) -> Result<Input, InputError> {
    if let Some(filetype) = &input_reads.stdin_format {
        // reads will be stdin
        Ok(Input::Single {
            source: ReadSource {
                name: String::from("stdin"),
                filetype: FileType::from(filetype.clone()),
                buffer: Box::new(BufReader::new(stdin())),
            },
            with_reverse_complement: input_reads.with_reverse_complement,
        })
    } else if let Some(input_reads_file) = &input_reads.reads {
        // reads are from a file; or maybe two files

        // first, open the main read file
        let (filetype, buffer) = get_filetype_and_buffer(&input_reads_file)?;

        if let Some(paired_filename) = &input_reads.paired_with {
            // open this file too
            let (paired_filetype, paired_buffer) = get_filetype_and_buffer(&paired_filename)?;

            Ok(Input::Paired {
                r1: ReadSource {
                    name: input_reads_file.clone(),
                    filetype: filetype.clone(),
                    buffer,
                },
                r2: ReadSource {
                    name: paired_filename.clone(),
                    filetype: paired_filetype.clone(),
                    buffer: paired_buffer,
                },
            })
        } else {
            Ok(Input::Single {
                source: ReadSource {
                    name: input_reads_file.clone(),
                    filetype: filetype.clone(),
                    buffer,
                },
                with_reverse_complement: input_reads.with_reverse_complement,
            })
        }
    } else {
        // no reads; debug mode!
        Err(InputError::NoInputError)
    }
}

/// Takes an Input and selects the appropriate Reader.
pub fn reader_from_input(input: Input) -> Result<Box<dyn Reader>, InputError> {
    match input {
        Input::Single {
            source,
            with_reverse_complement,
        } => match (source.filetype, with_reverse_complement) {
            (FileType::Fasta, false) => Ok(Box::new(FastaReader::new(source.buffer))),
            (FileType::Fasta, true) => Ok(Box::new(RevCompFastaReader::new(source.buffer))),
            (FileType::Fastq, false) => Ok(Box::new(FastqReader::new(source.buffer))),
            (FileType::Fastq, true) => Ok(Box::new(RevCompFastqReader::new(source.buffer))),
            (FileType::Sam, false) => Ok(Box::new(SamReader::new(source.buffer)?)),
            (FileType::Sam, true) => Ok(Box::new(RevCompSamReader::new(source.buffer)?)),
            (FileType::Bam, false) => Ok(Box::new(BamReader::new(source.buffer)?)),
            (FileType::Bam, true) => Ok(Box::new(RevCompBamReader::new(source.buffer)?)),
            (FileType::Matchbox, _) => todo!(),
            (FileType::List, _) => todo!(),
            (FileType::CSV, _) => Ok(Box::new(DSVReader::new(source.buffer, b','))),
            (FileType::TSV, _) => Ok(Box::new(DSVReader::new(source.buffer, b'\t'))),
        },
        Input::Paired { r1, r2 } => match (&r1.filetype, &r2.filetype) {
            (FileType::Fasta, FileType::Fasta) => {
                Ok(Box::new(PairedFastaReader::new(r1.buffer, r2.buffer)))
            }
            (FileType::Fastq, FileType::Fastq) => {
                Ok(Box::new(PairedFastqReader::new(r1.buffer, r2.buffer)))
            }
            (FileType::Sam, FileType::Sam) => {
                Ok(Box::new(PairedSamReader::new(r1.buffer, r2.buffer)?))
            }
            (FileType::Bam, FileType::Bam) => {
                Ok(Box::new(PairedBamReader::new(r1.buffer, r2.buffer)?))
            }
            _ => Err(InputError::PairedFiletypes {
                r1: r1.filetype.to_string(),
                r2: r2.filetype.to_string(),
            }),
        },
    }
}
