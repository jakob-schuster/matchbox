use std::{
    collections::HashMap,
    fmt::{Display, Pointer},
    fs::File,
    io::{stdin, BufRead, BufReader, Error, Read},
    path::Path,
    sync::Arc,
};

use clap::ValueEnum;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelBridge,
    ParallelIterator,
};

use crate::{
    core::{
        self,
        rec::{self, CSVHeader, ConcreteRec, FullyConcreteRec, Rec},
        Effect, EvalError, Val,
    },
    output::{OutputHandler, OutputHandlerSummary},
    ui::Interface,
    util::{Arena, Cache, CoreRecField, Env},
    InputReads,
};

#[derive(Debug, Clone)]
pub enum ExecError {
    Eval(EvalError),
    Input(InputError),
}

#[derive(Debug, Clone)]
pub enum InputError {
    FileNameError { filename: String },
    FileTypeError(FileTypeError),
    FileOpenError { filename: String },
    NoInputError,
    PairedFiletypes { r1: String, r2: String },
    Read,
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
        }
    }
}

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

#[derive(Clone, ValueEnum)]
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

#[derive(Clone)]
pub enum ComplexFileType {
    FileType {
        filetype: FileType,
    },
    Gz {
        filetype: Arc<ComplexFileType>,
    },
    Paired {
        r1: Arc<ComplexFileType>,
        r2: Arc<ComplexFileType>,
    },
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

/// Checks the arguments, and either opens a file or reads from stdin.
/// Also returns the filetype.
pub fn get_complex_filetype_and_buffer_from_input_reads(
    input_file: &InputReads,
) -> Result<(ComplexFileType, Box<dyn BufRead>), InputError> {
    if let Some(filetype) = &input_file.stdin_format {
        Ok((
            ComplexFileType::FileType {
                filetype: filetype.clone(),
            },
            Box::new(BufReader::new(stdin())),
        ))
    } else if let Some(reads_filename) = &input_file.reads {
        let path = Path::new(&reads_filename);

        let file = File::open(path).map_err(|_| InputError::FileOpenError {
            filename: reads_filename.to_string(),
        })?;

        match &get_extensions(&reads_filename)[..] {
            [.., ext, "gz"] => match FileType::try_from(*ext) {
                Ok(filetype) => Ok((
                    ComplexFileType::FileType { filetype },
                    Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(file))),
                )),
                Err(err) => Err(InputError::FileTypeError(err)),
            },

            [.., ext] => match FileType::try_from(*ext) {
                Ok(filetype) => Ok((
                    ComplexFileType::FileType { filetype },
                    Box::new(BufReader::new(file)),
                )),
                Err(err) => Err(InputError::FileTypeError(err)),
            },

            _ => Err(InputError::FileNameError {
                filename: reads_filename.to_string(),
            }),
        }
    } else {
        todo!()
    }
}

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
                    bar: BarProgress { bar },
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
                    bar: BarProgress { bar },
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
}

struct ReadSource {
    name: String,
    filetype: FileType,
    buffer: Box<dyn BufRead>,
}

pub enum Input {
    Single { source: ReadSource },
    Paired { r1: ReadSource, r2: ReadSource },
}

impl Input {
    fn name(&self) -> &str {
        match self {
            Input::Single { source } => &source.name,
            Input::Paired { r1, r2 } => &r1.name,
        }
    }
}

fn estimate(input_reads: &InputReads, buffer_len: u64) -> Result<Option<u64>, InputError> {
    if let Some(filetype) = &input_reads.stdin_format {
        // no estimate, reading from stdin
        Ok(None)
    } else if let Some(input_reads_file) = &input_reads.reads {
        // reads are from a file
        let filename = input_reads_file.clone();

        // first, open the main read file
        let (filetype, buffer) = get_filetype_and_buffer(&filename)?;

        let f = File::open(&filename).map_err(|e| InputError::FileOpenError {
            filename: filename.to_string(),
        })?;

        // get the length of the file in bytes
        let file_len = f
            .metadata()
            .map_err(|e| InputError::FileOpenError {
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

pub fn open(input_reads: &InputReads) -> Result<Input, InputError> {
    if let Some(filetype) = &input_reads.stdin_format {
        println!("{}", filetype);
        // reads will be stdin
        Ok(Input::Single {
            source: ReadSource {
                name: String::from("stdin"),
                filetype: filetype.clone(),
                buffer: Box::new(BufReader::new(stdin())),
            },
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
            })
        }
    } else {
        // no reads; debug mode!
        Err(InputError::NoInputError)
    }
}

pub fn reader_from_input(input: Input) -> Result<Box<dyn Reader>, InputError> {
    match input {
        Input::Single { source } => match source.filetype {
            FileType::Fasta => Ok(Box::new(FastaReader::new(source.buffer))),
            FileType::Fastq => Ok(Box::new(FastqReader::new(source.buffer))),
            FileType::Sam => Ok(Box::new(SamReader::new(source.buffer))),
            FileType::Bam => Ok(Box::new(BamReader::new(source.buffer))),
            FileType::Matchbox => todo!(),
            FileType::List => todo!(),
            FileType::CSV => Ok(Box::new(DSVReader::new(source.buffer, b','))),
            FileType::TSV => Ok(Box::new(DSVReader::new(source.buffer, b'\t'))),
        },
        Input::Paired { r1, r2 } => match (&r1.filetype, &r2.filetype) {
            (FileType::Fasta, FileType::Fasta) => {
                Ok(Box::new(PairedFastaReader::new(r1.buffer, r2.buffer)))
            }
            (FileType::Fastq, FileType::Fastq) => {
                Ok(Box::new(PairedFastqReader::new(r1.buffer, r2.buffer)))
            }
            (FileType::Sam, FileType::Sam) => {
                Ok(Box::new(PairedSamReader::new(r1.buffer, r2.buffer)))
            }
            (FileType::Bam, FileType::Bam) => {
                Ok(Box::new(PairedBamReader::new(r1.buffer, r2.buffer)))
            }
            _ => Err(InputError::PairedFiletypes {
                r1: r1.filetype.to_string(),
                r2: r2.filetype.to_string(),
            }),
        },
    }
}

pub struct FastaReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl FastaReader {
    fn new(buffer: Box<dyn BufRead>) -> FastaReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"seq",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
            ],
        };

        FastaReader { buffer, ty }
    }
}

impl Reader for FastaReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(rec::FastaRead { read }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct PairedFastaReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedFastaReader {
    fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedFastaReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedFastaReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedFastaReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fasta::Reader::from_bufread(&mut self.paired_buffer).records());

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|(record, paired_record)| match (record, paired_record) {
                        (Ok(read), Ok(paired_read)) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastaRead { read }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastaRead { read: paired_read }),
                                            },
                                        ),
                                    ]),
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        _ => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct FastqReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl FastqReader {
    fn new(buffer: Box<dyn BufRead>) -> FastqReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"seq",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"qual",
                    data: Val::StrTy,
                },
            ],
        };

        FastqReader { buffer, ty }
    }
}

impl Reader for FastqReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(rec::FastqRead { read }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct PairedFastqReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedFastqReader {
    fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedFastqReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedFastqReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedFastqReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fastq::Reader::from_bufread(&mut self.paired_buffer).records());

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|(record, paired_record)| match (record, paired_record) {
                        (Ok(read), Ok(paired_read)) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastqRead { read }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastqRead { read: paired_read }),
                                            },
                                        ),
                                    ]),
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        _ => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct SamReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl SamReader {
    fn new(buffer: Box<dyn BufRead>) -> SamReader {
        let ty = Val::RecTy {
            // in the order as described in the spec
            fields: vec![
                // Query template name
                CoreRecField {
                    name: b"qname",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                // Bitwise flag
                CoreRecField {
                    name: b"flag",
                    data: Val::NumTy,
                },
                CoreRecField {
                    name: b"flag_rec",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"paired",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mapped_in_proper_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"unmapped",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mate_unmapped",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"reverse_strand",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mate_reverse_strand",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"first_in_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"second_in_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"not_primary_alignment",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"fails_platform_quality_checks",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"pcr_or_optical_duplicate",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"supplementary_alignment",
                                data: Val::BoolTy,
                            },
                        ],
                    },
                },
                // Reference sequence name
                CoreRecField {
                    name: b"rname",
                    data: Val::StrTy,
                },
                // 1-based leftmost mapping position
                CoreRecField {
                    name: b"pos",
                    data: Val::NumTy,
                },
                // Mapping quality
                CoreRecField {
                    name: b"mapq",
                    data: Val::NumTy,
                },
                // The CIGAR string
                CoreRecField {
                    name: b"cigar",
                    data: Val::StrTy,
                },
                // Reference name of the mate / next read
                CoreRecField {
                    name: b"rnext",
                    data: Val::StrTy,
                },
                // Position of the mate / next read
                CoreRecField {
                    name: b"pnext",
                    data: Val::NumTy,
                },
                // Observed template length
                CoreRecField {
                    name: b"tlen",
                    data: Val::NumTy,
                },
                // The actual sequence of the alignment
                CoreRecField {
                    name: b"seq",
                    data: Val::StrTy,
                },
                // The quality string of the alignment
                CoreRecField {
                    name: b"qual",
                    data: Val::StrTy,
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"tags",
                    data: Val::StrTy,
                    // data: Val::ListTy {
                    //     ty: Arc::new(Val::StrTy),
                    // },
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
            ],
        };

        SamReader { buffer, ty }
    }
}

impl Reader for SamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();
        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let cigar = read.cigar();
                            let seq = read.sequence();
                            let qual = read.quality_scores();
                            let data = read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(rec::SamRead {
                                    read: &read,
                                    cigar: &cigar,
                                    seq: &seq,
                                    qual: &qual,
                                    data: &data,
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct PairedSamReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedSamReader {
    fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedSamReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            CoreRecField {
                                name: b"flag_rec",
                                data: Val::RecTy {
                                    fields: vec![
                                        CoreRecField {
                                            name: b"paired",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mapped_in_proper_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"first_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"second_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"not_primary_alignment",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"fails_platform_quality_checks",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"pcr_or_optical_duplicate",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"supplementary_alignment",
                                            data: Val::BoolTy,
                                        },
                                    ],
                                },
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            CoreRecField {
                                name: b"flag_rec",
                                data: Val::RecTy {
                                    fields: vec![
                                        CoreRecField {
                                            name: b"paired",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mapped_in_proper_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"first_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"second_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"not_primary_alignment",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"fails_platform_quality_checks",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"pcr_or_optical_duplicate",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"supplementary_alignment",
                                            data: Val::BoolTy,
                                        },
                                    ],
                                },
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedSamReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedSamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let mut paired_reader = noodles::sam::io::Reader::new(&mut self.paired_buffer);

        let header = reader.read_header();
        let paired_header = paired_reader.read_header();

        let input_records = reader.records().zip(paired_reader.records());

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|(record, paired_record)| match (record, paired_record) {
                        (Ok(read), Ok(paired_read)) => {
                            let arena = Arena::new();

                            let cigar = read.cigar();
                            let paired_cigar = paired_read.cigar();

                            let seq = read.sequence();
                            let paired_seq = paired_read.sequence();

                            let qual = read.quality_scores();
                            let paired_qual = paired_read.quality_scores();

                            let data = read.data();
                            let paired_data = paired_read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::SamRead {
                                                    read,
                                                    cigar: &cigar,
                                                    seq: &seq,
                                                    qual: &qual,
                                                    data: &data,
                                                }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::SamRead {
                                                    read: paired_read,
                                                    cigar: &paired_cigar,
                                                    seq: &paired_seq,
                                                    qual: &paired_qual,
                                                    data: &paired_data,
                                                }),
                                            },
                                        ),
                                    ]),
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        _ => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct BamReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl BamReader {
    fn new(buffer: Box<dyn BufRead>) -> BamReader {
        let ty = Val::RecTy {
            // in the order as described in the spec
            fields: vec![
                // Query template name
                CoreRecField {
                    name: b"qname",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                // Bitwise flag
                CoreRecField {
                    name: b"flag",
                    data: Val::NumTy,
                },
                CoreRecField {
                    name: b"flag_rec",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"paired",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mapped_in_proper_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"unmapped",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mate_unmapped",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"reverse_strand",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"mate_reverse_strand",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"first_in_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"second_in_pair",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"not_primary_alignment",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"fails_platform_quality_checks",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"pcr_or_optical_duplicate",
                                data: Val::BoolTy,
                            },
                            CoreRecField {
                                name: b"supplementary_alignment",
                                data: Val::BoolTy,
                            },
                        ],
                    },
                },
                // Reference sequence name
                CoreRecField {
                    name: b"rname",
                    data: Val::StrTy,
                },
                // 1-based leftmost mapping position
                CoreRecField {
                    name: b"pos",
                    data: Val::NumTy,
                },
                // Mapping quality
                CoreRecField {
                    name: b"mapq",
                    data: Val::NumTy,
                },
                // The CIGAR string
                CoreRecField {
                    name: b"cigar",
                    data: Val::StrTy,
                },
                // Reference name of the mate / next read
                CoreRecField {
                    name: b"rnext",
                    data: Val::StrTy,
                },
                // Position of the mate / next read
                CoreRecField {
                    name: b"pnext",
                    data: Val::NumTy,
                },
                // Observed template length
                CoreRecField {
                    name: b"tlen",
                    data: Val::NumTy,
                },
                // The actual sequence of the alignment
                CoreRecField {
                    name: b"seq",
                    data: Val::StrTy,
                },
                // The quality string of the alignment
                CoreRecField {
                    name: b"qual",
                    data: Val::StrTy,
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"tags",
                    data: Val::StrTy,
                    // data: Val::ListTy {
                    //     ty: Arc::new(Val::StrTy),
                    // },
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
            ],
        };

        BamReader { buffer, ty }
    }
}

impl Reader for BamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let mut reader = noodles::bam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();
        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let cigar = read.cigar();
                            let seq = read.sequence();
                            let qual = read.quality_scores();
                            let data = read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(rec::SamRead {
                                    read: &read,
                                    cigar: &cigar,
                                    seq: &seq,
                                    qual: &qual,
                                    data: &data,
                                }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct PairedBamReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedBamReader {
    fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedBamReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            CoreRecField {
                                name: b"flag_rec",
                                data: Val::RecTy {
                                    fields: vec![
                                        CoreRecField {
                                            name: b"paired",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mapped_in_proper_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"first_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"second_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"not_primary_alignment",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"fails_platform_quality_checks",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"pcr_or_optical_duplicate",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"supplementary_alignment",
                                            data: Val::BoolTy,
                                        },
                                    ],
                                },
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            CoreRecField {
                                name: b"flag_rec",
                                data: Val::RecTy {
                                    fields: vec![
                                        CoreRecField {
                                            name: b"paired",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mapped_in_proper_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_unmapped",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"mate_reverse_strand",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"first_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"second_in_pair",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"not_primary_alignment",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"fails_platform_quality_checks",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"pcr_or_optical_duplicate",
                                            data: Val::BoolTy,
                                        },
                                        CoreRecField {
                                            name: b"supplementary_alignment",
                                            data: Val::BoolTy,
                                        },
                                    ],
                                },
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedBamReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedBamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let mut reader = noodles::bam::io::Reader::new(&mut self.buffer);
        let mut paired_reader = noodles::bam::io::Reader::new(&mut self.paired_buffer);

        let header = reader.read_header();
        let paired_header = paired_reader.read_header();

        let input_records = reader.records().zip(paired_reader.records());

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|(record, paired_record)| match (record, paired_record) {
                        (Ok(read), Ok(paired_read)) => {
                            let arena = Arena::new();

                            let cigar = read.cigar();
                            let paired_cigar = paired_read.cigar();

                            let seq = read.sequence();
                            let paired_seq = paired_read.sequence();

                            let qual = read.quality_scores();
                            let paired_qual = paired_read.quality_scores();

                            let data = read.data();
                            let paired_data = paired_read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::SamRead {
                                                    read,
                                                    cigar: &cigar,
                                                    seq: &seq,
                                                    qual: &qual,
                                                    data: &data,
                                                }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::SamRead {
                                                    read: paired_read,
                                                    cigar: &paired_cigar,
                                                    seq: &paired_seq,
                                                    qual: &paired_qual,
                                                    data: &paired_data,
                                                }),
                                            },
                                        ),
                                    ]),
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        _ => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}

pub struct DSVReader {
    reader: csv::Reader<Box<dyn BufRead>>,
    field_names: Vec<String>,
}

impl DSVReader {
    fn new(buffer: Box<dyn BufRead>, delimiter: u8) -> DSVReader {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_reader(buffer);
        let header = reader.headers();

        let field_names = header
            .iter()
            .flat_map(|a| a.iter().map(String::from))
            .collect::<Vec<_>>();

        println!("{}", field_names.join(","));

        // let fields = field_names
        //     .iter()
        //     .map(|name| CoreRecField::new(name.as_bytes(), Val::StrTy))
        //     .collect::<Vec<_>>();

        // let ty = Val::RecTy { fields };

        DSVReader {
            reader,
            field_names,
        }
    }
}

impl Reader for DSVReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let header = CSVHeader::new(self.reader.byte_headers().unwrap());

        let input_records = self.reader.byte_records();
        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();

                            let val = core::Val::Rec {
                                rec: Arc::new(rec::CSVRead {
                                    header: &header,
                                    fields: read,
                                }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        self.reader.records().count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        let fields = self
            .field_names
            .iter()
            .map(|field_name| {
                CoreRecField::new(arena.alloc(field_name.as_bytes().to_vec()), Val::StrTy)
            })
            .collect();

        Val::RecTy { fields }
    }
}
