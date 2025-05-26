use std::{
    collections::HashMap,
    fmt::{Display, Pointer},
    fs::File,
    io::{stdin, BufRead, BufReader, Error},
    path::Path,
    sync::Arc,
};

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelBridge,
    ParallelIterator,
};

use crate::{
    core::{
        self,
        rec::{self, CSVHeader, ConcreteRec, FastaRead, FullyConcreteRec, Rec},
        Effect, EvalError, Val,
    },
    output::{OutputHandler, OutputHandlerSummary},
    ui::Interface,
    util::{Arena, Cache, Env},
};

#[derive(Debug)]
pub enum InputError {
    FileNameError { filename: String },
    FileTypeError(FileTypeError),
    FileOpenError { filename: String },
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
        }
    }
}

#[derive(Debug)]
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

pub struct ReaderWithUI {
    interface: UIProgress,
    reader: Box<dyn Reader>,
}

impl ReaderWithUI {
    pub fn new(filename: &str, paired_filename_opt: Option<String>) -> ReaderWithUI {
        let estimate = estimate_reads(filename, 1000000).unwrap();

        // make the bar
        let style = ProgressStyle::with_template(
            "{prefix} {elapsed_precise} [{bar:40.red/yellow}] {percent}% {msg}",
        )
        .unwrap()
        .progress_chars(" @=");
        let bar = ProgressBar::new(estimate)
            .with_style(style)
            .with_prefix(String::from(filename));
        bar.set_draw_target(ProgressDrawTarget::stderr());

        ReaderWithUI {
            interface: UIProgress::new(Interface::new(bar)),
            reader: reader_from_filename(filename, paired_filename_opt),
        }
    }

    pub fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
    ) {
        self.reader
            .map(prog, env, cache, output_handler, &mut self.interface);
    }
}

pub struct ReaderWithBar {
    bar: BarProgress,
    reader: Box<dyn Reader>,
}

impl ReaderWithBar {
    pub fn new(filename: &str, paired_filename_opt: Option<String>) -> ReaderWithBar {
        let estimate = estimate_reads(filename, 1000000).unwrap();

        // make the bar
        let style = ProgressStyle::with_template(
            "{prefix} {elapsed_precise} [{bar:40.red/yellow}] {percent}% {msg}",
        )
        .unwrap()
        .progress_chars(" @=");
        let bar = ProgressBar::new(estimate)
            .with_style(style)
            .with_prefix(String::from(filename));
        bar.set_draw_target(ProgressDrawTarget::stderr());

        // just use bio parsers for now; can't use seq_io in parallel on gzipped files
        let reader = reader_from_filename(filename, paired_filename_opt);

        ReaderWithBar {
            bar: BarProgress { bar },
            reader,
        }
    }

    pub fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
    ) {
        self.reader
            .map(prog, env, cache, output_handler, &mut self.bar);
    }
}

fn estimate_reads(filename: &str, buffer_len: u64) -> Result<u64, InputError> {
    use std::io::Read;

    let (filetype, _) = get_filetype_and_buffer(filename)?;

    let f = File::open(filename).map_err(|e| InputError::FileOpenError {
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
    let short_bufread = BufReader::new(f).take(buffer_len);

    // read in that buffer and count the records you find
    let count = reader_from_buffer_and_filetype(filetype, Box::new(short_bufread), None).count();

    // calculate the estimate
    let estimate = (count as f32 * file_len as f32 / buffer_len as f32) as u64;

    Ok(estimate)
}

pub trait Reader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    );

    fn count(&mut self) -> usize;
}

pub fn reader_from_filename(
    filename: &str,
    paired_filename_opt: Option<String>,
) -> Box<dyn Reader> {
    let (filetype, buffer) = get_filetype_and_buffer(filename).unwrap();
    let paired_file_opt =
        paired_filename_opt.map(|filename| get_filetype_and_buffer(&filename).unwrap());

    reader_from_buffer_and_filetype(filetype, buffer, paired_file_opt)
}

pub fn reader_from_buffer_and_filetype(
    filetype: FileType,
    buffer: Box<dyn BufRead>,
    paired_file: Option<(FileType, Box<dyn BufRead>)>,
) -> Box<dyn Reader> {
    match paired_file {
        // currently, handle paired reads through this clumsy other means
        Some((paired_filetype, paired_buffer)) => match (&filetype, &paired_filetype) {
            (FileType::Fasta, FileType::Fasta) => Box::new(PairedFastaReader {
                buffer,
                paired_buffer,
            }),
            (FileType::Fastq, FileType::Fastq) => Box::new(PairedFastqReader {
                buffer,
                paired_buffer,
            }),
            (FileType::Sam, FileType::Sam) => Box::new(PairedSamReader {
                buffer,
                paired_buffer,
            }),
            _ => panic!(
                "unexpected combination of {} and {}",
                filetype, paired_filetype
            ),
        },
        None => match filetype {
            FileType::Fasta => Box::new(FastaReader { buffer }),
            FileType::Fastq => Box::new(FastqReader { buffer }),
            FileType::Sam => Box::new(SamReader { buffer }),
            FileType::CSV => Box::new(CSVReader { buffer }),
            FileType::TSV => Box::new(TSVReader { buffer }),
            _ => panic!("unexpected filetype?!"),
        },
    }
}

pub struct FastaReader {
    buffer: Box<dyn BufRead>,
}

impl Reader for FastaReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(rec::FastaRead { read }),
                            };
                            prog.eval(&arena, env, cache, val)
                        }
                        Err(_) => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }
}

pub struct PairedFastaReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
}

impl Reader for PairedFastaReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fasta::Reader::from_bufread(&mut self.paired_buffer).records());

        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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

                            prog.eval(&arena, env, cache, val)
                        }
                        _ => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }
}

pub struct FastqReader {
    buffer: Box<dyn BufRead>,
}

impl Reader for FastqReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(rec::FastqRead { read }),
                            };
                            prog.eval(&arena, env, cache, val)
                        }
                        Err(_) => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }
}

pub struct PairedFastqReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
}

impl Reader for PairedFastqReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fastq::Reader::from_bufread(&mut self.paired_buffer).records());

        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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

                            prog.eval(&arena, env, cache, val)
                        }
                        _ => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }
}

pub struct SamReader {
    buffer: Box<dyn BufRead>,
}

impl Reader for SamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();
        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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
                            let effects = prog.eval(&arena, env, cache, val);

                            effects
                        }
                        Err(_) => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }
}

pub struct PairedSamReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
}

impl Reader for PairedSamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let mut paired_reader = noodles::sam::io::Reader::new(&mut self.paired_buffer);

        let header = reader.read_header();
        let paired_header = paired_reader.read_header();

        let input_records = reader.records().zip(paired_reader.records());

        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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

                            prog.eval(&arena, env, cache, val)
                        }
                        _ => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();

        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let mut reader = noodles::sam::io::Reader::new(&mut self.buffer);
        let header = reader.read_header();
        let input_records = reader.records();

        input_records.count()
    }
}

pub struct CSVReader {
    buffer: Box<dyn BufRead>,
}

impl Reader for CSVReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let mut reader = csv::Reader::from_reader(&mut self.buffer);

        let header = CSVHeader::new(reader.byte_headers().unwrap());

        let input_records = reader.byte_records();
        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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
                            let effects = prog.eval(&arena, env, cache, val);

                            effects
                        }
                        Err(_) => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let mut reader = csv::Reader::from_reader(&mut self.buffer);

        reader.records().count()
    }
}

pub struct TSVReader {
    buffer: Box<dyn BufRead>,
}

impl Reader for TSVReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(&mut self.buffer);

        let header = CSVHeader::new(reader.byte_headers().unwrap());

        let input_records = reader.byte_records();
        let final_progress = input_records.into_iter().chunks(10000).into_iter().fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
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
                            let effects = prog.eval(&arena, env, cache, val);

                            effects
                        }
                        Err(_) => panic!("bad read?!"),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().expect("") {
                        output_handler.handle(effect).unwrap();
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                progress0
            },
        );

        final_progress.finish();
        output_handler.finish();
    }

    fn count(&mut self) -> usize {
        let mut reader = csv::Reader::from_reader(&mut self.buffer);

        reader.records().count()
    }
}
