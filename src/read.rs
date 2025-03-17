use std::{
    collections::HashMap,
    fs::File,
    io::{stdin, BufRead, BufReader},
    path::Path,
    sync::Arc,
};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    core::{
        rec::{self, ConcreteRec, FastaRead, Rec},
        Effect, Val,
    },
    util::Arena,
};

fn main() {
    let prog = Prog { data: () };

    let filename = "test.fa";
    let mut global_data = ();

    read(filename, |val| prog.eval(val), |a, b| (), &mut global_data);
}

struct Prog {
    data: (),
}

impl Prog {
    fn eval<'a>(&self, val: Val<'a>) -> Vec<Effect> {
        // first, create a context with val assigned

        // then, execute the program in that context to get all the effects

        todo!()
    }
}

#[derive(Debug)]
pub enum InputError {
    FileNameError,
    FileTypeError(FileTypeError),
    FileOpenError(std::io::Error),
}

#[derive(Debug)]
pub enum FileTypeError {
    UnknownFileType,
}

pub enum FileType {
    Fasta,
    Fastq,
    Sam,
    Bam,
    List,
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

            _ => Err(FileTypeError::UnknownFileType),
        }
    }
}

/// Checks the arguments, and either opens a file or reads from stdin.
/// Also returns the filetype.
fn get_filetype_and_buffer(input_file: &str) -> Result<(FileType, Box<dyn BufRead>), InputError> {
    if input_file.eq("stdin") {
        // just read straight from stdin
        Ok((FileType::Fasta, Box::new(BufReader::new(stdin()))))
    } else {
        let path = Path::new(&input_file);

        let file = File::open(path).map_err(InputError::FileOpenError)?;

        match &input_file.split('.').collect_vec()[..] {
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

            _ => Err(InputError::FileNameError),
        }
    }
}

pub struct BioFastqReader {
    buffer: Box<dyn BufRead>,
    parallel_chunk_size: usize,
    take_first: Option<usize>,
}

impl BioFastqReader {
    pub fn new(
        filename: &str,
        parallel_chunk_size: usize,
        take_first: Option<usize>,
    ) -> Result<BioFastqReader, InputError> {
        Ok(BioFastqReader {
            buffer: todo!(),
            parallel_chunk_size,
            take_first,
        })
    }
}

/// A reader for input files, taking either Fastq or Fasta files.
/// Currently, only the rust-bio parser backend is used.
pub enum Reader {
    BioFasta {
        buffer: Box<dyn BufRead>,
        parallel_chunk_size: usize,
        take_first: Option<usize>,
    },
}

impl Reader {
    /// Creates a new reader, given the filename,
    /// the size of each parallel-processed chunk of reads,
    /// and the option to only take the first n reads.
    pub fn new(
        filename: &str,
        parallel_chunk_size: usize,
        take_first: Option<usize>,
    ) -> Result<Reader, InputError> {
        let (filetype, bufread) = get_filetype_and_buffer(filename)?;

        // just use bio parsers for now; can't use seq_io in parallel on gzipped files
        Ok(Reader::from_bufread(
            bufread,
            &filetype,
            parallel_chunk_size,
            take_first,
        ))
    }

    pub fn from_bufread(
        buffer: Box<dyn BufRead>,
        filetype: &FileType,
        parallel_chunk_size: usize,
        take_first: Option<usize>,
    ) -> Reader {
        match filetype {
            FileType::Fasta => Reader::BioFasta {
                buffer,
                parallel_chunk_size,
                take_first,
            },
            _ => todo!(),
        }
    }

    pub fn map_single_threaded<'a, T, R>(
        self,
        local_fn: impl Fn(FastaRead) -> T,
        global_fn: impl Fn(T, &mut R),
        global_data: &mut R,
    ) {
        match self {
            Reader::BioFasta {
                buffer,
                parallel_chunk_size,
                take_first,
            } => {
                let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

                let recs = match take_first {
                    Some(n) => itertools::Either::Right(input_records.take(n)),
                    None => itertools::Either::Left(input_records),
                };

                recs.for_each(|res| match res {
                    Ok(rec) => {
                        // perform the global and local functions!
                        let a = rec::FastaRead { read: rec };
                        global_fn(local_fn(a), global_data);
                    }
                    Err(_) => panic!("Bad record!"),
                });
            }
        }
    }

    /// Maps a function across all the reads in the input.
    pub fn map<'a, T: Send, R>(
        self,
        local_fn: impl Fn(Val<'a>, &'a Arena) -> T + Sync,
        global_fn: impl Fn(T, &mut R),
        global_data: &mut R,
    ) {
        match self {
            Reader::BioFasta {
                buffer,
                parallel_chunk_size,
                take_first,
            } => {
                let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

                let recs = match take_first {
                    Some(n) => itertools::Either::Right(input_records.take(n)),
                    None => itertools::Either::Left(input_records),
                };

                for bunch in &recs.chunks(parallel_chunk_size) {
                    let mut outs = Vec::new();

                    bunch
                        .collect_vec()
                        .into_par_iter()
                        .map(|result| match result {
                            Err(_) => panic!("Bad record!"),
                            Ok(record) => {
                                let arena = Arena::new();
                                local_fn(
                                    Val::Rec(arena.alloc(rec::FastaRead { read: record })),
                                    &arena,
                                )
                            }
                        })
                        .collect_into_vec(&mut outs);

                    for t in outs {
                        global_fn(t, global_data)
                    }
                }
            }
        }
    }
}

pub fn read_fa<'a, T: Send, R>(
    filename: &str,
    local_fn: impl Fn(Val<'a>) -> T + Sync,
    global_fn: impl Fn(T, &mut R),
    global_data: &mut R,
) {
    let input_records = bio::io::fasta::Reader::from_file(filename)
        .unwrap()
        .records();

    for bunch in &input_records.chunks(10000) {
        let mut outs = Vec::new();

        bunch
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|result| match result {
                Ok(record) => todo!(),
                // TODO this is a userspace error, make it so
                Err(_) => panic!("bad record?!"),
            })
            .collect_into_vec(&mut outs);

        for t in outs {
            global_fn(t, global_data);
        }
    }
}

fn read<'a, T: Send, R>(
    filename: &str,
    local_fn: impl Fn(Val<'a>) -> T + Sync,
    global_fn: impl Fn(T, &mut R),
    global_data: &mut R,
) {
    let reader = bio::io::fasta::Reader::from_file(filename).unwrap();

    for chunk in &reader.records().chunks(1000) {
        let mut outs = Vec::new();

        chunk
            .collect_vec()
            .into_par_iter()
            .map(|rec| match rec {
                Ok(rec) => {
                    let arena = Arena::new();
                    // let v = Val::Rec(arena.alloc(FastaRead { read: rec }));

                    // let mut map: HashMap<&[u8], &Val> = HashMap::new();
                    // map.insert(b"0", arena.alloc(v.clone()));
                    // map.insert(b"1", arena.alloc(v));
                    // let conc = ConcreteRec { map };
                    // let paired = Val::Rec(&conc);

                    // local_fn(paired)
                }
                Err(_) => panic!(),
            })
            .collect_into_vec(&mut outs);

        for t in outs {
            // global_fn(t, global_data)
        }
    }
}
