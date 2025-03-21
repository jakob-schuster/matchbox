use std::{
    collections::HashMap,
    fmt::{Display, Pointer},
    fs::File,
    io::{stdin, BufRead, BufReader},
    path::Path,
    sync::Arc,
};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};

use crate::{
    core::{
        self,
        rec::{self, ConcreteRec, FastaRead, Rec},
        Effect, Val,
    },
    output::OutputHandler,
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
    extension: String,
}

impl Display for FileTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!("inappropriate file extension '{}'", self.extension).fmt(f)
    }
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

            _ => Err(FileTypeError {
                extension: value.to_string(),
            }),
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

        let file = File::open(path).map_err(|_| InputError::FileOpenError {
            filename: input_file.to_string(),
        })?;

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

            _ => Err(InputError::FileNameError {
                filename: input_file.to_string(),
            }),
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
                                todo!()
                                // let arena = Arena::new();
                                // local_fn(
                                //     Val::Rec(arena.alloc(rec::FastaRead { read: record })),
                                //     &arena,
                                // )
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

pub fn read_any<'p, 'a: 'p>(
    filename: &str,
    prog: &core::Prog<'p>,
    arena: &'p Arena,
    output_handler: &mut OutputHandler,
) {
    let (filetype, buffer) = get_filetype_and_buffer(filename).unwrap();

    match filetype {
        FileType::Fasta => read_fa_new(buffer, prog, arena, output_handler),
        FileType::Fastq => read_fq_new(buffer, prog, arena, output_handler),
        _ => todo!(),
    }
}

pub fn read_fa_new<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    arena: &'p Arena,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

    // let arena = Arena::new();
    for record in input_records {
        match record {
            Ok(read) => {
                let val = core::Val::Rec(arena.alloc(rec::FastaRead { read }));
                let effects = prog.eval(arena.alloc(val), &arena).expect("");

                // println!(
                //     "{}",
                //     effects
                //         .iter()
                //         .map(|a| a.to_string())
                //         .collect::<Vec<_>>()
                //         .join(", ")
                // );

                for effect in &effects {
                    output_handler.handle(effect);
                }
            }
            Err(_) => panic!("bad read?!"),
        }
    }

    output_handler.finish();
}

// pub fn read_fa_multithreaded_new<'p, 'a: 'p>(
//     buffer: Box<dyn BufRead>,
//     prog: &core::Prog<'p>,
//     arena: &'p Arena,
//     output_handler: &mut OutputHandler,
// ) {
//     let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

//     // let arena = Arena::new();
//     for chunk in &input_records.chunks(10000) {
//         chunk
//             .collect_vec()
//             .into_par_iter()
//             .map(|record| match record {
//                 Ok(read) => {
//                     let arena = Arena::new();
//                     let val = core::Val::Rec(arena.alloc(rec::FastaRead { read }));
//                     let effects = prog.eval(arena.alloc(val), &arena).expect("");

//                     effects
//                 }
//                 Err(_) => panic!("bad read?!"),
//             });

//         // match record {
//         //     Ok(read) => {
//         //         let val = core::Val::Rec(arena.alloc(rec::FastaRead { read }));
//         //         let effects = prog.eval(arena.alloc(val), &arena).expect("");

//         //         // println!(
//         //         //     "{}",
//         //         //     effects
//         //         //         .iter()
//         //         //         .map(|a| a.to_string())
//         //         //         .collect::<Vec<_>>()
//         //         //         .join(", ")
//         //         // );

//         //         for effect in &effects {
//         //             output_handler.handle(effect);
//         //         }
//         //     }
//         //     Err(_) => panic!("bad read?!"),
//         // }
//     }

//     output_handler.finish();
// }

pub fn read_fq_new<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    arena: &'p Arena,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fastq::Reader::from_bufread(buffer).records();

    // let arena = Arena::new();
    for record in input_records {
        match record {
            Ok(read) => {
                let val = core::Val::Rec(arena.alloc(rec::FastqRead { read }));
                let effects = prog.eval(arena.alloc(val), &arena).expect("");

                for effect in &effects {
                    output_handler.handle(effect);
                }
            }
            Err(_) => panic!("bad read?!"),
        }
    }

    output_handler.finish();
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
