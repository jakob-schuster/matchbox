use std::{
    collections::HashMap,
    fmt::{Display, Pointer},
    fs::File,
    io::{stdin, BufRead, BufReader, Error},
    path::Path,
    sync::Arc,
};

use itertools::Itertools;
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelBridge,
    ParallelIterator,
};

use crate::{
    core::{
        self,
        rec::{self, ConcreteRec, FastaRead, FullyConcreteRec, Rec},
        Effect, EvalError, Val,
    },
    output::OutputHandler,
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

pub fn read_any<'p, 'a: 'p>(
    filename: &str,
    paired_filename_opt: Option<String>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let (filetype, buffer) = get_filetype_and_buffer(filename).unwrap();

    match paired_filename_opt {
        // currently, handle paired reads through this clumsy other means
        Some(paired_filename) => {
            let (paired_filetype, paired_buffer) =
                get_filetype_and_buffer(&paired_filename).unwrap();

            match (&filetype, &paired_filetype) {
                (FileType::Fasta, FileType::Fasta) => read_paired_fa_multithreaded(
                    buffer,
                    paired_buffer,
                    prog,
                    env,
                    cache,
                    output_handler,
                ),
                (FileType::Fastq, FileType::Fastq) => read_paired_fq_multithreaded(
                    buffer,
                    paired_buffer,
                    prog,
                    env,
                    cache,
                    output_handler,
                ),
                (FileType::Sam, FileType::Sam) => todo!(),
                _ => panic!(
                    "unexpected combination of {} and {}",
                    filetype, paired_filetype
                ),
            }
        }
        None => match filetype {
            FileType::Fasta => read_fa_multithreaded(buffer, prog, env, cache, output_handler),
            FileType::Fastq => read_fq_multithreaded(buffer, prog, env, cache, output_handler),
            FileType::Sam => read_sam_multithreaded(buffer, prog, env, cache, output_handler),
            _ => panic!("unexpected filetype?!"),
        },
    }
}

pub fn read_fa<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

    for record in input_records {
        match record {
            Ok(read) => {
                let mut arena = Arena::new();
                let val = core::Val::Rec {
                    rec: Arc::new(rec::FastaRead { read: &read }),
                };
                let effects = prog.eval(&arena, env, cache, val).expect("");

                for effect in &effects {
                    output_handler.handle(effect).unwrap();
                }

                arena.reset();
            }
            Err(_) => panic!("bad read?!"),
        }
    }

    output_handler.finish();
}

pub fn read_fa_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fasta::Reader::from_bufread(buffer).records();

    input_records
        .into_iter()
        .chunks(10000)
        .into_iter()
        .for_each(|chunk| {
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
        });

    output_handler.finish();
}

pub fn read_paired_fa_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fasta::Reader::from_bufread(buffer)
        .records()
        .zip(bio::io::fasta::Reader::from_bufread(paired_buffer).records());

    input_records
        .into_iter()
        .chunks(10000)
        .into_iter()
        .for_each(|chunk| {
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
        });

    output_handler.finish();
}

pub fn read_fq<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fastq::Reader::from_bufread(buffer).records();

    input_records
        .into_iter()
        .chunks(10000)
        .into_iter()
        .for_each(|chunk| {
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
        });

    output_handler.finish();
}

pub fn read_fq_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fastq::Reader::from_bufread(buffer).records();

    for chunk in &input_records.chunks(10000) {
        let mut effects = vec![];

        chunk
            .collect_vec()
            .into_par_iter()
            .map(|record| match record {
                Ok(read) => {
                    let arena = Arena::new();
                    let val = core::Val::Rec {
                        rec: Arc::new(rec::FastqRead { read: &read }),
                    };
                    let effects = prog.eval(&arena, env, cache, val);

                    effects
                }
                Err(_) => panic!("bad read?!"),
            })
            .collect_into_vec(&mut effects);

        for result_effects in &effects {
            for effect in result_effects.as_ref().expect("") {
                output_handler.handle(effect).unwrap();
            }
        }
    }

    output_handler.finish();
}

pub fn read_paired_fq_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let input_records = bio::io::fastq::Reader::from_bufread(buffer)
        .records()
        .zip(bio::io::fastq::Reader::from_bufread(paired_buffer).records());

    input_records
        .into_iter()
        .chunks(10000)
        .into_iter()
        .for_each(|chunk| {
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
        });

    output_handler.finish();
}

pub fn read_sam_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let mut reader = noodles::sam::io::Reader::new(buffer);
    let header = reader.read_header();
    let input_records = reader.records();

    for chunk in &input_records.chunks(10000) {
        let mut effects = vec![];

        chunk
            .collect_vec()
            .into_par_iter()
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
            .collect_into_vec(&mut effects);

        for result_effects in &effects {
            for effect in result_effects.as_ref().expect("") {
                output_handler.handle(effect).unwrap();
            }
        }
    }

    output_handler.finish();
}

pub fn read_paired_sam_multithreaded<'p, 'a: 'p>(
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    prog: &core::Prog<'p>,
    env: &Env<Val<'p>>,
    cache: &Cache<Val<'p>>,
    output_handler: &mut OutputHandler,
) {
    let mut reader = noodles::sam::io::Reader::new(buffer);
    let mut paired_reader = noodles::sam::io::Reader::new(paired_buffer);

    let header = reader.read_header();
    let paired_header = paired_reader.read_header();

    let input_records = reader.records().zip(paired_reader.records());

    input_records
        .into_iter()
        .chunks(10000)
        .into_iter()
        .for_each(|chunk| {
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
        });

    output_handler.finish();
}

// enum RecordIterable {
//     Fasta {
//         recs: bio::io::fasta::Records<Box<dyn BufRead>>,
//         fun: Arc<dyn for<'a> Fn(&'a bio::io::fasta::Record) -> Val<'a>>,
//     },

//     Fastq {
//         recs: bio::io::fastq::Records<Box<dyn BufRead>>,
//     },

//     Sam {
//         recs: Box<dyn Iterator<Item = Result<noodles::sam::Record, Error>>>,
//     },

//     Paired {
//         recs1: Arc<RecordIterable>,
//         recs2: Arc<RecordIterable>,
//     },
// }

// impl RecordIterable {
//     fn get_fun(&self) {
//         match self {
//             RecordIterable::Fasta { recs, fun } => todo!(),
//             RecordIterable::Fastq { recs } => todo!(),
//             RecordIterable::Sam { recs } => todo!(),
//             RecordIterable::Paired { recs1, recs2 } => todo!(),
//         }
//     }
// }

// pub struct ReaderIterator {
//     recs: RecordIterable,
//     fun: Arc<dyn Fn(i32) -> str>,
// }

// impl ReaderIterator {
//     fn map<'p>(
//         &mut self,
//         prog: &core::Prog<'p>,
//         env: &Env<Val<'p>>,
//         cache: &Cache<Val<'p>>,
//         output_handler: &mut OutputHandler,
//     ) {
//         match &mut self.recs {
//             RecordIterable::Fasta { recs, .. } => recs
//                 .into_iter()
//                 .chunks(10000)
//                 .into_iter()
//                 .for_each(|chunk| {
//                     let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
//                     chunk
//                         .collect_vec()
//                         .par_iter()
//                         .map(|record| match record {
//                             Ok(read) => {
//                                 let arena = Arena::new();
//                                 let val = core::Val::Rec {
//                                     rec: Arc::new(rec::FastaRead { read }),
//                                 };
//                                 prog.eval(&arena, env, cache, val)
//                             }
//                             Err(_) => panic!("bad read?!"),
//                         })
//                         .collect_into_vec(&mut vec);

//                     for result_effects in &vec {
//                         for effect in result_effects.as_ref().expect("") {
//                             output_handler.handle(effect).unwrap();
//                         }
//                     }
//                 }),
//             RecordIterable::Fastq { recs } => {
//                 recs.into_iter()
//                     .chunks(10000)
//                     .into_iter()
//                     .for_each(|chunk| {
//                         let mut vec: Vec<Result<Vec<Effect>, EvalError>> = vec![];
//                         chunk
//                             .collect_vec()
//                             .par_iter()
//                             .map(|record| match record {
//                                 Ok(read) => {
//                                     let arena = Arena::new();
//                                     let val = core::Val::Rec {
//                                         rec: Arc::new(rec::FastqRead { read }),
//                                     };
//                                     prog.eval(&arena, env, cache, val)
//                                 }
//                                 Err(_) => panic!("bad read?!"),
//                             })
//                             .collect_into_vec(&mut vec);

//                         for result_effects in &vec {
//                             for effect in result_effects.as_ref().expect("") {
//                                 output_handler.handle(effect).unwrap();
//                             }
//                         }
//                     })
//             }
//             RecordIterable::Sam { recs } => todo!(),
//             RecordIterable::Paired { recs1, recs2 } => match recs1 {
//                 RecordIterable::Fasta { recs, fun } => todo!(),
//                 RecordIterable::Fastq { recs } => todo!(),
//                 RecordIterable::Sam { recs } => todo!(),
//                 RecordIterable::Paired { recs1, recs2 } => todo!(),
//             },
//         }
//     }
// }

// trait Reader {
//     fn estimate_reads(filename: &str, buffer_len: u64);

//     fn reads(&self);
// }

// pub fn new_reader(
//     filename: &str,
//     paired_filename: Option<&str>,
// ) -> Result<Arc<dyn Reader>, InputError> {
//     todo!()
// }

// struct ReadError {
//     message: String,
// }

// impl ReadError {
//     fn new(message: &str) -> ReadError {
//         ReadError {
//             message: message.to_string(),
//         }
//     }
// }

// struct FastaReader {
//     records: bio::io::fasta::Records<Box<dyn BufRead>>,
// }

// impl<'a> FastaReader {
//     fn reads(&mut self, buffer: Box<dyn BufRead>) -> Arc<dyn Iterator<Item = Val<'a>>> {
//         // let into_iter().map(|record| match record {
//         //     Ok(read) => Ok(core::Val::Rec {
//         //         rec: Arc::new(rec::FastaRead { read: &read }),
//         //     }),
//         //     Err(_) => Err(ReadError::new("bad read!")),
//         // });

//         todo!()
//     }
//     fn next(&'a mut self) {
//         self.records.next().map(|record| {
//             record.as_ref().map(|read| core::Val::Rec {
//                 rec: Arc::new(FastaRead { read }),
//             })
//         });
//     }
// }
