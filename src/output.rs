use std::{
    collections::HashMap,
    fs::File,
    io::{stdout, BufWriter, Stdout, StdoutLock, Write},
    os::unix::ffi::OsStrExt,
    path::Path,
};

use itertools::Itertools;

use crate::core::{Effect, InternalError, PortableVal};
use crate::util::bytes_to_string;

pub struct OutputHandler<'a> {
    stdout_handler: BufferedStdoutHandler<'a>,
    counts_handler: CountsHandler,
    average_handler: AverageHandler,
    file_handler: FileHandler,
}

impl<'a> OutputHandler<'a> {
    pub fn new() -> OutputHandler<'a> {
        OutputHandler {
            stdout_handler: BufferedStdoutHandler::new(),
            counts_handler: CountsHandler::default(),
            average_handler: AverageHandler::default(),
            file_handler: FileHandler::default(),
        }
    }

    pub fn handle(&mut self, eff: &Effect) -> Result<(), InternalError> {
        match &eff.handler {
            PortableVal::Rec { fields } => {
                if let Some(PortableVal::Str { s: output }) = fields.get(&b"output".to_vec()) {
                    match &output[..] {
                        b"average" => match eff.val {
                            PortableVal::Num { n } => {
                                self.average_handler.handle(n.round() as i32);
                                Ok(())
                            }
                            _ => Err(InternalError::new("only numeric values can be averaged")),
                        },
                        b"counts" => {
                            self.counts_handler.exec(&eff.val);
                            Ok(())
                        }
                        b"stdout" => {
                            self.stdout_handler.handle(&eff.val);
                            Ok(())
                        }
                        b"file" => match fields.get(&b"filename".to_vec()) {
                            Some(PortableVal::Str { s: filename }) => self
                                .file_handler
                                .handle(&String::from_utf8(filename.clone()).unwrap(), &eff.val),
                            _ => todo!(),
                        },
                        _ => Err(InternalError::new("unrecognised output type")),
                    }
                } else {
                    Err(InternalError::new("unrecognised output type"))
                }
            }
            _ => Err(InternalError::new("unrecognised output type")),
        }
    }

    pub fn finish(&mut self) {
        self.stdout_handler.finish();
        self.counts_handler.print();
        self.average_handler.print();
    }
}

struct BufferedStdoutHandler<'a> {
    stdout: BufWriter<StdoutLock<'a>>,
    vec: Vec<String>,
    buffer_size: usize,
}
impl<'a> BufferedStdoutHandler<'a> {
    pub fn new() -> BufferedStdoutHandler<'a> {
        BufferedStdoutHandler {
            stdout: BufWriter::new(stdout().lock()),
            vec: vec![],
            buffer_size: 1000000,
        }
    }

    pub fn handle(&mut self, val: &PortableVal) {
        self.vec.push(val.to_string());

        if self.vec.len() > self.buffer_size {
            self.stdout.write_all(self.vec.join("\n").as_bytes());
            self.vec = vec![];
        }
    }

    pub fn finish(&mut self) {
        self.stdout.write_all(self.vec.join("\n").as_bytes());
        self.vec = vec![];
    }
}

/// Naive stdout handler; simply prints to stdout with println!
/// Included for benchmarking purposes
struct StdoutHandler {}
impl StdoutHandler {
    pub fn new() -> StdoutHandler {
        StdoutHandler {}
    }

    pub fn handle(&mut self, val: &PortableVal) {
        println!("{}", val);
    }

    pub fn finish(&mut self) {}
}

enum FileType {
    Text,
    Fasta,
    Fastq,
    Sam,
}

#[derive(Default)]
struct FileHandler {
    files: HashMap<String, BufWriter<File>>,
    types: HashMap<String, FileType>,
}

impl FileHandler {
    fn type_from_filename(filename: &str) -> FileType {
        match Path::new(filename).extension() {
            Some(s) => match s.as_bytes() {
                b"fq" | b"fastq" => FileType::Fastq,
                b"fa" | b"fasta" => FileType::Fasta,
                b"sam" => FileType::Sam,
                _ => FileType::Text,
            },
            None => FileType::Text,
        }
    }

    pub fn handle(&mut self, filename: &str, val: &PortableVal) -> Result<(), InternalError> {
        if let (Some(file), Some(filetype)) =
            (self.files.get_mut(filename), self.types.get(filename))
        {
            match filetype {
                FileType::Text => {
                    file.write_all(format!("{}\n", val).as_bytes())
                        .expect("Couldn't write to file!");
                    Ok(())
                }
                FileType::Fasta => match val {
                    PortableVal::Rec { fields } => {
                        if let (
                            Some(PortableVal::Str { s: seq }),
                            Some(PortableVal::Str { s: id }),
                            Some(PortableVal::Str { s: desc }),
                        ) = (
                            fields.get(&b"seq".to_vec()),
                            fields.get(&b"id".to_vec()),
                            fields.get(&b"desc".to_vec()),
                        ) {
                            file.write_all(
                                format!(
                                    ">{}{}\n{}\n",
                                    bytes_to_string(id).unwrap(),
                                    bytes_to_string(desc).unwrap(),
                                    bytes_to_string(seq).unwrap()
                                )
                                .as_bytes(),
                            )
                            .expect("Couldn't write to file!");

                            Ok(())
                        } else {
                            Err(InternalError::new(
                                "read didn't have correct fields to write to FASTA",
                            ))
                        }
                    }
                    _ => Err(InternalError::new(
                        "trying to write to a FASTA file with a value that isn't a read",
                    )),
                },
                FileType::Fastq => match val {
                    PortableVal::Rec { fields } => {
                        if let (
                            Some(PortableVal::Str { s: seq }),
                            Some(PortableVal::Str { s: id }),
                            Some(PortableVal::Str { s: desc }),
                            Some(PortableVal::Str { s: qual }),
                        ) = (
                            fields.get(&b"seq".to_vec()),
                            fields.get(&b"id".to_vec()),
                            fields.get(&b"desc".to_vec()),
                            fields.get(&b"qual".to_vec()),
                        ) {
                            file.write_all(
                                format!(
                                    "@{}{}\n{}\n+\n{}\n",
                                    bytes_to_string(id).unwrap(),
                                    bytes_to_string(desc).unwrap(),
                                    bytes_to_string(seq).unwrap(),
                                    bytes_to_string(qual).unwrap()
                                )
                                .as_bytes(),
                            )
                            .expect("Couldn't write to file!");

                            Ok(())
                        } else {
                            Err(InternalError::new(
                                "read didn't have correct fields to write to FASTA",
                            ))
                        }
                    }
                    _ => Err(InternalError::new(
                        "trying to write to a FASTA file with a value that isn't a read",
                    )),
                },

                FileType::Sam => match val {
                    PortableVal::Rec { fields } => {
                        if let (
                            Some(PortableVal::Str { s: qname }),
                            Some(PortableVal::Num { n: flag }),
                            Some(PortableVal::Str { s: rname }),
                            Some(PortableVal::Num { n: pos }),
                            Some(PortableVal::Num { n: mapq }),
                            Some(PortableVal::Str { s: cigar }),
                            Some(PortableVal::Str { s: rnext }),
                            Some(PortableVal::Num { n: pnext }),
                            Some(PortableVal::Num { n: tlen }),
                            Some(PortableVal::Str { s: seq }),
                            Some(PortableVal::Str { s: qual }),
                            Some(PortableVal::Str { s: tags }),
                        ) = (
                            fields.get(&b"qname".to_vec()),
                            fields.get(&b"flag".to_vec()),
                            fields.get(&b"rname".to_vec()),
                            fields.get(&b"pos".to_vec()),
                            fields.get(&b"mapq".to_vec()),
                            fields.get(&b"cigar".to_vec()),
                            fields.get(&b"rnext".to_vec()),
                            fields.get(&b"pnext".to_vec()),
                            fields.get(&b"tlen".to_vec()),
                            fields.get(&b"seq".to_vec()),
                            fields.get(&b"qual".to_vec()),
                            fields.get(&b"tags".to_vec()),
                        ) {
                            file.write_all(
                                format!(
                                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                                    bytes_to_string(qname).unwrap(),
                                    flag,
                                    bytes_to_string(rname).unwrap(),
                                    pos,
                                    mapq,
                                    bytes_to_string(cigar).unwrap(),
                                    bytes_to_string(rnext).unwrap(),
                                    pnext,
                                    tlen,
                                    bytes_to_string(seq).unwrap(),
                                    bytes_to_string(qual).unwrap(),
                                    bytes_to_string(tags).unwrap(),
                                    // data.iter()
                                    //     .map(|v| v.to_string())
                                    //     .collect::<Vec<_>>()
                                    //     .join("\t"),
                                )
                                .as_bytes(),
                            )
                            .expect("Couldn't write to file!");

                            Ok(())
                        } else {
                            Err(InternalError::new(
                                "read didn't have correct fields to write to FASTA",
                            ))
                        }
                    }
                    _ => Err(InternalError::new(
                        "trying to write to a SAM file with a value that isn't a read",
                    )),
                },
            }
        } else {
            // file needs to be created
            if let Ok(file) = File::create(filename) {
                // now, write to it!
                let t = Self::type_from_filename(filename);
                // and add to the list
                self.files
                    .insert(String::from(filename), BufWriter::new(file));
                self.types.insert(String::from(filename), t);

                // then, handle it!
                self.handle(filename, val)
            } else {
                panic!("File {} couldn't be created!", filename)
            }
        }
    }
}

#[derive(Default, PartialEq)]
struct CountsHandler {
    map: HashMap<String, i32>,
}

impl CountsHandler {
    fn exec(&mut self, val: &PortableVal) {
        let string = format!("{}", val);

        if let Some(count) = self.map.get(&string) {
            self.map.insert(string, count + 1);
        } else {
            self.map.insert(string, 1);
        }
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            println!(": --- Counted Values --- :");
            for (key, val) in self.map.iter().sorted_by_key(|(_, val)| **val) {
                println!("{val}\t{key}");
            }
        }
    }
}

#[derive(Default, PartialEq)]
struct AverageHandler {
    count: i32,
    mean: f32,
    m2: f32,
}

impl AverageHandler {
    fn handle(&mut self, num: i32) {
        self.count += 1;
        let delta = num as f32 - self.mean;
        self.mean += delta / self.count as f32;
        let delta2 = num as f32 - self.mean;
        self.m2 += delta * delta2;
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            println!(
                "Average: {:.2} ± {:.2}",
                self.mean,
                (self.m2 / self.count as f32).sqrt()
            );
        }
    }
}
