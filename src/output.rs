use std::{
    collections::{hash_set::Intersection, HashMap},
    fs::File,
    io::{stdout, BufWriter, Stdout, StdoutLock, Write},
    os::unix::ffi::OsStrExt,
    path::Path,
};

use itertools::Itertools;

use crate::core::{Effect, InternalError, PortableVal};
use crate::util::bytes_to_string;

pub struct OutputHandler<'a> {
    output_directory: Option<String>,
    stdout_handler: BufferedStdoutHandler<'a>,
    multi_counts_handler: MultiCountsHandler,
    multi_average_handler: MultiAverageHandler,
    file_handler: FileHandler,
}

#[derive(Clone)]
pub struct OutputHandlerSummary {
    pub stdout_handler: StdoutHandlerSummary,
    pub multi_counts_handler: MultiCountsHandlerSummary,
    pub multi_average_handler: MultiAverageHandlerSummary,
    pub file_handler: FileHandlerSummary,
}

#[derive(Debug)]
pub struct OutputError {
    pub message: String,
}

impl OutputError {
    fn new(message: &str) -> OutputError {
        OutputError {
            message: message.to_string(),
        }
    }
}

impl<'a> OutputHandler<'a> {
    pub fn new(output_path: Option<String>) -> Result<OutputHandler<'a>, OutputError> {
        if let Some(output_path) = output_path.clone() {
            let path = Path::new(&output_path);

            if !path.exists() {
                return Err(OutputError::new(&format!(
                    "path '{}' doesn't exist!",
                    output_path
                )));
            }

            if !path.is_dir() {
                return Err(OutputError::new(&format!(
                    "path '{}' is not a directory!",
                    output_path
                )));
            }
        }

        Ok(OutputHandler {
            output_directory: output_path,
            stdout_handler: BufferedStdoutHandler::new(),
            multi_counts_handler: MultiCountsHandler::default(),
            multi_average_handler: MultiAverageHandler::default(),
            file_handler: FileHandler::default(),
        })
    }

    pub fn handle(&mut self, eff: &Effect) -> Result<(), InternalError> {
        match &eff.handler {
            PortableVal::Rec { fields } => {
                if let Some(PortableVal::Str { s: output }) = fields.get(&b"output".to_vec()) {
                    match &output[..] {
                        b"average" => {
                            self.multi_average_handler.handle(&eff.val)?;
                            Ok(())
                        }
                        b"counts" => {
                            self.multi_counts_handler.handle(&eff.val)?;
                            Ok(())
                        }
                        b"stdout" => {
                            self.stdout_handler.handle(&eff.val)?;
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
        self.multi_counts_handler.finish(&self.output_directory);
        self.multi_average_handler.finish(&self.output_directory);
    }

    pub fn summarize(&self) -> OutputHandlerSummary {
        OutputHandlerSummary {
            stdout_handler: self.stdout_handler.summarize(),
            multi_counts_handler: self.multi_counts_handler.summarize(),
            multi_average_handler: self.multi_average_handler.summarize(),
            file_handler: self.file_handler.summarize(),
        }
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

    pub fn handle(&mut self, val: &PortableVal) -> Result<(), InternalError> {
        self.vec.push(val.to_string());

        if self.vec.len() > self.buffer_size {
            self.stdout.write_all(self.vec.join("\n").as_bytes());
            self.vec = vec![];
        }

        Ok(())
    }

    pub fn finish(&mut self) {
        self.stdout.write_all(self.vec.join("\n").as_bytes());
        self.vec = vec![];
    }

    fn summarize(&self) -> StdoutHandlerSummary {
        StdoutHandlerSummary { lines: vec![] }
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

    fn summarize(&self) -> StdoutHandlerSummary {
        StdoutHandlerSummary { lines: vec![] }
    }
}

#[derive(Clone)]
pub struct StdoutHandlerSummary {
    lines: Vec<String>,
}

#[derive(Clone)]
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
                                    ">{} {}\n{}\n",
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
                                    "@{} {}\n{}\n+\n{}\n",
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

    fn summarize(&self) -> FileHandlerSummary {
        let mut files = vec![];

        for (filename, _) in &self.files {
            let filetype = self.types.get(filename).expect("file without type?!");

            files.push((filename.clone(), filetype.clone(), "".to_string()));
        }

        FileHandlerSummary { files }
    }
}

#[derive(Clone)]
pub struct FileHandlerSummary {
    files: Vec<(String, FileType, String)>,
}

#[derive(Default, PartialEq)]
struct MultiCountsHandler {
    map: HashMap<Vec<u8>, CountsHandler>,
}

impl MultiCountsHandler {
    fn handle(&mut self, val: &PortableVal) -> Result<(), InternalError> {
        match val {
            PortableVal::Rec { fields } => {
                if let (Some(PortableVal::Str { s: name }), Some(val)) = (
                    fields.get(&(b"name".to_vec())),
                    fields.get(&(b"val".to_vec())),
                ) {
                    if let Some(handler) = self.map.get_mut(name) {
                        handler.handle(val)?;
                    } else {
                        let mut handler = CountsHandler::default();
                        handler.handle(val)?;
                        self.map.insert(name.clone(), handler);
                    }

                    Ok(())
                } else {
                    Err(InternalError::new("counts received invalid record?!"))
                }
            }
            _ => Err(InternalError::new("counts received non-record type?!")),
        }
    }

    fn finish(&self, output_directory: &Option<String>) {
        self.print();

        if let Some(output_directory) = output_directory {
            self.print_csv(output_directory);
        }
    }

    fn print(&self) {
        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            println!("{}", String::from_utf8(name.to_vec()).unwrap());
            handler.print();
        }
    }

    fn print_csv(&self, output_directory: &str) {
        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            handler.print_csv(&format!(
                "{}/{}.csv",
                output_directory,
                bytes_to_string(name).unwrap()
            ));
        }
    }

    fn summarize(&self) -> MultiCountsHandlerSummary {
        MultiCountsHandlerSummary {
            all: self
                .map
                .iter()
                .map(|(name, handler)| {
                    (
                        String::from_utf8(name.clone()).unwrap(),
                        handler.summarize(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
pub struct MultiCountsHandlerSummary {
    all: Vec<(String, CountsHandlerSummary)>,
}

#[derive(Default, PartialEq)]
struct CountsHandler {
    map: HashMap<String, i32>,
}

impl CountsHandler {
    fn handle(&mut self, val: &PortableVal) -> Result<(), InternalError> {
        let string = format!("{}", val);

        if let Some(count) = self.map.get(&string) {
            self.map.insert(string, count + 1);
        } else {
            self.map.insert(string, 1);
        }

        Ok(())
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            for (key, val) in self.map.iter().sorted_by_key(|(_, val)| **val) {
                println!("\t{val}\t{key}");
            }
        }
    }

    fn print_csv(&self, filename: &str) {
        let mut file = File::create(filename).unwrap();
        file.write_all(b"value,count\n");

        for (name, value) in self.map.iter().sorted_by_key(|(_, val)| **val) {
            file.write_all(format!("{},{}\n", name, value).as_bytes());
        }
    }

    fn summarize(&self) -> CountsHandlerSummary {
        CountsHandlerSummary {
            top: self.map.clone().into_iter().collect::<Vec<_>>(),
        }
    }
}

#[derive(Clone)]
pub struct CountsHandlerSummary {
    top: Vec<(String, i32)>,
}

#[derive(Default, PartialEq)]
struct MultiAverageHandler {
    map: HashMap<Vec<u8>, AverageHandler>,
}

impl MultiAverageHandler {
    fn handle(&mut self, val: &PortableVal) -> Result<(), InternalError> {
        match val {
            PortableVal::Rec { fields } => {
                if let (Some(PortableVal::Str { s: name }), Some(PortableVal::Num { n: val })) = (
                    fields.get(&(b"name".to_vec())),
                    fields.get(&(b"val".to_vec())),
                ) {
                    if let Some(handler) = self.map.get_mut(name) {
                        handler.handle(*val)?;
                    } else {
                        let mut handler = AverageHandler::default();
                        handler.handle(*val)?;
                        self.map.insert(name.clone(), handler);
                    }

                    Ok(())
                } else {
                    Err(InternalError::new("counts received invalid record?!"))
                }
            }
            _ => Err(InternalError::new("counts received non-record type?!")),
        }
    }

    fn finish(&self, output_directory: &Option<String>) {
        self.print();

        if let Some(output_directory) = output_directory {
            self.print_csv(output_directory);
        }
    }

    fn print(&self) {
        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            println!("{}", String::from_utf8(name.to_vec()).unwrap());
            handler.print();
        }
    }

    fn print_csv(&self, output_directory: &str) {
        let mut file = File::create(format!("{}/stats.csv", output_directory)).unwrap();

        file.write_all(b"name,mean,variance\n");

        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            file.write_all(
                format!(
                    "{},{},{}\n",
                    bytes_to_string(name).unwrap(),
                    handler.mean,
                    handler.variance()
                )
                .as_bytes(),
            );
        }
    }

    fn summarize(&self) -> MultiAverageHandlerSummary {
        MultiAverageHandlerSummary {
            all: self
                .map
                .iter()
                .map(|(name, handler)| {
                    (
                        String::from_utf8(name.clone()).unwrap(),
                        handler.summarize(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
pub struct MultiAverageHandlerSummary {
    all: Vec<(String, AverageHandlerSummary)>,
}

#[derive(Default, PartialEq)]
struct AverageHandler {
    count: i32,
    mean: f32,
    m2: f32,
}

impl AverageHandler {
    fn handle(&mut self, num: f32) -> Result<(), InternalError> {
        self.count += 1;
        let delta = num - self.mean;
        self.mean += delta / self.count as f32;
        let delta2 = num - self.mean;
        self.m2 += delta * delta2;

        Ok(())
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            println!("\t{:.2} ± {:.2}", self.mean, self.variance());
        }
    }

    fn summarize(&self) -> AverageHandlerSummary {
        AverageHandlerSummary {
            mean: self.mean,
            variance: self.variance(),
        }
    }

    fn variance(&self) -> f32 {
        (self.m2 / self.count as f32).sqrt()
    }
}

#[derive(Clone)]
pub struct AverageHandlerSummary {
    pub mean: f32,
    pub variance: f32,
}
