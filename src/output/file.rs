//! Write to any number of output files.

mod bam;
mod fasta;
mod fastq;
mod sam;
mod txt;

use std::{collections::HashMap, fs::File, io::BufWriter, os::unix::ffi::OsStrExt, path::Path};

use crate::{
    core::PortableVal,
    input::AuxiliaryInputData,
    output::{
        file::{
            bam::SamOrBamWriter, fasta::FastaWriter, fastq::FastqWriter, sam::SamWriter,
            txt::TxtWriter,
        },
        OutputError,
    },
    util::bytes_to_string,
};

/// A file type of an output file
#[derive(Clone, Debug)]
enum FileType {
    Text,
    Fasta,
    Fastq,
    Sam,
    Bam,
}

/// Represents all of the output files.
pub struct FileHandler {
    files: HashMap<Vec<u8>, BufWriter<File>>,
    types: HashMap<Vec<u8>, FileType>,
    new_files: HashMap<Vec<u8>, Box<dyn FileWriter>>,
    dir: String,
    aux_data: AuxiliaryInputData,
}

impl FileHandler {
    /// Create a new file handler, with the auxiliary data passed through from the input
    pub fn new(dir: String, aux_data: AuxiliaryInputData) -> FileHandler {
        FileHandler {
            files: HashMap::default(),
            types: HashMap::default(),

            new_files: HashMap::default(),
            dir,
            aux_data,
        }
    }

    fn type_from_filename(filename: &str) -> FileType {
        match Path::new(filename).extension() {
            Some(s) => match s.as_bytes() {
                b"fa" | b"fasta" => FileType::Fasta,
                b"fq" | b"fastq" => FileType::Fastq,
                b"sam" => FileType::Sam,
                b"bam" => FileType::Bam,
                _ => FileType::Text,
            },
            None => FileType::Text,
        }
    }

    /// Handle a new value, by either creating a new file or adding it to an existing one.
    pub fn handle(&mut self, filename: &[u8], val: &PortableVal) -> Result<(), OutputError> {
        if let Some(file) = self.new_files.get_mut(filename) {
            file.write(val)
        } else {
            // file needs to be created
            // create the file in the subdirectory
            let altered_filename = format!("{}/{}", self.dir, bytes_to_string(filename).unwrap());

            // now, write to it!
            let t = Self::type_from_filename(&altered_filename);

            let f: Box<dyn FileWriter> = match t {
                FileType::Text => Box::new(TxtWriter::new(&altered_filename)?),
                FileType::Fasta => Box::new(FastaWriter::new(&altered_filename)?),
                FileType::Fastq => Box::new(FastqWriter::new(&altered_filename)?),
                FileType::Sam => Box::new(SamWriter::new(&altered_filename, &self.aux_data)?),
                // FileType::Sam => Box::new(SamOrBamWriter::new(
                //     &altered_filename,
                //     &self.aux_data,
                //     noodles_util::alignment::io::Format::Sam,
                // )?),
                FileType::Bam => Box::new(SamOrBamWriter::new(
                    &altered_filename,
                    &self.aux_data,
                    noodles_util::alignment::io::Format::Bam,
                )?),
            };

            // and add to the list, just referring to it by name!
            self.new_files.insert(filename.to_vec(), f);
            self.types.insert(filename.to_vec(), t);

            // then, handle it!
            self.handle(filename, val)
        }
    }

    /// Summarize the file handler by just getting the name of each file
    pub fn summarize(&self) -> FileHandlerSummary {
        FileHandlerSummary {
            files: self.new_files.keys().cloned().collect(),
        }
    }
}

pub trait FileWriter {
    fn write(&mut self, val: &PortableVal) -> Result<(), OutputError>;

    fn finish(&mut self) -> Result<(), OutputError>;
}

#[derive(Clone)]
pub struct FileHandlerSummary {
    files: Vec<Vec<u8>>,
}
