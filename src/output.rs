//! Handle various forms of output.

use itertools::Itertools;

use std::{
    collections::{hash_set::Intersection, HashMap},
    fmt::Display,
    fs::File,
    io::{stdout, BufWriter, Stdout, StdoutLock, Write},
    os::unix::ffi::OsStrExt,
    path::Path,
    process::Output,
};

use crate::{
    core::Val,
    output::{
        average::{MultiAverageHandler, MultiAverageHandlerSummary},
        counts::{MultiCountsHandler, MultiCountsHandlerSummary},
        file::{FileHandler, FileHandlerSummary},
        stdout::{BufferedStdoutHandler, StdoutHandlerSummary},
    },
    util::bytes_to_string,
};
use crate::{
    core::{Effect, InternalError, PortableVal},
    input::AuxiliaryInputData,
};

mod average;
mod counts;
mod file;
mod stdout;

/// A handler for all output handlers.
pub struct OutputHandler<'a> {
    output_directory: String,
    stdout_handler: BufferedStdoutHandler<'a>,
    multi_counts_handler: MultiCountsHandler,
    multi_average_handler: MultiAverageHandler,
    file_handler: FileHandler,
}

/// A summary reporting on the progress of various outputs.
#[derive(Clone)]
pub struct OutputHandlerSummary {
    pub stdout_handler: StdoutHandlerSummary,
    pub multi_counts_handler: MultiCountsHandlerSummary,
    pub multi_average_handler: MultiAverageHandlerSummary,
    pub file_handler: FileHandlerSummary,
}

/// An error resulting from producing output.
/// This is expected and should be displayed nicely.
#[derive(Debug, Clone)]
pub enum OutputError {
    /// Error when creating file
    Create {
        filename: String,
    },

    /// Error with type of written value
    Type {
        val: PortableVal,
    },
    TypeCounts {
        val: PortableVal,
    },

    /// Error when writing to file
    Write,

    /// Error when flushing
    Flush,

    Other {
        message: String,
    },

    /// Trying to write to an unrecognized output type
    UnrecognizedOutputType,

    BadSAMTag,
}

impl OutputError {
    fn new(message: &str) -> OutputError {
        OutputError::Other {
            message: message.to_string(),
        }
    }
}

impl Display for OutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputError::Create { filename } => format!("couldn't create file '{filename}'").fmt(f),
            OutputError::Type { val } => format!("error sending value '{}' into file", val).fmt(f),
            OutputError::TypeCounts { val } => {
                format!("error sending value '{}' to output", val).fmt(f)
            }
            OutputError::Write => "error writing to file".fmt(f),
            OutputError::Flush => "error writing to file".fmt(f),
            OutputError::Other { message } => message.fmt(f),
            OutputError::UnrecognizedOutputType => "unrecognized output type".fmt(f),
            OutputError::BadSAMTag => {
                "error writing SAM/BAM tag; make sure you are following the SAM specification"
                    .fmt(f)
            }
        }
    }
}

impl<'a> OutputHandler<'a> {
    pub fn new(
        output_directory: String,
        aux_data: AuxiliaryInputData,
    ) -> Result<OutputHandler<'a>, OutputError> {
        let path = Path::new(&output_directory);

        if !path.exists() {
            return Err(OutputError::new(&format!(
                "path '{}' doesn't exist!",
                output_directory
            )));
        }

        if !path.is_dir() {
            return Err(OutputError::new(&format!(
                "path '{}' is not a directory!",
                output_directory
            )));
        }

        Ok(OutputHandler {
            output_directory: output_directory.clone(),
            stdout_handler: BufferedStdoutHandler::new(),
            multi_counts_handler: MultiCountsHandler::default(),
            multi_average_handler: MultiAverageHandler::default(),
            file_handler: FileHandler::new(output_directory.clone(), aux_data),
        })
    }

    pub fn handle(&mut self, eff: &Effect) -> Result<(), OutputError> {
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
                        _ => Err(OutputError::UnrecognizedOutputType),
                    }
                } else {
                    Err(OutputError::UnrecognizedOutputType)
                }
            }
            _ => Err(OutputError::UnrecognizedOutputType),
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
