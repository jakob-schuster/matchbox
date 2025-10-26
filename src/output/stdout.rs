//! Write to stdout.

use std::io::{BufWriter, StdoutLock, Write};

use crate::{
    core::PortableVal,
    output::{stdout, OutputError},
};

pub struct BufferedStdoutHandler<'a> {
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

    pub fn handle(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        self.vec.push(val.to_string());

        if self.vec.len() > self.buffer_size {
            self.stdout.write_all(self.vec.join("\n").as_bytes());
            // print one more newline following,
            // so that the next 1M will be concatenated correctly
            self.stdout.write_all(b"\n");
            self.vec = vec![];
        }

        Ok(())
    }

    pub fn finish(&mut self) {
        self.stdout.write_all(self.vec.join("\n").as_bytes());
        // print one more newline following,
        // so that the result of printing multiples of 1M values
        // is consistent with the result of printing any other amount
        self.stdout.write_all(b"\n");
        self.vec = vec![];
    }

    pub fn summarize(&self) -> StdoutHandlerSummary {
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

/// A summary of the current stdout status
#[derive(Clone)]
pub struct StdoutHandlerSummary {
    lines: Vec<String>,
}
