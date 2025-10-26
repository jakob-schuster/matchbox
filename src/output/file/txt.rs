//! Write to text files.

use std::{
    fs::File,
    io::{BufWriter, Write},
};

use crate::{
    core::PortableVal,
    output::{file::FileWriter, OutputError},
};
pub struct TxtWriter {
    writer: BufWriter<File>,
}

impl TxtWriter {
    pub fn new(filename: &str) -> Result<TxtWriter, OutputError> {
        let file = File::create(filename).map_err(|_| OutputError::Create {
            filename: filename.to_string(),
        })?;
        let writer = BufWriter::new(file);

        Ok(TxtWriter { writer })
    }
}

impl FileWriter for TxtWriter {
    fn write(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
            PortableVal::Str { s } => {
                self.writer.write_all(s);
                self.writer.write_all(b"\n");

                Ok(())
            }
            _ => Err(OutputError::Type { val: val.clone() }),
        }
    }

    fn finish(&mut self) -> Result<(), OutputError> {
        self.writer.flush().map_err(|_| OutputError::Flush)
    }
}
