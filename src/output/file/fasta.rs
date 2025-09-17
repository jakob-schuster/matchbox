use std::fs::File;

use crate::{
    core::PortableVal,
    output::{file::FileWriter, OutputError},
};

pub struct FastaWriter {
    writer: bio::io::fasta::Writer<File>,
}

impl FastaWriter {
    pub fn new(filename: &str) -> Result<FastaWriter, OutputError> {
        let file = File::create(filename).map_err(|_| OutputError::Create {
            filename: filename.to_string(),
        })?;
        let writer = bio::io::fasta::Writer::new(file);

        Ok(FastaWriter { writer })
    }
}

impl FileWriter for FastaWriter {
    fn write(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
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
                    self.writer
                        .write(
                            str::from_utf8(id).unwrap(),
                            Some(str::from_utf8(desc).unwrap()),
                            seq,
                        )
                        .map_err(|_| OutputError::Write)?;

                    Ok(())
                } else {
                    Err(OutputError::Type { val: val.clone() })
                }
            }
            _ => Err(OutputError::Type { val: val.clone() }),
        }
    }

    fn finish(&mut self) -> Result<(), OutputError> {
        self.writer.flush().map_err(|_| OutputError::Flush)
    }
}
