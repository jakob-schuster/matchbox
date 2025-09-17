use std::fs::File;

use crate::{
    core::PortableVal,
    output::{file::FileWriter, OutputError},
};

pub struct FastqWriter {
    writer: bio::io::fastq::Writer<File>,
}

impl FastqWriter {
    pub fn new(filename: &str) -> Result<FastqWriter, OutputError> {
        let file = File::create(filename).map_err(|_| OutputError::Create {
            filename: filename.to_string(),
        })?;
        let writer = bio::io::fastq::Writer::new(file);

        Ok(FastqWriter { writer })
    }
}

impl FileWriter for FastqWriter {
    fn write(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
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
                    self.writer
                        .write(
                            str::from_utf8(id).unwrap(),
                            Some(str::from_utf8(desc).unwrap()),
                            seq,
                            qual,
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
