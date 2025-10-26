//! Write to sequence alignment map (SAM) files.

use std::{fs::File, io::Write};

use noodles::sam::header::record::value::{map::tag, Map};

use crate::{
    core::PortableVal,
    input::AuxiliaryInputData,
    output::{file::FileWriter, OutputError},
    util::bytes_to_string,
};

pub struct SamWriter {
    writer: noodles::sam::io::Writer<File>,
}

impl SamWriter {
    pub fn new(filename: &str, aux_data: &AuxiliaryInputData) -> Result<SamWriter, OutputError> {
        let mut header = match aux_data {
            // no header, so just construct a new empty one
            AuxiliaryInputData::None => noodles::sam::Header::default(),
            AuxiliaryInputData::SAMHeader { header } => header.clone(),
        };

        header
            .programs_mut()
            .add(
                "matchbox",
                Map::builder()
                    .insert(tag::Other::try_from([b'P', b'N']).unwrap(), b"matchbox")
                    .insert(
                        tag::Other::try_from([b'V', b'N']).unwrap(),
                        env!("CARGO_PKG_VERSION"),
                    )
                    .insert(
                        tag::Other::try_from([b'C', b'L']).unwrap(),
                        std::env::args().collect::<Vec<_>>().join(" "),
                    )
                    .build()
                    .unwrap(),
            )
            .map_err(|_| OutputError::Create {
                filename: filename.to_string(),
            })?;

        let file = File::create(filename).map_err(|_| OutputError::Create {
            filename: filename.to_string(),
        })?;

        let mut writer = noodles::sam::io::Writer::new(file);
        writer
            .write_header(&header)
            .map_err(|_| OutputError::Write)?;

        Ok(SamWriter { writer })
    }
}

impl FileWriter for SamWriter {
    fn write(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
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
                    self.writer
                        .get_mut()
                        .write_all(
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
                            )
                            .as_bytes(),
                        )
                        .expect("Couldn't write to file!");

                    Ok(())
                } else {
                    Err(OutputError::Type { val: val.clone() })
                }
            }
            _ => Err(OutputError::Type { val: val.clone() }),
        }
    }

    fn finish(&mut self) -> Result<(), OutputError> {
        self.writer
            .get_mut()
            .flush()
            .map_err(|_| OutputError::Flush)
    }
}
