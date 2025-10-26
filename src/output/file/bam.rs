//! Write to binary alignment map (BAM) files.

use std::fs::File;

use noodles::sam::{
    self,
    header::record::value::{map::tag, Map},
};

use crate::{
    core::PortableVal,
    input::AuxiliaryInputData,
    output::{file::FileWriter, OutputError},
    util::bytes_to_string,
};

pub struct SamOrBamWriter {
    writer: noodles_util::alignment::io::writer::Writer,
    header: noodles::sam::Header,
}

impl SamOrBamWriter {
    pub fn new(
        filename: &str,
        aux_data: &AuxiliaryInputData,
        format: noodles_util::alignment::io::Format,
    ) -> Result<SamOrBamWriter, OutputError> {
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

        let mut writer = noodles_util::alignment::io::writer::Builder::default()
            .set_format(format)
            .build_from_writer(file)
            .map_err(|_| OutputError::Create {
                filename: filename.to_string(),
            })?;
        writer
            .write_header(&header)
            .map_err(|_| OutputError::Other {
                message: String::from("header"),
            })?;

        Ok(SamOrBamWriter { writer, header })
    }
}

impl FileWriter for SamOrBamWriter {
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
                    // shockingly inefficient - first, format the record as a string
                    let string_record = format!(
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
                    );

                    // then, convert it to a SAM record
                    let record = noodles_util::alignment::Record::Sam(
                        sam::Record::try_from(string_record.as_bytes())
                            .map_err(|_| OutputError::Type { val: val.clone() })?,
                    );

                    // then, write it out via an agnostic writer
                    self.writer
                        .write_record(&self.header, &record)
                        .map_err(|e| match e.kind() {
                            std::io::ErrorKind::InvalidData => match e.to_string().as_str() {
                                "invalid field terminator" => OutputError::BadSAMTag,
                                _ => OutputError::Write,
                            },
                            _ => OutputError::Write,
                        })?;

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
            .finish(&self.header)
            .map_err(|_| OutputError::Flush)
    }
}
