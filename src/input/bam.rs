use std::{collections::HashMap, fmt::Pointer, io::BufRead, sync::Arc};

use itertools::Itertools;
use noodles::sam::io::writer::record::write_cigar;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        rec::{self, FullyConcreteRec},
        Effect, InternalError, Val,
    },
    output::OutputHandler,
    input::{AuxiliaryInputData, ExecError, Input, InputError, Progress, ProgressSummary, Reader},
    util::{Arena, Cache, CoreRecField, Env},
};

pub struct BamReader {
    reader: noodles::bam::io::Reader<noodles::bgzf::io::Reader<Box<dyn BufRead>>>,
    header: noodles::sam::Header,
    ty: Val<'static>,
}

impl BamReader {
    pub fn new(buffer: Box<dyn BufRead>) -> Result<BamReader, InputError> {
        let mut reader = noodles::bam::io::Reader::new(buffer);
        let header = reader.read_header().map_err(|e| InputError::Header)?;

        let ty = Val::RecTy {
            // in the order as described in the spec
            fields: vec![
                // Query template name
                CoreRecField {
                    name: b"qname",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                // Bitwise flag
                CoreRecField {
                    name: b"flag",
                    data: Val::NumTy,
                },
                // Reference sequence name
                CoreRecField {
                    name: b"rname",
                    data: Val::StrTy,
                },
                // 1-based leftmost mapping position
                CoreRecField {
                    name: b"pos",
                    data: Val::NumTy,
                },
                // Mapping quality
                CoreRecField {
                    name: b"mapq",
                    data: Val::NumTy,
                },
                // The CIGAR string
                CoreRecField {
                    name: b"cigar",
                    data: Val::StrTy,
                },
                // Reference name of the mate / next read
                CoreRecField {
                    name: b"rnext",
                    data: Val::StrTy,
                },
                // Position of the mate / next read
                CoreRecField {
                    name: b"pnext",
                    data: Val::NumTy,
                },
                // Observed template length
                CoreRecField {
                    name: b"tlen",
                    data: Val::NumTy,
                },
                // The actual sequence of the alignment
                CoreRecField {
                    name: b"seq",
                    data: Val::StrTy,
                },
                // The quality string of the alignment
                CoreRecField {
                    name: b"qual",
                    data: Val::StrTy,
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"tags",
                    data: Val::StrTy,
                    // data: Val::ListTy {
                    //     ty: Arc::new(Val::StrTy),
                    // },
                },
                // The optional tags associated with the alignment
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
            ],
        };

        Ok(BamReader { reader, header, ty })
    }
}

impl Reader for BamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let reference_sequences = self
            .header
            .reference_sequences()
            .keys()
            .map(|rname| rname.to_vec())
            .collect_vec();

        let input_records = self.reader.records();
        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let arena = Arena::new();
                            let mut cigar = Vec::new();
                            write_cigar(&mut cigar, &read.cigar());
                            let seq = read.sequence().iter().collect_vec();
                            let qual = read
                                .quality_scores()
                                .as_ref()
                                .iter()
                                .map(|a| a + 33)
                                .collect_vec();
                            let data = data_to_vec(&read.data()).map_err(ExecError::Input)?;

                            let rname = match read.reference_sequence_id() {
                                Some(id) => match reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let mate_rname = match read.mate_reference_sequence_id() {
                                Some(id) => match reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let val = core::Val::Rec {
                                rec: Arc::new(rec::BamRead {
                                    read: &read,
                                    rname,
                                    mate_rname,
                                    cigar: &cigar,
                                    seq: &seq,
                                    qual: &qual,
                                    data: &data,
                                }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).map_err(ExecError::Output)?;
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();

        Ok(())
    }

    fn count(&mut self) -> usize {
        let input_records = self.reader.records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }

    fn get_aux_data(&self) -> super::AuxiliaryInputData {
        AuxiliaryInputData::SAMHeader {
            header: self.header.clone(),
        }
    }
}

pub struct PairedBamReader {
    reader: noodles::bam::io::Reader<noodles::bgzf::io::Reader<Box<dyn BufRead>>>,
    header: noodles::sam::Header,

    paired_reader: noodles::bam::io::Reader<noodles::bgzf::io::Reader<Box<dyn BufRead>>>,
    paired_header: noodles::sam::Header,
    ty: Val<'static>,
}

impl PairedBamReader {
    pub fn new(
        buffer: Box<dyn BufRead>,
        paired_buffer: Box<dyn BufRead>,
    ) -> Result<PairedBamReader, InputError> {
        let mut reader = noodles::bam::io::Reader::new(buffer);
        let header = reader.read_header().map_err(|e| InputError::Header)?;

        let mut paired_reader = noodles::bam::io::Reader::new(paired_buffer);
        let paired_header = reader.read_header().map_err(|e| InputError::Header)?;

        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2",
                    data: Val::RecTy {
                        // in the order as described in the spec
                        fields: vec![
                            // Query template name
                            CoreRecField {
                                name: b"qname",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            // Bitwise flag
                            CoreRecField {
                                name: b"flag",
                                data: Val::NumTy,
                            },
                            // Reference sequence name
                            CoreRecField {
                                name: b"rname",
                                data: Val::StrTy,
                            },
                            // 1-based leftmost mapping position
                            CoreRecField {
                                name: b"pos",
                                data: Val::NumTy,
                            },
                            // Mapping quality
                            CoreRecField {
                                name: b"mapq",
                                data: Val::NumTy,
                            },
                            // The CIGAR string
                            CoreRecField {
                                name: b"cigar",
                                data: Val::StrTy,
                            },
                            // Reference name of the mate / next read
                            CoreRecField {
                                name: b"rnext",
                                data: Val::StrTy,
                            },
                            // Position of the mate / next read
                            CoreRecField {
                                name: b"pnext",
                                data: Val::NumTy,
                            },
                            // Observed template length
                            CoreRecField {
                                name: b"tlen",
                                data: Val::NumTy,
                            },
                            // The actual sequence of the alignment
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            // The quality string of the alignment
                            CoreRecField {
                                name: b"qual",
                                data: Val::StrTy,
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"tags",
                                data: Val::StrTy,
                                // data: Val::ListTy {
                                //     ty: Arc::new(Val::StrTy),
                                // },
                            },
                            // The optional tags associated with the alignment
                            CoreRecField {
                                name: b"desc",
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        Ok(PairedBamReader {
            reader,
            header,
            paired_reader,
            paired_header,
            ty,
        })
    }
}

impl Reader for PairedBamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let reference_sequences = self
            .header
            .reference_sequences()
            .keys()
            .map(|rname| rname.to_vec())
            .collect_vec();
        let paired_reference_sequences = self
            .paired_header
            .reference_sequences()
            .keys()
            .map(|rname| rname.to_vec())
            .collect_vec();

        let input_records = self.reader.records().zip(self.paired_reader.records());

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];
                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|(record, paired_record)| match (record, paired_record) {
                        (Ok(read), Ok(paired_read)) => {
                            let arena = Arena::new();

                            let mut cigar = Vec::new();
                            write_cigar(&mut cigar, &read.cigar());
                            let mut paired_cigar = Vec::new();
                            write_cigar(&mut paired_cigar, &paired_read.cigar());

                            let seq = read.sequence().iter().collect_vec();
                            let paired_seq = paired_read.sequence().iter().collect_vec();

                            let qual = read
                                .quality_scores()
                                .as_ref()
                                .iter()
                                .map(|a| a + 33)
                                .collect_vec();
                            let paired_qual = paired_read
                                .quality_scores()
                                .as_ref()
                                .iter()
                                .map(|a| a + 33)
                                .collect_vec();

                            let data = data_to_vec(&read.data()).map_err(ExecError::Input)?;
                            let paired_data =
                                data_to_vec(&paired_read.data()).map_err(ExecError::Input)?;

                            let rname = match read.reference_sequence_id() {
                                Some(id) => match reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let mate_rname = match read.mate_reference_sequence_id() {
                                Some(id) => match reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let paired_rname = match paired_read.reference_sequence_id() {
                                Some(id) => match paired_reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let paired_mate_rname = match paired_read.mate_reference_sequence_id() {
                                Some(id) => match paired_reference_sequences
                                    .get(id.map_err(|e| ExecError::Input(InputError::Read))?)
                                {
                                    Some(name) => name,
                                    None => return Err(ExecError::Input(InputError::ReferenceID)),
                                },
                                // if there is no reference sequence ID, use the default '*'
                                None => &vec![b'*'],
                            };

                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::BamRead {
                                                    read,
                                                    rname,
                                                    mate_rname,
                                                    cigar: &cigar,
                                                    seq: &seq,
                                                    qual: &qual,
                                                    data: &data,
                                                }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::BamRead {
                                                    read: paired_read,
                                                    rname: paired_rname,
                                                    mate_rname: paired_mate_rname,
                                                    cigar: &paired_cigar,
                                                    seq: &paired_seq,
                                                    qual: &paired_qual,
                                                    data: &paired_data,
                                                }),
                                            },
                                        ),
                                    ]),
                                }),
                            };

                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        _ => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).map_err(ExecError::Output)?;
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            })?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        self.reader.records().count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }

    fn get_aux_data(&self) -> super::AuxiliaryInputData {
        // for now, just r1 header
        super::AuxiliaryInputData::SAMHeader {
            header: self.header.clone(),
        }
    }
}

pub fn data_to_vec(data: &noodles::bam::record::Data) -> Result<Vec<u8>, InputError> {
    let mut v = vec![];

    for result in data.iter() {
        // println!("{:?}", data);

        let (tag, value) = result.map_err(|_| InputError::BAMTag)?;

        // NOTE: all of the other integer size tags (c, C, s, S, I) are NO LONGER IN USE and will throw ERRORS!
        // all must be replaced with i.

        let (ty, value) = match value {
            noodles::sam::alignment::record::data::field::Value::Character(c) => (b'A', vec![c]),
            noodles::sam::alignment::record::data::field::Value::Int8(i) => {
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::UInt8(i) => {
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::Int16(i) => {
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::UInt16(i) => {
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::Int32(i) => {
                eprintln!("parsing an int32 i");
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::UInt32(i) => {
                (b'i', i.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::Float(f) => {
                (b'f', f.to_string().as_bytes().to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::String(bstr) => {
                (b'Z', bstr.to_vec())
            }
            noodles::sam::alignment::record::data::field::Value::Hex(bstr) => (b'H', bstr.to_vec()),
            noodles::sam::alignment::record::data::field::Value::Array(array) => (
                b'B',
                match array {
                    noodles::sam::alignment::record::data::field::value::Array::Int8(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "c".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::UInt8(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "C".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::Int16(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "s".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::UInt16(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "S".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::Int32(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "i".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::UInt32(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "I".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                    noodles::sam::alignment::record::data::field::value::Array::Float(values) => {
                        let mut a = values
                            .iter()
                            .map(|value| match value {
                                Ok(v) => Ok(v.to_string()),
                                Err(_) => Err(InputError::BAMArrayTag),
                            })
                            .collect::<Result<Vec<String>, InputError>>()?;
                        a.insert(0, "f".to_string());

                        a.join(",").as_bytes().to_vec()
                    }
                },
            ),
        };

        let tag_vec = tag
            .as_ref()
            .iter()
            .chain(b":")
            .chain(&[ty])
            .chain(b":")
            .chain(&value)
            .cloned()
            .collect_vec();

        v.push(tag_vec);
    }

    let c = v.iter().intersperse(&vec![b'\t']).cloned().concat();

    Ok(c)
}
