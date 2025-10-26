//! Handle sequence alignment map (SAM) reading with noodles.

use std::{collections::HashMap, io::BufRead, sync::Arc};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        rec::{self, FullyConcreteRec},
        Effect, Val,
    },
    input::{AuxiliaryInputData, ExecError, Input, InputError, Progress, ProgressSummary, Reader},
    output::OutputHandler,
    util::{Arena, Cache, CoreRecField, Env},
};
pub struct SamReader {
    reader: noodles::sam::io::Reader<Box<dyn BufRead>>,
    header: noodles::sam::Header,
    ty: Val<'static>,
}

impl SamReader {
    pub fn new(buffer: Box<dyn BufRead>) -> Result<SamReader, InputError> {
        let mut reader = noodles::sam::io::Reader::new(buffer);
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

        Ok(SamReader { ty, reader, header })
    }
}

impl Reader for SamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
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
                            let cigar = read.cigar();
                            let seq = read.sequence();
                            let qual = read.quality_scores();
                            let data = read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(rec::SamRead {
                                    read: &read,
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

pub struct PairedSamReader {
    reader: noodles::sam::io::Reader<Box<dyn BufRead>>,
    header: noodles::sam::Header,

    paired_reader: noodles::sam::io::Reader<Box<dyn BufRead>>,
    paired_header: noodles::sam::Header,

    ty: Val<'static>,
}

impl PairedSamReader {
    pub fn new(
        buffer: Box<dyn BufRead>,
        paired_buffer: Box<dyn BufRead>,
    ) -> Result<PairedSamReader, InputError> {
        let mut reader = noodles::sam::io::Reader::new(buffer);
        let header = reader.read_header().map_err(|e| InputError::Header)?;

        let mut paired_reader = noodles::sam::io::Reader::new(paired_buffer);
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

        Ok(PairedSamReader {
            reader,
            header,
            paired_reader,
            paired_header,
            ty,
        })
    }
}

impl Reader for PairedSamReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
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

                            let cigar = read.cigar();
                            let paired_cigar = paired_read.cigar();

                            let seq = read.sequence();
                            let paired_seq = paired_read.sequence();

                            let qual = read.quality_scores();
                            let paired_qual = paired_read.quality_scores();

                            let data = read.data();
                            let paired_data = paired_read.data();

                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::SamRead {
                                                    read,
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
                                                rec: Arc::new(rec::SamRead {
                                                    read: paired_read,
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

    fn get_aux_data(&self) -> AuxiliaryInputData {
        // for now, just report the header of the first file...
        AuxiliaryInputData::SAMHeader {
            header: self.header.clone(),
        }
    }
}
