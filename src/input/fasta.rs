//! Handle FASTA reading with Rust Bio.

use std::{collections::HashMap, io::BufRead, sync::Arc};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        eval::env::Env,
        rec::{self, ConcreteRec, FastaRead, FullyConcreteRec},
        Effect, Val,
    },
    input::{ExecError, InputError, Progress, ProgressSummary, Reader},
    output::OutputHandler,
    util::{location::Location, recfield::CoreRecField},
};

pub struct FastaReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl FastaReader {
    pub fn new(buffer: Box<dyn BufRead>) -> FastaReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"seq".to_vec(),
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id".to_vec(),
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"desc".to_vec(),
                    data: Val::StrTy,
                },
            ],
        };

        FastaReader { buffer, ty }
    }
}

impl Reader for FastaReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

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
                            let val = core::Val::Rec {
                                rec: rec::RecVal::FastaRead {
                                    r: rec::FastaRead { read },
                                },
                            };

                            prog.eval2(global, &Env::default(), val)
                                .map_err(ExecError::Eval)
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
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}

pub struct PairedFastaReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedFastaReader {
    pub fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedFastaReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"r1".to_vec(),
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq".to_vec(),
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id".to_vec(),
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc".to_vec(),
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
                CoreRecField {
                    name: b"r2".to_vec(),
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq".to_vec(),
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id".to_vec(),
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc".to_vec(),
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedFastaReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedFastaReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fasta::Reader::from_bufread(&mut self.paired_buffer).records());

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
                            let val = core::Val::Rec {
                                rec: rec::RecVal::FullyConcrete {
                                    r: FullyConcreteRec {
                                        map: HashMap::from([
                                            (
                                                b"r1".to_vec(),
                                                core::Val::Rec {
                                                    rec: rec::RecVal::FastaRead {
                                                        r: FastaRead { read },
                                                    },
                                                },
                                            ),
                                            (
                                                b"r2".to_vec(),
                                                core::Val::Rec {
                                                    rec: rec::RecVal::FastaRead {
                                                        r: FastaRead { read: paired_read },
                                                    },
                                                },
                                            ),
                                        ]),
                                    },
                                },
                            };

                            prog.eval2(global, &Env::default(), val)
                                .map_err(ExecError::Eval)
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
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}

pub struct RevCompFastaReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl RevCompFastaReader {
    pub fn new(buffer: Box<dyn BufRead>) -> RevCompFastaReader {
        let ty = Val::RecTy {
            fields: vec![
                CoreRecField {
                    name: b"seq".to_vec(),
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id".to_vec(),
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"desc".to_vec(),
                    data: Val::StrTy,
                },
            ],
        };

        RevCompFastaReader { buffer, ty }
    }
}

impl Reader for RevCompFastaReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

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
                            let val = core::Val::Rec {
                                rec: rec::RecVal::FastaRead {
                                    r: FastaRead { read },
                                },
                            };

                            let reverse_complement_val =
                                core::library::unary_read_reverse_complement(
                                    &Location::new(0, 0),
                                    &vec![val.clone()],
                                )
                                // should never happen
                                .unwrap();
                            let effs_forward = prog
                                .eval2(global, &Env::default(), val)
                                .map_err(ExecError::Eval)?;
                            let effs_reverse = prog
                                .eval2(global, &Env::default(), reverse_complement_val)
                                .map_err(ExecError::Eval)?;

                            Ok(effs_forward.into_iter().chain(effs_reverse).collect_vec())
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
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}
