use std::{collections::HashMap, io::BufRead, sync::Arc};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        rec::{self, FullyConcreteRec},
        Effect, Val,
    },
    output::OutputHandler,
    input::{ExecError, InputError, Progress, ProgressSummary, Reader},
    util::{Arena, Cache, CoreRecField, Env},
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
                    name: b"seq",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"id",
                    data: Val::StrTy,
                },
                CoreRecField {
                    name: b"desc",
                    data: Val::StrTy,
                },
            ],
        };

        FastaReader { buffer, ty }
    }
}

impl Reader for FastaReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
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
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(rec::FastaRead { read }),
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
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
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
                    name: b"r1",
                    data: Val::RecTy {
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
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
                        fields: vec![
                            CoreRecField {
                                name: b"seq",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"id",
                                data: Val::StrTy,
                            },
                            CoreRecField {
                                name: b"desc",
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
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
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
                            let arena = Arena::new();
                            let val = core::Val::Rec {
                                rec: Arc::new(FullyConcreteRec {
                                    map: HashMap::from([
                                        (
                                            b"r1".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastaRead { read }),
                                            },
                                        ),
                                        (
                                            b"r2".to_vec(),
                                            core::Val::Rec {
                                                rec: Arc::new(rec::FastaRead { read: paired_read }),
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
        let input_records = bio::io::fasta::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&self, arena: &'a Arena) -> Val<'a> {
        self.ty.coerce()
    }
}
