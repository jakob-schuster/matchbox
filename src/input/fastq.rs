//! Handle FASTQ reading with Rust Bio.

use std::{collections::HashMap, io::BufRead, sync::Arc};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        eval::env::Env,
        rec::{self, FullyConcreteRec, Rec2},
        Effect, Val,
    },
    input::{ExecError, InputError, Progress, ProgressSummary, Reader},
    output::OutputHandler,
    util::{location::Location, recfield::CoreRecField},
};

pub struct FastqReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl FastqReader {
    pub fn new(buffer: Box<dyn BufRead>) -> FastqReader {
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
                CoreRecField {
                    name: b"qual".to_vec(),
                    data: Val::StrTy,
                },
            ],
        };

        FastqReader { buffer, ty }
    }
}

impl Reader for FastqReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

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
                                rec: rec::Rec2::FastqRead {
                                    r: rec::FastqRead { read },
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
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}

pub struct PairedFastqReader {
    buffer: Box<dyn BufRead>,
    paired_buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl PairedFastqReader {
    pub fn new(buffer: Box<dyn BufRead>, paired_buffer: Box<dyn BufRead>) -> PairedFastqReader {
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
                            CoreRecField {
                                name: b"qual".to_vec(),
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
                            CoreRecField {
                                name: b"qual".to_vec(),
                                data: Val::StrTy,
                            },
                        ],
                    },
                },
            ],
        };

        PairedFastqReader {
            buffer,
            paired_buffer,
            ty,
        }
    }
}

impl Reader for PairedFastqReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer)
            .records()
            .zip(bio::io::fastq::Reader::from_bufread(&mut self.paired_buffer).records());

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
                                rec: Rec2::FullyConcrete {
                                    r: FullyConcreteRec {
                                        map: HashMap::from([
                                            (
                                                b"r1".to_vec(),
                                                core::Val::Rec {
                                                    rec: Rec2::FastqRead {
                                                        r: rec::FastqRead { read },
                                                    },
                                                },
                                            ),
                                            (
                                                b"r2".to_vec(),
                                                core::Val::Rec {
                                                    rec: Rec2::FastqRead {
                                                        r: rec::FastqRead { read: paired_read },
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
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}

pub struct RevCompFastqReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl RevCompFastqReader {
    pub fn new(buffer: Box<dyn BufRead>) -> RevCompFastqReader {
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
                CoreRecField {
                    name: b"qual".to_vec(),
                    data: Val::StrTy,
                },
            ],
        };

        RevCompFastqReader { buffer, ty }
    }
}

impl Reader for RevCompFastqReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

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
                                rec: Rec2::FastqRead {
                                    r: rec::FastqRead { read },
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
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}

pub struct FastqDebugReader {
    buffer: Box<dyn BufRead>,
    ty: Val<'static>,
}

impl FastqDebugReader {
    pub fn new(buffer: Box<dyn BufRead>) -> FastqDebugReader {
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
                CoreRecField {
                    name: b"qual".to_vec(),
                    data: Val::StrTy,
                },
            ],
        };

        FastqDebugReader { buffer, ty }
    }
}

impl Reader for FastqDebugReader {
    fn map<'p, 'q>(
        &mut self,
        prog: &'q core::Prog<'p>,
        global: &'q Env<core::Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        let final_progress = input_records
            .into_iter()
            .chunks(10000)
            .into_iter()
            .try_fold(progress, |progress0, chunk| {
                let vec: Vec<Result<Vec<Effect>, ExecError>> = chunk
                    .collect_vec()
                    .iter()
                    .map(|record| match record {
                        Ok(read) => {
                            let val = core::Val::Rec {
                                rec: Rec2::FastqRead {
                                    r: rec::FastqRead { read },
                                },
                            };
                            prog.eval2(global, &Env::default(), val)
                                .map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_vec();

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
        let input_records = bio::io::fastq::Reader::from_bufread(&mut self.buffer).records();

        input_records.count()
    }

    fn get_ty<'a>(&'a self) -> Val<'a> {
        self.ty.reference()
    }
}
