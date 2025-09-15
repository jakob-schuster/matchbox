use std::{collections::HashMap, io::BufRead, sync::Arc};

use itertools::Itertools;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{
        self,
        rec::{self, CSVHeader, FullyConcreteRec},
        Effect, Val,
    },
    output::OutputHandler,
    read::{ExecError, InputError, Progress, ProgressSummary, Reader},
    util::{Arena, Cache, CoreRecField, Env},
};

pub struct DSVReader {
    reader: csv::Reader<Box<dyn BufRead>>,
    field_names: Vec<String>,
}

impl DSVReader {
    pub fn new(buffer: Box<dyn BufRead>, delimiter: u8) -> DSVReader {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_reader(buffer);
        let header = reader.headers();

        let field_names = header
            .iter()
            .flat_map(|a| a.iter().map(String::from))
            .collect::<Vec<_>>();

        println!("{}", field_names.join(","));

        // let fields = field_names
        //     .iter()
        //     .map(|name| CoreRecField::new(name.as_bytes(), Val::StrTy))
        //     .collect::<Vec<_>>();

        // let ty = Val::RecTy { fields };

        DSVReader {
            reader,
            field_names,
        }
    }
}

impl Reader for DSVReader {
    fn map<'p>(
        &mut self,
        prog: &core::Prog<'p>,
        env: &Env<Val<'p>>,
        cache: &Cache<Val<'p>>,
        output_handler: &mut OutputHandler,
        progress: &mut dyn Progress,
    ) -> Result<(), ExecError> {
        let header = CSVHeader::new(self.reader.byte_headers().unwrap());

        let input_records = self.reader.byte_records();
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
                                rec: Arc::new(rec::CSVRead {
                                    header: &header,
                                    fields: read,
                                }),
                            };
                            prog.eval(&arena, env, cache, val).map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).unwrap();
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
        let fields = self
            .field_names
            .iter()
            .map(|field_name| {
                CoreRecField::new(arena.alloc(field_name.as_bytes().to_vec()), Val::StrTy)
            })
            .collect();

        Val::RecTy { fields }
    }
}
