use std::io::{BufRead, Lines};

use itertools::{Itertools, PeekingNext};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    core::{eval2::env::Env, library::unary_read_reverse_complement, Effect, StrVal, Val},
    input::{ExecError, ProgressSummary, Reader},
    util::{location::Location, Arena},
};

pub struct ListReader {
    lines: Lines<Box<dyn BufRead>>,
}

impl ListReader {
    pub fn new(buffer: Box<dyn BufRead>) -> ListReader {
        let mut lines = buffer.lines();

        ListReader { lines }
    }
}

impl Reader for ListReader {
    fn map<'p>(
        &mut self,
        prog: &'p crate::core::Prog<'p>,
        global: &'p Env<crate::core::Val<'p>>,
        output_handler: &mut crate::output::OutputHandler,
        progress: &mut dyn super::Progress,
    ) -> Result<(), super::ExecError> {
        let final_progress = (&mut self.lines).chunks(10000).into_iter().try_fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];

                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|res| match res {
                        Ok(read) => {
                            let val = Val::Str {
                                s: StrVal::new_reference(read.as_bytes()),
                            };

                            prog.eval2(global, &Env::default(), val)
                                .map_err(ExecError::Eval)
                        }
                        Err(_) => Err(ExecError::Input(crate::input::InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).map_err(ExecError::Output)?;
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            },
        )?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        (&mut self.lines).count()
    }

    fn get_ty<'a>(&self) -> crate::core::Val<'a> {
        crate::core::Val::StrTy
    }
}

pub struct RevCompListReader {
    lines: Lines<Box<dyn BufRead>>,
}

impl RevCompListReader {
    pub fn new(buffer: Box<dyn BufRead>) -> ListReader {
        let mut lines = buffer.lines();

        ListReader { lines }
    }
}

impl Reader for RevCompListReader {
    fn map<'p>(
        &mut self,
        prog: &'p crate::core::Prog<'p>,
        global: &'p Env<crate::core::Val<'p>>,
        output_handler: &mut crate::output::OutputHandler,
        progress: &mut dyn super::Progress,
    ) -> Result<(), super::ExecError> {
        let final_progress = (&mut self.lines).chunks(10000).into_iter().try_fold(
            progress,
            |progress0, chunk| {
                let mut vec: Vec<Result<Vec<Effect>, ExecError>> = vec![];

                chunk
                    .collect_vec()
                    .par_iter()
                    .map(|res| match res {
                        Ok(read) => {
                            let val = Val::Str {
                                s: StrVal::new_reference(read.as_bytes()),
                            };
                            let reverse_complement_val = unary_read_reverse_complement(
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
                        Err(_) => Err(ExecError::Input(crate::input::InputError::Read)),
                    })
                    .collect_into_vec(&mut vec);

                for result_effects in &vec {
                    for effect in result_effects.as_ref().map_err(|e| e.clone())? {
                        output_handler.handle(effect).map_err(ExecError::Output)?;
                    }
                }

                progress0.update(&ProgressSummary::new(10000, output_handler.summarize()));
                Ok(progress0)
            },
        )?;

        final_progress.finish();
        output_handler.finish();
        Ok(())
    }

    fn count(&mut self) -> usize {
        (&mut self.lines).count()
    }

    fn get_ty<'a>(&self) -> crate::core::Val<'a> {
        crate::core::Val::StrTy
    }
}
