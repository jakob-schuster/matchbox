use std::fmt::Display;

use crate::core::{self, library, StrVal, Val};
use crate::surface::elab::ElabError;
use crate::util::location::Location;
use crate::util::{env::Env, Arena};

/// An elaboration context. Contains a list of names associated with types.
#[derive(Clone, Default, Debug)]
pub struct Context<'a> {
    pub size: usize,
    pub names: Env<String>,
    pub tys: Env<core::Val<'a>>,
    pub tms: Env<core::Val<'a>>,
}

impl<'a> Context<'a> {
    /// Returns the next variable that will be bound in the context after
    /// calling bind_def or bind_param
    pub fn next_var(&self, arena: &'a Arena) -> core::Val<'a> {
        core::Val::Neutral {
            neutral: core::Neutral::Var { level: self.size },
        }
    }

    /// Binds a definition in the context
    pub fn bind_def(&self, name: String, ty: core::Val<'a>, tm: core::Val<'a>) -> Context<'a> {
        Context {
            size: self.size + 1,
            names: self.names.with(name),
            tys: self.tys.with(ty),
            tms: self.tms.with(tm),
        }
    }

    pub fn unbind_def(&self, name: &str) -> Context<'a> {
        // name must be present
        let (index, _) = self
            .names
            .iter()
            .find_position(|name0| (*name0).eq(name))
            .unwrap();

        Context {
            size: self.size - 1,
            names: self.names.without(index),
            tys: self.tys.without(index),
            tms: self.tms.without(index),
        }
    }

    /// Binds a parameter in the context
    pub fn bind_param(&self, name: String, ty: core::Val<'a>, arena: &'a Arena) -> Context<'a> {
        self.bind_def(name, ty, self.next_var(arena))
    }

    /// Looks up a name in the context
    pub fn lookup(&self, name: String) -> Option<(usize, &core::Val<'a>)> {
        // Find the index of most recent binding in the context identified by
        // name, starting from the most recent binding. This gives us the
        // de Bruijn index of the variable.
        let index = self.size - 1 - self.names.find_last(&name)?;
        let val = self.tys.get_index(index);

        Some((index, val))
    }

    pub fn bind_read_from_reader(&self, arena: &'a Arena, ty: Val<'a>) -> Context<'a> {
        self.bind_param("read".to_string(), ty, arena)
    }

    /// Binds the read to a parameter in the context
    pub fn bind_read(&self, arena: &'a Arena, filename: String) -> Context<'a> {
        // filetype must be allocated since it must survive
        let filename_bytes = arena.alloc(filename.to_string().as_bytes().to_vec());

        self.bind_param(
            "read".to_string(),
            library::read_ty(
                arena,
                &Location::new(0, 0),
                &[crate::core::Val::Str {
                    s: StrVal::new_reference(filename_bytes),
                }],
            )
            .unwrap(),
            arena,
        )
    }

    /// Binds the read to a parameter in the context,
    /// taking into account an optional read pair.
    pub fn bind_read_paired(
        &self,
        arena: &'a Arena,
        filename: String,
        paired_filetype: &Option<String>,
    ) -> Context<'a> {
        match paired_filetype {
            Some(paired_filetype) => {
                // filetype must be allocated since it must survive
                let filetype_bytes = arena.alloc(filename.to_string().as_bytes().to_vec());
                let paired_filetype_bytes =
                    arena.alloc(paired_filetype.to_string().as_bytes().to_vec());

                self.bind_param(
                    "read".to_string(),
                    core::library::paired_read_ty(
                        arena,
                        &Location::new(0, 0),
                        &[
                            core::Val::Str {
                                s: StrVal::new_reference(filetype_bytes),
                            },
                            core::Val::Str {
                                s: StrVal::new_reference(paired_filetype_bytes),
                            },
                        ],
                    )
                    .unwrap(),
                    arena,
                )
            }
            None => self.bind_read(arena, filename),
        }
    }

    /// Asserts that a name is unique.
    pub fn assert_unique_name(&self, location: &Location, name: &String) -> Result<(), ElabError> {
        // check that the name isn't already assigned
        if self.names.iter().contains(name) {
            Err(ElabError::new(
                &location,
                &format!(
                    "tried to name a variable '{}', but that name is already in use!",
                    name
                ),
            ))
        } else {
            Ok(())
        }
    }
}

impl<'a> Display for Context<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (0..self.size)
            .map(|i| {
                format!(
                    "{}: {} = {}",
                    self.names.get_index(i),
                    self.tys.get_index(i),
                    self.tms.get_index(i)
                )
            })
            .join("\n")
            .fmt(f)
    }
}
