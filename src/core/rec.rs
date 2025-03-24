use crate::util::{self, Arena, Location};
use std::{collections::HashMap, fmt::Display, rc::Rc};

use super::{EvalError, InternalError, Val};

#[derive(Debug)]
pub enum RecError {
    BadIndex(String),
}

pub trait Rec<'p: 'a, 'a>: Display + Send + Sync {
    fn get(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'p, 'a>, InternalError>;
    fn all(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'p, 'a>>;
    fn with(&self, key: &'a [u8], val: &'a Val<'p, 'a>, arena: &'a Arena) -> ConcreteRec<'p, 'a> {
        let mut map = self.all(arena);
        map.insert(key, val);

        ConcreteRec { map }
    }
    fn with_all(
        &self,
        entries: &[(&'a [u8], &'a Val<'p, 'a>)],
        arena: &'a Arena,
    ) -> ConcreteRec<'p, 'a> {
        let mut map = self.all(arena);

        for (key, val) in entries {
            map.insert(key, val);
        }

        ConcreteRec { map }
    }
}

pub struct ConcreteRec<'p: 'a, 'a> {
    pub map: HashMap<&'a [u8], &'a Val<'p, 'a>>,
}

impl<'p: 'a, 'a> Rec<'p, 'a> for ConcreteRec<'p, 'a> {
    fn get(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'p, 'a>, InternalError> {
        match self.map.get(key) {
            Some(val) => Ok(val),
            None => Err(InternalError {
                message: format!(
                    "could not find field '{}'",
                    String::from_utf8(key.to_vec()).expect("bad field name?!")
                ),
            }),
        }
    }

    fn all(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'p, 'a>> {
        self.map.clone()
    }
}

impl<'p: 'a, 'a> Display for ConcreteRec<'p, 'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format!(
            "{{ {} }}",
            self.map
                .iter()
                .map(|(name, val)| format!("{} = {}", util::bytes_to_string(name).unwrap(), val))
                .collect::<Vec<_>>()
                .join(", ")
        )
        .fmt(f)
    }
}

pub struct FastaRead {
    pub read: bio::io::fasta::Record,
}

impl<'p: 'a, 'a> Rec<'p, 'a> for FastaRead {
    fn get(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'p, 'a>, InternalError> {
        match key {
            b"seq" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.seq().to_vec()),
            })),
            b"id" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.id().as_bytes().to_vec()),
            })),
            b"desc" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.desc().unwrap_or_default().as_bytes().to_vec()),
            })),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'p, 'a>> {
        let mut map: HashMap<&'a [u8], &'a Val<'p, 'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl Display for FastaRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct FastqRead {
    pub read: bio::io::fastq::Record,
}

impl<'p: 'a, 'a> Rec<'p, 'a> for FastqRead {
    fn get(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'p, 'a>, InternalError> {
        match key {
            b"seq" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.seq().to_vec()),
            })),
            b"id" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.id().as_bytes().to_vec()),
            })),
            b"desc" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.desc().unwrap_or_default().as_bytes().to_vec()),
            })),
            b"qual" => Ok(arena.alloc(Val::Str {
                s: arena.alloc(self.read.desc().unwrap_or_default().as_bytes().to_vec()),
            })),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'p, 'a>> {
        let mut map: HashMap<&'a [u8], &'a Val<'p, 'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc", b"qual"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl Display for FastqRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
