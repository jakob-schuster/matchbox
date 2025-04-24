use crate::util::{self, Arena, Location};
use std::{collections::HashMap, fmt::Display, rc::Rc};

use super::{EvalError, InternalError, Val};

#[derive(Debug)]
pub enum RecError {
    BadIndex(String),
}

pub trait Rec<'p>: Display + Send + Sync {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'a>, InternalError>
    where
        'p: 'a;
    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'a>>
    where
        'p: 'a;
    fn with<'a>(&self, key: &'a [u8], val: &'a Val<'a>, arena: &'a Arena) -> ConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all(arena);
        map.insert(key, val);

        ConcreteRec { map }
    }
    fn with_all<'a>(&self, entries: &[(&'a [u8], &'a Val<'a>)], arena: &'a Arena) -> ConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all(arena);

        for (key, val) in entries {
            map.insert(key, val);
        }

        ConcreteRec { map }
    }

    fn coerce<'a>(&self, arena: &'a Arena) -> &'a dyn Rec<'a>
    where
        'p: 'a,
    {
        arena.alloc(ConcreteRec {
            map: self.all(arena),
        })
    }

    fn is_neutral(&self) -> bool {
        // this is a bit unhinged but we can just make a quick arena for this,
        // since we're just returning a bool out
        let arena = Arena::new();
        self.all(&arena).iter().any(|(_, val)| val.is_neutral())
    }
}

pub struct ConcreteRec<'p> {
    pub map: HashMap<&'p [u8], &'p Val<'p>>,
}

impl<'p> Rec<'p> for ConcreteRec<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match self.map.get(key) {
            Some(val) => Ok(arena.alloc(val.coerce(arena))),
            None => Err(InternalError {
                message: format!(
                    "could not find field '{}'",
                    String::from_utf8(key.to_vec()).expect("bad field name?!")
                ),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'a>>
    where
        'p: 'a,
    {
        self.map
            .iter()
            .map(|(a, b)| {
                (
                    arena.alloc(a.to_vec()) as &'a [u8],
                    arena.alloc(b.coerce(arena)) as &'a Val<'a>,
                )
            })
            .collect()
    }
}

impl<'a> Display for ConcreteRec<'a> {
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

pub struct FastaRead<'a> {
    pub read: &'a bio::io::fasta::Record,
}

impl<'p> Rec<'p> for FastaRead<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match key {
            b"seq" => Ok(arena.alloc(Val::Str { s: self.read.seq() })),
            b"id" => Ok(arena.alloc(Val::Str {
                s: self.read.id().as_bytes(),
            })),
            b"desc" => Ok(arena.alloc(Val::Str {
                s: self.read.desc().unwrap_or_default().as_bytes(),
            })),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<&'a [u8], &'a Val<'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'a> Display for FastaRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // note: this seems highly wasteful
        let arena = Arena::new();
        let map = self.all(&arena);
        ConcreteRec { map }.fmt(f)
    }
}

pub struct FastqRead<'a> {
    pub read: &'a bio::io::fastq::Record,
}

impl<'p> Rec<'p> for FastqRead<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<&'a Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match key {
            b"seq" => Ok(arena.alloc(Val::Str { s: self.read.seq() })),
            b"id" => Ok(arena.alloc(Val::Str {
                s: self.read.id().as_bytes(),
            })),
            b"desc" => Ok(arena.alloc(Val::Str {
                s: self.read.desc().unwrap_or_default().as_bytes(),
            })),
            b"qual" => Ok(arena.alloc(Val::Str {
                s: self.read.desc().unwrap_or_default().as_bytes(),
            })),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], &'a Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<&'a [u8], &'a Val<'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc", b"qual"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'a> Display for FastqRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
