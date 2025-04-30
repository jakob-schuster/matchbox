use noodles::sam::record::Sequence;

use crate::util::{self, Arena, CoreRecField, Location};
use std::{collections::HashMap, fmt::Display, rc::Rc, sync::Arc};

use super::{EvalError, InternalError, Val};

#[derive(Debug)]
pub enum RecError {
    BadIndex(String),
}

pub trait Rec<'p>: Display + Send + Sync {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a;
    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a;
    fn with<'a>(&self, key: &[u8], val: Val<'a>, arena: &'a Arena) -> ConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all(arena);
        map.insert(arena.alloc(key.to_vec()), val);

        ConcreteRec { map }
    }
    fn with_all<'a>(&self, entries: &[(&[u8], Val<'a>)], arena: &'a Arena) -> ConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all(arena);

        for (key, val) in entries {
            map.insert(arena.alloc(key.to_vec()), val.clone());
        }

        ConcreteRec { map }
    }

    fn coerce<'a>(&self, arena: &'a Arena) -> Arc<dyn Rec<'a> + 'a>
    where
        'p: 'a,
    {
        let rec: ConcreteRec<'a> = ConcreteRec {
            map: self.all(arena) as HashMap<&'a [u8], Val<'a>>,
        } as ConcreteRec<'a>;

        Arc::new(rec) as Arc<dyn Rec<'a>>
    }

    fn is_neutral(&self) -> bool {
        // this is a bit unhinged but we can just make a quick arena for this,
        // since we're just returning a bool out
        let arena = Arena::new();
        let b = self
            .all(&arena)
            .iter()
            .map(|(_, val)| val.is_neutral())
            .any(|a| a);

        b
    }
}

pub struct ConcreteRec<'p> {
    pub map: HashMap<&'p [u8], Val<'p>>,
}

impl<'p> Rec<'p> for ConcreteRec<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match self.map.get(key) {
            Some(val) => Ok(val.coerce(arena)),
            None => Err(InternalError {
                message: format!(
                    "could not find field '{}'",
                    String::from_utf8(key.to_vec()).expect("bad field name?!")
                ),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a,
    {
        self.map
            .iter()
            .map(|(a, b)| (a as &'a [u8], b.coerce(arena) as Val<'a>))
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
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match key {
            b"seq" => Ok(Val::Str { s: self.read.seq() }),
            b"id" => Ok(Val::Str {
                s: self.read.id().as_bytes(),
            }),
            b"desc" => Ok(Val::Str {
                s: self.read.desc().unwrap_or_default().as_bytes(),
            }),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<&'a [u8], Val<'a>> = HashMap::new();
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
        let s = ConcreteRec { map }.fmt(f);
        s
    }
}

pub struct FastqRead<'a> {
    pub read: &'a bio::io::fastq::Record,
}

impl<'p> Rec<'p> for FastqRead<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match key {
            b"seq" => Ok(Val::Str { s: self.read.seq() }),
            b"id" => Ok(Val::Str {
                s: self.read.id().as_bytes(),
            }),
            b"desc" => Ok(Val::Str {
                s: self.read.desc().unwrap_or_default().as_bytes(),
            }),
            b"qual" => Ok(Val::Str {
                s: self.read.qual(),
            }),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<&'a [u8], Val<'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc", b"qual"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'a> Display for FastqRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // note: this seems highly wasteful
        let arena = Arena::new();
        let map = self.all(&arena);
        let s = ConcreteRec { map }.fmt(f);
        s
    }
}

pub struct SamRead<'a> {
    pub read: &'a noodles::sam::Record,

    pub cigar: &'a noodles::sam::record::Cigar<'a>,
    pub seq: &'a noodles::sam::record::Sequence<'a>,
    pub qual: &'a noodles::sam::record::QualityScores<'a>,
    pub data: &'a noodles::sam::record::Data<'a>,
}

impl<'a> SamRead<'a> {
    // pub fn new(read: &'a noodles::sam::Record) -> SamRead<'a> {
    //     SamRead {
    //         read,
    //         cigar: read.cigar(),
    //         seq: todo!(),
    //         qual: todo!(),
    //         data: todo!(),
    //     }
    // }
}

impl<'p> Rec<'p> for SamRead<'p> {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match key {
            b"qname" | b"id" => Ok(Val::Str {
                s: self.read.name().unwrap_or_default(),
            }),

            b"flag" => Ok(Val::Num {
                n: self.read.flags().unwrap_or_default().bits() as f32,
            }),

            b"flag_rec" => Ok(Val::Rec {
                rec: Arc::new(SamFlags {
                    flags: self.read.flags().unwrap(),
                }),
            }),

            b"rname" => Ok(Val::Str {
                s: self.read.reference_sequence_name().unwrap_or_default(),
            }),

            b"pos" => Ok(Val::Num {
                n: self.read.alignment_start().unwrap().unwrap().get() as f32,
            }),

            b"mapq" => Ok(Val::Num {
                n: self.read.mapping_quality().unwrap().unwrap().get() as f32,
            }),

            b"cigar" => Ok(Val::Str {
                s: self.cigar.as_ref(),
            }),

            b"rnext" => Ok(Val::Str {
                s: self.read.mate_reference_sequence_name().unwrap_or_default(),
            }),

            b"pnext" => Ok(Val::Num {
                n: self.read.mate_alignment_start().unwrap().unwrap().get() as f32,
            }),

            b"tlen" => Ok(Val::Num {
                n: self.read.template_length().unwrap_or_default() as f32,
            }),

            b"seq" => Ok(Val::Str {
                s: self.seq.as_ref(),
            }),

            b"qual" => Ok(Val::Str {
                s: self.qual.as_ref(),
            }),

            // WARN need to do this
            b"tags" => todo!(),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<&'a [u8], Val<'a>> = HashMap::new();
        let fields: Vec<&[u8]> = vec![b"seq", b"id", b"desc", b"qual"];

        for field in fields {
            map.insert(field, self.get(field, arena).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'p> Display for SamRead<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // note: this seems highly wasteful
        let arena = Arena::new();
        let map = self.all(&arena);
        let s = ConcreteRec { map }.fmt(f);
        s
    }
}

pub struct SamFlags {
    pub flags: noodles::sam::alignment::record::Flags,
}

impl<'p> Rec<'p> for SamFlags {
    fn get<'a>(&self, key: &[u8], arena: &'a Arena) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        todo!()
    }

    fn all<'a>(&self, arena: &'a Arena) -> HashMap<&'a [u8], Val<'a>>
    where
        'p: 'a,
    {
        todo!()
    }
}

impl<'p> Display for SamFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
