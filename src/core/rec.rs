use noodles::sam::record::Sequence;

use crate::util::{self, Arena, CoreRecField, Location};
use std::{collections::HashMap, fmt::Display, rc::Rc, sync::Arc};

use super::{EvalError, InternalError, Val};

#[derive(Debug)]
pub enum RecError {
    BadIndex(String),
}

pub trait Rec<'p>: Display + Send + Sync {
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
    where
        'p: 'a;
    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a;
    fn with<'a>(&self, key: &[u8], val: Val<'a>) -> FullyConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all();
        map.insert(key.to_vec(), val);

        FullyConcreteRec { map }
    }
    fn with_all<'a>(&self, entries: &[(&[u8], Val<'a>)]) -> FullyConcreteRec<'a>
    where
        'p: 'a,
    {
        let mut map = self.all();

        for (key, val) in entries {
            map.insert(key.to_vec(), val.clone());
        }

        FullyConcreteRec { map }
    }

    fn coerce<'a>(&self) -> Arc<dyn Rec<'a> + 'a>
    where
        'p: 'a,
    {
        let rec: FullyConcreteRec<'a> =
            FullyConcreteRec { map: self.all() } as FullyConcreteRec<'a>;

        Arc::new(rec) as Arc<dyn Rec<'a>>
    }

    fn is_neutral(&self) -> bool {
        self.all().iter().any(|(_, val)| val.is_neutral())
    }

    fn slice<'a>(&self, start: usize, end: usize) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        // record must have a sequence to be sliced
        if let Val::Str { s: seq } = self
            .get(b"seq")
            .map_err(|_| InternalError::new("trying to slice a non-read record?!"))?
            as Val<'p>
        {
            match self.get(b"qual") {
                Ok(Val::Str { s: qual }) => Ok(Val::Rec {
                    rec: Arc::new(self.with_all(&[
                        (
                            b"seq",
                            Val::Str {
                                s: &seq[start..end] as &'a [u8],
                            },
                        ),
                        (
                            b"qual",
                            Val::Str {
                                s: &qual[start..end] as &'a [u8],
                            },
                        ),
                    ])),
                }),
                _ => Ok(Val::Rec {
                    rec: Arc::new(self.with(
                        b"seq",
                        Val::Str {
                            s: &seq[start..end],
                        },
                    )),
                }),
            }
        } else {
            Err(InternalError::new("message"))
        }
    }
}

pub struct ConcreteRec<'p> {
    pub map: HashMap<&'p [u8], Val<'p>>,
}

impl<'p> Rec<'p> for ConcreteRec<'p> {
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        todo!()
        // match self.map.get(key) {
        //     Some(val) => Ok(val.coerce(arena)),
        //     None => Err(InternalError {
        //         message: format!(
        //             "could not find field '{}'",
        //             String::from_utf8(key.to_vec()).expect("bad field name?!")
        //         ),
        //     }),
        // }
    }

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        self.map
            .iter()
            .map(|(a, b)| (a.to_vec(), b.coerce()))
            .collect()
        // self.map
        //     .iter()
        //     .map(|(a, b)| (a as &'a [u8], b.coerce(arena) as Val<'a>))
        //     .collect()
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

pub struct FullyConcreteRec<'p> {
    pub map: HashMap<Vec<u8>, Val<'p>>,
}

impl<'p> Rec<'p> for FullyConcreteRec<'p> {
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        match self.map.get(key) {
            Some(val) => Ok(val.coerce()),
            None => Err(InternalError {
                message: format!(
                    "could not find field '{}'",
                    String::from_utf8(key.to_vec()).expect("bad field name?!")
                ),
            }),
        }
    }

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        self.map
            .iter()
            .map(|(a, b)| (a.clone(), b.coerce() as Val<'a>))
            .collect()
    }
}

impl<'a> Display for FullyConcreteRec<'a> {
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
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
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

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<Vec<u8>, Val<'a>> = HashMap::new();
        let fields: Vec<Vec<u8>> = vec![b"seq".to_vec(), b"id".to_vec(), b"desc".to_vec()];

        for field in &fields {
            map.insert(
                field.clone(),
                self.get(&field).expect("Couldn't get field!"),
            );
        }

        map
    }
}

impl<'a> Display for FastaRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let map = self.all();
        let s = FullyConcreteRec { map }.fmt(f);
        s
    }
}

pub struct FastqRead<'a> {
    pub read: &'a bio::io::fastq::Record,
}

impl<'p> Rec<'p> for FastqRead<'p> {
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
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

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<Vec<u8>, Val<'a>> = HashMap::new();
        let fields: Vec<Vec<u8>> = vec![
            b"seq".to_vec(),
            b"id".to_vec(),
            b"desc".to_vec(),
            b"qual".to_vec(),
        ];

        for field in &fields {
            map.insert(
                field.clone(),
                self.get(&field).expect("Couldn't get field!"),
            );
        }

        map
    }
}

impl<'a> Display for FastqRead<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // note: this seems highly wasteful
        let map = self.all();
        let s = FullyConcreteRec { map }.fmt(f);
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
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
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

            // b"flag_rec" => Ok(Val::Rec {
            //     rec: Arc::new(SamFlags {
            //         flags: self.read.flags().unwrap(),
            //     }),
            // }),
            b"rname" => Ok(Val::Str {
                s: self
                    .read
                    .reference_sequence_name()
                    .map(|a| a.as_ref())
                    .unwrap_or(b"*"),
            }),

            b"pos" => Ok(Val::Num {
                n: self
                    .read
                    .alignment_start()
                    .map(|r| r.map(|pos| pos.get() as f32))
                    .unwrap_or(Ok(0.0))
                    .unwrap(),
            }),

            b"mapq" => Ok(Val::Num {
                n: self
                    .read
                    .mapping_quality()
                    .map(|r| r.map(|mapq| mapq.get() as f32))
                    .unwrap_or(Ok(-1.0))
                    .unwrap(),
            }),

            b"cigar" => Ok(Val::Str {
                s: match self.cigar.as_ref() {
                    b"" => b"*",
                    r => r,
                },
            }),

            b"rnext" => Ok(Val::Str {
                s: self
                    .read
                    .mate_reference_sequence_name()
                    .map(|a| a.as_ref())
                    .unwrap_or(b"*"),
            }),

            b"pnext" => Ok(Val::Num {
                n: self
                    .read
                    .mate_alignment_start()
                    .map(|r| r.map(|pos| pos.get() as f32))
                    .unwrap_or(Ok(0.0))
                    .unwrap(),
            }),

            b"tlen" => Ok(Val::Num {
                n: self
                    .read
                    .template_length()
                    .map(|tlen| tlen as f32)
                    .unwrap_or(0.0),
            }),

            b"seq" => Ok(Val::Str {
                s: match self.seq.as_ref() {
                    b"" => b"*",
                    r => r,
                },
            }),

            b"qual" => Ok(Val::Str {
                s: self.qual.as_ref(),
            }),

            // WARN need to do this
            b"tags" => Ok(Val::Str {
                s: self.data.as_ref(),
            }),

            _ => Err(InternalError {
                message: String::from_utf8(key.to_vec()).expect("Couldn't convert vec to string!"),
            }),
        }
    }

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<Vec<u8>, Val<'a>> = HashMap::new();
        let fields: Vec<Vec<u8>> = vec![
            b"qname".to_vec(),
            b"id".to_vec(),
            b"flag".to_vec(), // b"flag_rec".to_vec(),
            b"rname".to_vec(),
            b"pos".to_vec(),
            b"mapq".to_vec(),
            b"cigar".to_vec(),
            b"rnext".to_vec(),
            b"pnext".to_vec(),
            b"tlen".to_vec(),
            b"seq".to_vec(),
            b"qual".to_vec(),
            b"tags".to_vec(),
        ];

        for field in &fields {
            map.insert(field.clone(), self.get(field).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'p> Display for SamRead<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // note: this seems highly wasteful
        let map = self.all();
        let s = FullyConcreteRec { map }.fmt(f);
        s
    }
}

pub struct SamFlags {
    pub flags: noodles::sam::alignment::record::Flags,
}

impl<'p> Rec<'p> for SamFlags {
    fn get<'a>(&self, key: &[u8]) -> Result<Val<'a>, InternalError>
    where
        'p: 'a,
    {
        todo!()
    }

    fn all<'a>(&self) -> HashMap<Vec<u8>, Val<'a>>
    where
        'p: 'a,
    {
        let mut map: HashMap<Vec<u8>, Val<'a>> = HashMap::new();
        let fields: Vec<Vec<u8>> = vec![
            b"qname".to_vec(),
            b"id".to_vec(),
            b"flag".to_vec(), // b"flag_rec".to_vec(),
            b"rname".to_vec(),
            b"pos".to_vec(),
            b"mapq".to_vec(),
            b"cigar".to_vec(),
            b"rnext".to_vec(),
            b"pnext".to_vec(),
            b"tlen".to_vec(),
            b"seq".to_vec(),
            b"qual".to_vec(),
            b"tags".to_vec(),
        ];

        for field in &fields {
            map.insert(field.clone(), self.get(field).expect("Couldn't get field!"));
        }

        map
    }
}

impl<'p> Display for SamFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
