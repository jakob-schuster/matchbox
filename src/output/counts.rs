//! Accumulate global counts.

use std::{collections::HashMap, fs::File, io::Write};

use itertools::Itertools;

use crate::{core::PortableVal, output::OutputError, util::bytes_to_string};

#[derive(Default, PartialEq)]
pub struct MultiCountsHandler {
    map: HashMap<Vec<u8>, CountsHandler>,
}

impl MultiCountsHandler {
    pub fn handle(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
            PortableVal::Rec { fields } => {
                if let (Some(PortableVal::Str { s: name }), Some(val)) = (
                    fields.get(&(b"name".to_vec())),
                    fields.get(&(b"val".to_vec())),
                ) {
                    if let Some(handler) = self.map.get_mut(name) {
                        handler.handle(val)?;
                    } else {
                        let mut handler = CountsHandler::default();
                        handler.handle(val)?;
                        self.map.insert(name.clone(), handler);
                    }

                    Ok(())
                } else {
                    Err(OutputError::TypeCounts { val: val.clone() })
                }
            }
            _ => Err(OutputError::TypeCounts { val: val.clone() }),
        }
    }

    pub fn finish(&self, output_directory: &String) {
        self.print();

        // do not write out an empty count
        if !self.map.is_empty() {
            self.print_csv(output_directory);
        }
    }

    pub fn print(&self) {
        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            println!("{}", String::from_utf8(name.to_vec()).unwrap());
            handler.print();
        }
    }

    pub fn print_csv(&self, output_directory: &str) {
        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            handler.print_csv(&format!(
                "{}/{}.csv",
                output_directory,
                bytes_to_string(name).unwrap()
            ));
        }
    }

    pub fn summarize(&self) -> MultiCountsHandlerSummary {
        MultiCountsHandlerSummary {
            all: self
                .map
                .iter()
                .map(|(name, handler)| {
                    (
                        String::from_utf8(name.clone()).unwrap(),
                        handler.summarize(),
                    )
                })
                .collect(),
        }
    }
}

#[derive(Clone)]
pub struct MultiCountsHandlerSummary {
    all: Vec<(String, CountsHandlerSummary)>,
}

#[derive(Default, PartialEq)]
struct CountsHandler {
    map: HashMap<String, i32>,
}

impl CountsHandler {
    fn handle(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        let string = format!("{}", val);

        if let Some(count) = self.map.get(&string) {
            self.map.insert(string, count + 1);
        } else {
            self.map.insert(string, 1);
        }

        Ok(())
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            for (key, val) in self.map.iter().sorted_by_key(|(key, _)| *key) {
                println!("\t{val}\t{key}");
            }
        }
    }

    fn print_csv(&self, filename: &str) {
        let mut file = File::create(filename).unwrap();
        file.write_all(b"value,count\n");

        for (name, value) in self.map.iter().sorted_by_key(|(key, _)| *key) {
            file.write_all(format!("{},{}\n", name, value).as_bytes());
        }
    }

    fn summarize(&self) -> CountsHandlerSummary {
        CountsHandlerSummary {
            top: self.map.clone().into_iter().collect::<Vec<_>>(),
        }
    }
}

#[derive(Clone)]
pub struct CountsHandlerSummary {
    top: Vec<(String, i32)>,
}
