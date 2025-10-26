//! Accumulate a global mean.

use std::{collections::HashMap, fs::File, io::Write};

use itertools::Itertools;

use crate::{core::PortableVal, output::OutputError, util::bytes_to_string};

#[derive(Default, PartialEq)]
pub struct MultiAverageHandler {
    map: HashMap<Vec<u8>, AverageHandler>,
}

impl MultiAverageHandler {
    pub fn handle(&mut self, val: &PortableVal) -> Result<(), OutputError> {
        match val {
            PortableVal::Rec { fields } => {
                if let (Some(PortableVal::Str { s: name }), Some(PortableVal::Num { n: val })) = (
                    fields.get(&(b"name".to_vec())),
                    fields.get(&(b"val".to_vec())),
                ) {
                    if let Some(handler) = self.map.get_mut(name) {
                        handler.handle(*val)?;
                    } else {
                        let mut handler = AverageHandler::default();
                        handler.handle(*val)?;
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

    pub fn finish(&self, output_directory: &str) {
        self.print();

        // don't print empty averages
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
        let mut file = File::create(format!("{}/stats.csv", output_directory)).unwrap();

        file.write_all(b"name,mean,variance\n");

        for (name, handler) in self.map.iter().sorted_by_key(|(name, _)| *name) {
            file.write_all(
                format!(
                    "{},{},{}\n",
                    bytes_to_string(name).unwrap(),
                    handler.mean,
                    handler.variance()
                )
                .as_bytes(),
            );
        }
    }

    pub fn summarize(&self) -> MultiAverageHandlerSummary {
        MultiAverageHandlerSummary {
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
pub struct MultiAverageHandlerSummary {
    all: Vec<(String, AverageHandlerSummary)>,
}

#[derive(Default, PartialEq)]
struct AverageHandler {
    count: i32,
    mean: f32,
    m2: f32,
}

impl AverageHandler {
    fn handle(&mut self, num: f32) -> Result<(), OutputError> {
        self.count += 1;
        let delta = num - self.mean;
        self.mean += delta / self.count as f32;
        let delta2 = num - self.mean;
        self.m2 += delta * delta2;

        Ok(())
    }

    fn print(&self) {
        if !self.eq(&Self::default()) {
            println!("\t{:.2} ± {:.2}", self.mean, self.variance());
        }
    }

    fn summarize(&self) -> AverageHandlerSummary {
        AverageHandlerSummary {
            mean: self.mean,
            variance: self.variance(),
        }
    }

    fn variance(&self) -> f32 {
        (self.m2 / self.count as f32).sqrt()
    }
}

#[derive(Clone)]
pub struct AverageHandlerSummary {
    pub mean: f32,
    pub variance: f32,
}
