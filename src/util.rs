//! Utility functions used throughout matchbox.

use std::fmt::{self, Display, Formatter};

use crate::core::InternalError;

pub mod cache;
pub mod env;
pub mod location;
pub mod ran;
pub mod recfield;

pub type Arena = bumpalo::Bump;

/// Convert a byte slice to a string.
pub fn bytes_to_string(bytes: &[u8]) -> Result<String, InternalError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| {
        InternalError::new(&format!(
            "couldn't convert bytes to string!? {:?}",
            bytes.to_vec()
        ))
    })
}

/// Translates a sequence into protein.
pub fn translate(seq: &[u8], stop_codon: &u8, illegal_codon: &u8) -> String {
    fn translate_codon(codon: &[u8], stop_codon: &u8, illegal_codon: &u8) -> char {
        match codon {
            b"TTT" => 'F',
            b"TTC" => 'F',
            b"TTA" => 'L',
            b"TTG" => 'L',
            b"TCT" => 'S',
            b"TCC" => 'S',
            b"TCA" => 'S',
            b"TCG" => 'S',
            b"TAT" => 'Y',
            b"TAC" => 'Y',
            b"TAA" => *stop_codon as char,
            b"TAG" => *stop_codon as char,
            b"TGT" => 'C',
            b"TGC" => 'C',
            b"TGA" => *stop_codon as char,
            b"TGG" => 'W',

            b"CTT" => 'L',
            b"CTC" => 'L',
            b"CTA" => 'L',
            b"CTG" => 'L',
            b"CCT" => 'P',
            b"CCC" => 'P',
            b"CCA" => 'P',
            b"CCG" => 'P',
            b"CAT" => 'H',
            b"CAC" => 'H',
            b"CAA" => 'Q',
            b"CAG" => 'Q',
            b"CGT" => 'R',
            b"CGC" => 'R',
            b"CGA" => 'R',
            b"CGG" => 'R',

            b"ATT" => 'I',
            b"ATC" => 'I',
            b"ATA" => 'I',
            b"ATG" => 'M',
            b"ACT" => 'T',
            b"ACC" => 'T',
            b"ACA" => 'T',
            b"ACG" => 'T',
            b"AAT" => 'N',
            b"AAC" => 'N',
            b"AAA" => 'K',
            b"AAG" => 'K',
            b"AGT" => 'S',
            b"AGC" => 'S',
            b"AGA" => 'R',
            b"AGG" => 'R',

            b"GTT" => 'V',
            b"GTC" => 'V',
            b"GTA" => 'V',
            b"GTG" => 'V',
            b"GCT" => 'A',
            b"GCC" => 'A',
            b"GCA" => 'A',
            b"GCG" => 'A',
            b"GAT" => 'D',
            b"GAC" => 'D',
            b"GAA" => 'E',
            b"GAG" => 'E',
            b"GGT" => 'G',
            b"GGC" => 'G',
            b"GGA" => 'G',
            b"GGG" => 'G',

            _ => *illegal_codon as char,
        }
    }

    if seq.len() < 3 {
        // terminate on the end of sequences
        String::new()
    } else {
        // or, recursively call
        format!(
            "{}{}",
            translate_codon(&seq[..3], stop_codon, illegal_codon),
            translate(&seq[3..], stop_codon, illegal_codon)
        )
    }
}

/// Takes the reverse complement of a sequence.
pub fn rev_comp(seq: &[u8]) -> Vec<u8> {
    bio::alphabets::dna::revcomp(seq)
}

/// Check if the bit i places from the right in n is active.
pub fn get_bit(n: u16, i: usize) -> bool {
    (n >> i & 1) == 1
}
