#[derive(Clone)]
pub struct Seq {
    bytes: Vec<u8>,
    dist: usize,
    myers: VarMyers,
}

impl Seq {
    pub fn new(bytes: &[u8], error: f64) -> Seq {
        Seq {
            bytes: bytes.to_ascii_uppercase(),
            myers: VarMyers::new(&bytes.to_ascii_uppercase()),
            dist: (bytes.len() as f64 * error).floor() as usize,
        }
    }

    pub fn find_all_disjoint(
        &self,
        seq: &[u8],
        search_ran: &Ran<usize>,
        fixed: &Ran<bool>,
    ) -> Vec<Mat> {
        let (start, end): (i32, i32) = match (fixed.start, fixed.end) {
            // somehow pin both ends
            // - i guess just calculate straight levenshtein distance?
            (true, true) => todo!(),

            // pin the start
            (true, false) => (
                search_ran.start as i32,
                (search_ran.start + self.bytes.len() + self.dist as usize) as i32,
            ),

            // pin the end
            (false, true) => (
                search_ran.end as i32 - self.bytes.len() as i32 - self.dist as i32,
                search_ran.end as i32,
            ),

            // just grab the whole slice of the sequence
            (false, false) => (search_ran.start as i32, search_ran.end as i32),
        };

        // return early if the range is inappropriate
        if start < 0 || end > seq.len() as i32 {
            return vec![];
        }

        let trimmed = &seq[start as usize..end as usize];

        self.myers
            .find_all_disjoint(trimmed, self.dist)
            .iter()
            .map(|(mat_start, mat_end, dist)| {
                Mat::new(
                    Ran::new(*mat_start + start as usize, *mat_end + start as usize),
                    *dist as u8,
                )
            })
            .collect()
    }
}
