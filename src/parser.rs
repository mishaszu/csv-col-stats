use crate::{CsvColError, MedianConfig, Stats};

mod column;
mod file;
mod median;

pub use column::ColumnParseError;
pub use file::parse_file;
use median::Median;

pub(in crate::parser) fn is_empty(bytes: &[u8]) -> bool {
    bytes.is_empty() || bytes == b"NaN" || bytes == b"nan" || bytes == b"null" || bytes == b"N/A"
}

pub(in crate::parser) fn trim_bytes(mut bytes: &[u8]) -> &[u8] {
    while let Some((c, tail)) = bytes.split_first() {
        if c.is_ascii_whitespace() {
            bytes = tail;
        } else {
            break;
        }
    }

    while let Some((c, head)) = bytes.split_last() {
        if c.is_ascii_whitespace() {
            bytes = head;
        } else {
            break;
        }
    }

    bytes
}

pub(in crate::parser) struct ColStats {
    sum: i64,
    count: usize,
    max: Option<i64>,
    min: Option<i64>,
    median_approach: Median,
}

impl ColStats {
    fn new(median_config: &MedianConfig) -> Self {
        Self {
            sum: Default::default(),
            count: Default::default(),
            max: Default::default(),
            min: Default::default(),
            median_approach: Median::new(median_config.into()),
        }
    }

    fn update(&mut self, value: i64) {
        // TODO: handle overflow
        self.sum = self.sum.strict_add(value);
        self.count += 1;
        match self.max.as_ref() {
            Some(current_max) if current_max < &value => {
                self.max = Some(value);
            }
            None => self.max = Some(value),
            _ => (),
        }
        match self.min.as_ref() {
            Some(current_min) if current_min > &value => self.min = Some(value),
            None => self.min = Some(value),
            _ => (),
        }

        self.median_approach.add(value);
    }
}

impl TryInto<Stats> for ColStats {
    type Error = CsvColError;

    fn try_into(self) -> std::result::Result<Stats, Self::Error> {
        let stats = Stats {
            max: self.max,
            min: self.min,
            mean: if self.count > 0 {
                let result = self.sum as f64 / self.count as f64;
                Some((result * 100.0).round() / 100.0)
            } else {
                None
            },
            median: self.median_approach.calculate()?,
        };
        Ok(stats)
    }
}
