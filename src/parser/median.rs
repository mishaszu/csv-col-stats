use sketches_ddsketch::{Config as DDConfig, DDSketch};
use std::{cmp::Reverse, collections::BinaryHeap};

use crate::{CsvColError, MedianConfig, Result};

#[derive(Default)]
pub struct MedianHeap {
    top: BinaryHeap<Reverse<i64>>,
    bottom: BinaryHeap<i64>,
}

impl MedianHeap {
    fn new() -> Self {
        Self {
            top: BinaryHeap::new(),
            bottom: BinaryHeap::new(),
        }
    }

    fn add(&mut self, value: i64) {
        if self
            .bottom
            .peek()
            .is_none_or(|&bottom_max| bottom_max >= value)
        {
            self.bottom.push(value);
        } else {
            self.top.push(Reverse(value));
        }

        if self.bottom.len() > self.top.len() + 1 {
            let value = self.bottom.pop().unwrap();
            self.top.push(Reverse(value));
        }
        if self.top.len() > self.bottom.len() + 1 {
            let value = self.top.pop().unwrap().0;
            self.bottom.push(value);
        }

        if let (Some(&bottom), Some(&Reverse(top))) = (self.bottom.peek(), self.top.peek())
            && bottom > top
        {
            let bottom = self.bottom.pop().unwrap();
            let top = self.top.pop().unwrap().0;

            self.bottom.push(top);
            self.top.push(Reverse(bottom));
        }
    }

    fn median(&self) -> Option<f64> {
        if self.bottom.is_empty() && self.top.is_empty() {
            return None;
        }

        if self.top.len() > self.bottom.len() {
            Some(self.top.peek().unwrap().0 as f64)
        } else if self.bottom.len() > self.top.len() {
            Some(*self.bottom.peek().unwrap() as f64)
        } else {
            let top = self.top.peek().unwrap().0 as f64;
            let bottom = *self.bottom.peek().unwrap() as f64;
            Some((top + bottom) / 2.)
        }
    }
}

pub enum MedianSettings {
    Exact,
    Approximate(Option<u32>),
}

impl From<&MedianConfig> for MedianSettings {
    fn from(config: &MedianConfig) -> Self {
        if config.exact_median {
            MedianSettings::Exact
        } else {
            MedianSettings::Approximate(config.buckets)
        }
    }
}

pub enum Median {
    Exact(MedianHeap),
    Approximate(DDSketch),
}

impl Median {
    pub fn new(config: MedianSettings) -> Self {
        match config {
            MedianSettings::Exact => Self::Exact(MedianHeap::new()),
            MedianSettings::Approximate(bins) => {
                let mut ddcondig = DDConfig::default();
                if let Some(value) = bins {
                    ddcondig.max_num_bins = value;
                };
                Self::Approximate(DDSketch::new(ddcondig))
            }
        }
    }

    pub fn add(&mut self, value: i64) {
        match self {
            Self::Exact(heaps) => heaps.add(value),
            Self::Approximate(ddsketch) => ddsketch.add(value as f64),
        }
    }

    pub fn calculate(&self) -> Result<Option<f64>> {
        match self {
            Self::Approximate(ddsketch) => ddsketch.quantile(0.5).map_err(CsvColError::DDSketch),
            Self::Exact(heaps) => Ok(heaps.median()),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::median::MedianHeap;

    #[test]
    fn test_median_heap_empty() {
        let heap = MedianHeap::new();
        assert_eq!(heap.median(), None);
    }

    #[test]
    fn test_median_heap_odd() {
        let test_set = vec![1, 3, 5, 8, 9, 6, 3];
        let mut heap = MedianHeap::new();
        for item in test_set {
            heap.add(item);
        }
        assert_eq!(heap.median().unwrap(), 5.);
    }

    #[test]
    fn test_median_heap_even() {
        let test_set = vec![12, 6, 1, 2, 3, 8];
        let mut heap = MedianHeap::new();
        for item in test_set {
            heap.add(item);
        }
        assert_eq!(heap.median().unwrap(), 4.5);
    }
}
