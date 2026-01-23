use crate::parser::ColStats;
use crate::{CsvColError, Result};

pub fn parse_column(
    field: &[u8],
    field_index: usize,
    row_index: usize,
    is_exact_median: bool,
    column_stats: &mut [Option<ColStats>],
) -> Result<()> {
    match lexical_core::parse::<i64>(field) {
        Ok(value) => {
            if let Some(stats) = column_stats.get_mut(field_index) {
                match stats {
                    Some(stats) => {
                        stats.update(value);
                    }
                    // initialize stats only on first row
                    None if row_index == 0 => {
                        let mut new_stats = ColStats::new(is_exact_median);
                        new_stats.update(value);
                        *stats = Some(new_stats);
                    }
                    // TODO: if first rows are empty it's still should be possible to
                    // initialize column later
                    None => (),
                }
            }
        }
        Err(e) => {
            if column_stats[field_index].is_some() {
                return Err(CsvColError::ColumnParse(row_index, field_index, e));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_column() {
        let mut column_stats = vec![None, None];

        parse_column(b"16", 1, 0, true, &mut column_stats).unwrap();

        match column_stats.get(1).unwrap() {
            Some(stat) => {
                assert_eq!(stat.min.unwrap(), 16);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 16);
                assert_eq!(stat.count, 1);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 16.);
            }
            None => panic!("Stat should be initialized"),
        }

        parse_column(b"4", 1, 0, true, &mut column_stats).unwrap();

        match column_stats.get(1).unwrap() {
            Some(stat) => {
                assert_eq!(stat.min.unwrap(), 4);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 20);
                assert_eq!(stat.count, 2);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 10.);
            }
            None => panic!("Stat should be initialized"),
        }

        parse_column(b"2", 1, 0, true, &mut column_stats).unwrap();

        match column_stats.get(1).unwrap() {
            Some(stat) => {
                assert_eq!(stat.min.unwrap(), 2);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 22);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 4.);
            }
            None => panic!("Stat should be initialized"),
        }
    }

    #[test]
    fn test_text_parse_column() {
        let mut column_stats = vec![None, None];

        parse_column(b"test", 1, 0, true, &mut column_stats).unwrap();

        let item = column_stats.into_iter().nth(1).unwrap();
        assert!(item.is_none());
    }

    #[test]
    fn test_uninitialized_parse_column() {
        let mut column_stats = vec![None, Some(ColStats::new(true)), None];

        parse_column(b"120", 2, 1, true, &mut column_stats).unwrap();

        let item = column_stats.into_iter().nth(2).unwrap();
        assert!(item.is_none());
    }
}
