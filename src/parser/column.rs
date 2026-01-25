use crate::MedianConfig;
use crate::filter::Expression;
use crate::parser::ColStats;
use crate::{CsvColError, Result};

pub(super) enum ColumnOption {
    Uninitialized,
    UninitializedWithFilter(Expression),
    Ignored,
    Number(ColStats),
    FilteredNumber(ColStats, Expression),
}

pub fn parse_column(
    field: &[u8],
    field_index: usize,
    row_index: usize,
    median_config: &MedianConfig,
    stats: &mut ColumnOption,
) -> Result<()> {
    match lexical_core::parse::<i64>(field) {
        Ok(value) => {
            match stats {
                ColumnOption::Number(stats) => {
                    stats.update(value);
                }
                ColumnOption::FilteredNumber(stats, expression) => {
                    if expression.unchacked_validate(&value) {
                        stats.update(value);
                    }
                }
                ColumnOption::Uninitialized => {
                    let mut new_stats = ColStats::new(median_config);
                    new_stats.update(value);
                    *stats = ColumnOption::Number(new_stats);
                }
                // if filtered column is not number it's going to fail later
                ColumnOption::UninitializedWithFilter(expression) => {
                    let mut new_stats = ColStats::new(median_config);
                    if expression.unchacked_validate(&value) {
                        new_stats.update(value);
                    }
                    *stats = ColumnOption::FilteredNumber(new_stats, expression.clone())
                }
                ColumnOption::Ignored => (),
            }
        }
        Err(e) => {
            match stats {
                ColumnOption::Number(_) | ColumnOption::FilteredNumber(_, _) => {
                    // TODO: remove field_index
                    return Err(CsvColError::ColumnParse(row_index, field_index, e));
                }
                _ => (),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use ColumnOption::*;

    #[test]
    fn test_parse_column() {
        let mut column_stats = [Uninitialized, Uninitialized];

        let median_config = MedianConfig::default();
        parse_column(b"16", 1, 0, &median_config, &mut column_stats[1]).unwrap();

        match column_stats.get(1).unwrap() {
            Number(stat) => {
                assert_eq!(stat.min.unwrap(), 16);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 16);
                assert_eq!(stat.count, 1);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 16.);
            }
            _ => panic!("Stat should be initialized"),
        }

        parse_column(b"4", 1, 0, &median_config, &mut column_stats[1]).unwrap();

        match column_stats.get(1).unwrap() {
            Number(stat) => {
                assert_eq!(stat.min.unwrap(), 4);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 20);
                assert_eq!(stat.count, 2);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 10.);
            }
            _ => panic!("Stat should be initialized"),
        }

        parse_column(b"2", 1, 0, &median_config, &mut column_stats[1]).unwrap();

        match column_stats.get(1).unwrap() {
            Number(stat) => {
                assert_eq!(stat.min.unwrap(), 2);
                assert_eq!(stat.max.unwrap(), 16);
                assert_eq!(stat.sum, 22);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap().unwrap(), 4.);
            }
            _ => panic!("Stat should be initialized"),
        }
    }

    #[test]
    fn test_text_parse_column() {
        let mut column_stats = vec![Uninitialized, Uninitialized];

        let median_config = MedianConfig::default();

        parse_column(b"test", 1, 0, &median_config, &mut column_stats[1]).unwrap();

        let item = column_stats.into_iter().nth(1).unwrap();
        match item {
            Uninitialized | Ignored => (),
            _ => panic!("field should be uninitialized"),
        }
    }

    #[test]
    fn test_uninitialized_parse_column() {
        let median_config = MedianConfig::default();
        let mut column_stats = [
            Uninitialized,
            Number(ColStats::new(&median_config)),
            Uninitialized,
        ];

        parse_column(b"120", 2, 1, &median_config, &mut column_stats[2]).unwrap();

        match &column_stats[2] {
            Number(value) => {
                assert_eq!(value.max, Some(120))
            }
            _ => panic!("field should be initialized"),
        }
    }

    #[test]
    fn test_uninitialized_filter_parse_column() {
        let median_config = MedianConfig::default();
        let mut column_stats = [
            Uninitialized,
            UninitializedWithFilter(Expression::from_str("value > 1").unwrap()),
        ];

        parse_column(b"120", 1, 1, &median_config, &mut column_stats[1]).unwrap();

        // let item = column_stats.into_iter().nth(1).unwrap();
        match &column_stats[1] {
            FilteredNumber(s, _) => {
                assert_eq!(s.max, Some(120))
            }
            _ => panic!("field should be initialized"),
        }
    }

    #[test]
    fn test_filter_out_uninitialized_parse_column() {
        let median_config = MedianConfig::default();
        let mut column_stats = [
            Uninitialized,
            UninitializedWithFilter(Expression::from_str("value > 10").unwrap()),
        ];

        parse_column(
            b"5",
            1,
            Default::default(),
            &median_config,
            &mut column_stats[1],
        )
        .unwrap();

        // let item = column_stats.into_iter().nth(1).unwrap();
        match &column_stats[1] {
            FilteredNumber(s, _) => {
                assert_eq!(s.max, None);
                assert_eq!(s.count, 0);
            }
            _ => panic!("field should be initialized"),
        }
    }

    #[test]
    fn test_filter_out_initialized_parse_column() {
        let median_config = MedianConfig::default();
        let mut stat = ColStats::new(&median_config);
        stat.update(20);
        let mut column_stats = [
            Uninitialized,
            FilteredNumber(stat, Expression::from_str("value > 10").unwrap()),
        ];

        parse_column(
            b"5",
            1,
            Default::default(),
            &median_config,
            &mut column_stats[1],
        )
        .unwrap();

        // let item = column_stats.into_iter().nth(1).unwrap();
        match &column_stats[1] {
            FilteredNumber(s, _) => {
                assert_eq!(s.max, Some(20));
                assert_eq!(s.count, 1);
            }
            _ => panic!("field should be initialized"),
        }
    }
}
