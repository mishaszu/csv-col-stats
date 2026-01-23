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
