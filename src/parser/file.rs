use std::{
    fs::File,
    io::{BufReader, Read},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use csv::Reader;

use crate::{
    Config, CsvColError, Output, Result, Stats,
    parser::{
        column::{ColumnOption, parse_column},
        is_empty, trim_bytes,
    },
};

/// Parses a CSV file from disk and computes column statistics.
///
/// This function opens the file at `path`, configures the median calculation
/// strategy based on the file size and `config.median_config`, and delegates
/// parsing to [`parse_reader`].
///
/// The median strategy (exact vs approximate) is selected automatically by
/// comparing the configured memory budget against the input file size.
///
/// # Parameters
/// - `path`: Path to the CSV file to parse.
/// - `config`: Parsing and aggregation configuration. The median configuration
///   may be adjusted based on the input file size.
///
/// # Returns
/// Aggregated statistics for all numeric columns in the file.
///
/// # Errors
/// Returns an error if:
/// - The file cannot be opened or read.
/// - CSV parsing fails.
/// - A column previously identified as numeric encounters invalid data.
pub fn parse_file(path: PathBuf, mut config: Config) -> Result<Output> {
    let file = File::open(&path).map_err(|e| CsvColError::Io {
        path: path.clone(),
        source: e,
    })?;
    let file_size = file
        .metadata()
        .map_err(|e| CsvColError::Io { path, source: e })?
        .size();

    config.median_config.exact_median = config.median_config.memory_budget >= file_size as usize;

    let reader = BufReader::new(file);

    let columns = parse_reader(reader, config)?
        .into_iter()
        .flat_map(|(header, col)| match col {
            ColumnOption::FilteredNumber(col, _) | ColumnOption::Number(col) => {
                let col: Result<Stats> = col.try_into();
                match col {
                    Ok(col) => Some(Ok((header, col))),
                    Err(e) => Some(Err(e)),
                }
            }
            _ => None,
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(columns.into_iter().collect())
}

/// Parses CSV data from a reader and computes per-column statistics.
///
/// This function reads CSV records from `reader`, inspects each column,
/// and incrementally builds column statistics according to `config`.
///
/// Column behavior:
/// - Columns listed in `config.data_config.ignore_columns` are ignored.
/// - Columns matching a filter expression are conditionally updated.
/// - Columns are initialized as numeric on the first successfully parsed value.
/// - Empty or non-numeric values are ignored until a column becomes numeric.
/// - Once a column is classified as numeric, subsequent parse errors are reported.
///
/// Median calculation strategy (exact vs approximate) is determined by
/// `config.median_config`.
///
/// # Parameters
/// - `reader`: Any type implementing [`std::io::Read`] (e.g. file, buffer, cursor).
/// - `config`: Parsing and aggregation configuration.
///
/// # Returns
/// A vector of `(column_name, ColumnOption)` pairs, preserving CSV header order.
///
/// # Errors
/// Returns an error if:
/// - CSV parsing fails.
/// - A column previously identified as numeric encounters a non-numeric value.
pub fn parse_reader(reader: impl Read, config: Config) -> Result<Vec<(String, ColumnOption)>> {
    let mut csv_reader = Reader::from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(ToOwned::to_owned)
        .collect();

    let mut column_stats: Vec<ColumnOption> = Vec::with_capacity(headers.len());
    for header in headers.iter() {
        if config
            .data_config
            .ignore_columns
            .iter()
            .any(|v| v == header)
        {
            column_stats.push(ColumnOption::Ignored);
        } else if let Some(filter) = config.data_config.filter.clone()
            && filter.check_by_name(header)
        {
            column_stats.push(ColumnOption::UninitializedWithFilter(filter));
        } else {
            column_stats.push(ColumnOption::Uninitialized);
        }
    }

    for (row_index, row) in csv_reader.byte_records().enumerate() {
        let row: csv::ByteRecord = row?;

        for (field_index, field) in row.iter().enumerate() {
            if let ColumnOption::Ignored = column_stats[field_index] {
                continue;
            }
            if is_empty(field) {
                continue;
            }
            let trimmed_bytes = trim_bytes(field);
            parse_column(
                trimmed_bytes,
                &config.median_config,
                &mut column_stats[field_index],
            )
            .map_err(|e| CsvColError::ColumnParse(row_index, field_index, e))?;
        }
    }

    Ok(headers.into_iter().zip(column_stats).collect())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use crate::Config as CsvColCinfig;
    use crate::parser::column::ColumnOption::*;

    use super::*;

    fn build_test_set() -> String {
        "id,value1,value2\n
            1,10,20\n
            2,30,40\n
            3,25,35"
            .to_string()
    }

    fn build_test_set2() -> String {
        "id,name,value1,value2\n
            1,\"test\",\"\", 10\n
            2,\"foo\",\"\", 20\n
            3,\"boo\",\"N/A\",30 \n
            4,\"foo\",1,40\n
            5,\"boo\",2,50\n
            6,\"foo\",3,60\n"
            .to_string()
    }

    #[test]
    fn test_parse_reader() {
        let cursor = Cursor::new(build_test_set());
        let reader = BufReader::new(cursor);
        let config = CsvColCinfig::default();

        let result = parse_reader(reader, config).unwrap();

        assert_eq!(result.len(), 3);

        match result.first() {
            Some((_, Number(stat))) => {
                assert_eq!(stat.min, Some(1));
                assert_eq!(stat.max, Some(3));
                assert_eq!(stat.sum, 6);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(2.));
            }
            _ => panic!("Stat should be initialized"),
        }

        match result.get(1) {
            Some((_, Number(stat))) => {
                assert_eq!(stat.min, Some(10));
                assert_eq!(stat.max, Some(30));
                assert_eq!(stat.sum, 65);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(25.));
            }
            _ => panic!("Stat should be initialized"),
        }

        match result.get(2) {
            Some((_, Number(stat))) => {
                assert_eq!(stat.min, Some(20));
                assert_eq!(stat.max, Some(40));
                assert_eq!(stat.sum, 95);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(35.));
            }
            _ => panic!("Stat should be initialized"),
        }
    }

    #[test]
    fn test_parse_file() {
        let test_set = build_test_set();
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file
            .as_file_mut()
            .write_all(test_set.as_bytes())
            .unwrap();

        let mut result =
            parse_file(PathBuf::from(temp_file.path()), CsvColCinfig::default()).unwrap();

        let id_stats = Stats {
            min: Some(1),
            max: Some(3),
            mean: Some(2.),
            median: Some(2.),
        };
        assert_eq!(result.remove("id").unwrap(), id_stats);

        let value1_stats = Stats {
            min: Some(10),
            max: Some(30),
            mean: Some(21.67),
            median: Some(25.),
        };
        assert_eq!(result.remove("value1").unwrap(), value1_stats);

        let value2_stats = Stats {
            min: Some(20),
            max: Some(40),
            mean: Some(31.67),
            median: Some(35.),
        };
        assert_eq!(result.remove("value2").unwrap(), value2_stats);
    }

    #[test]
    fn test_parse_file_empty_fields() {
        let test_set = build_test_set2();
        let mut temp_file = tempfile::NamedTempFile::new().unwrap();

        temp_file
            .as_file_mut()
            .write_all(test_set.as_bytes())
            .unwrap();

        let mut config = CsvColCinfig::default();
        config.data_config.ignore_columns = vec!["id".to_string()];

        let mut result = parse_file(PathBuf::from(temp_file.path()), config).unwrap();

        assert_eq!(result.len(), 2);

        let value1_stats = Stats {
            min: Some(1),
            max: Some(3),
            mean: Some(2.),
            median: Some(2.),
        };
        assert_eq!(result.remove("value1").unwrap(), value1_stats);

        let value2_stats = Stats {
            min: Some(10),
            max: Some(60),
            mean: Some(35.),
            median: Some(35.),
        };
        assert_eq!(result.remove("value2").unwrap(), value2_stats);
    }
}
