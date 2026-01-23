use std::{
    fs::File,
    io::{BufReader, Read},
    os::unix::fs::MetadataExt,
    path::PathBuf,
};

use csv::Reader;

use crate::{
    CsvColError, Output, Result, Stats,
    parser::{ColStats, column::parse_column, is_empty},
};

pub fn parse_file(path: PathBuf, memory_budget: usize) -> Result<Output> {
    let file = File::open(&path).map_err(|e| CsvColError::Io {
        path: path.clone(),
        source: e,
    })?;
    let file_size = file
        .metadata()
        .map_err(|e| CsvColError::Io { path, source: e })?
        .size();
    let is_exact_median = file_size as usize <= memory_budget;

    let reader = BufReader::new(file);

    let columns = parse_reader(reader, is_exact_median)?
        .into_iter()
        .flat_map(|(header, col)| {
            if let Some(col) = col {
                let col: Result<Stats> = col.try_into();
                match col {
                    Ok(col) => Some(Ok((header, col))),
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(columns.into_iter().collect())
}

pub fn parse_reader(
    reader: impl Read,
    is_exact_median: bool,
) -> Result<Vec<(String, Option<ColStats>)>> {
    let mut csv_reader = Reader::from_reader(reader);

    let headers: Vec<String> = csv_reader
        .headers()?
        .iter()
        .map(ToOwned::to_owned)
        .collect();

    let mut column_stats: Vec<Option<ColStats>> = Vec::with_capacity(headers.len());
    for _ in 0..headers.len() {
        column_stats.push(None);
    }

    for (row_index, row) in csv_reader.byte_records().enumerate() {
        let row: csv::ByteRecord = row?;

        for (field_index, field) in row.iter().enumerate() {
            if is_empty(field) {
                continue;
            }
            parse_column(
                field,
                field_index,
                row_index,
                is_exact_median,
                &mut column_stats,
            )?;
        }
    }

    Ok(headers.into_iter().zip(column_stats).collect())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use crate::DEFAULT_MEMORY_BUDGET;

    use super::*;
    use csv::WriterBuilder;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        id: u32,
        value1: u32,
        value2: u32,
    }

    fn build_test_set() -> String {
        let mut wtr = WriterBuilder::new().from_writer(vec![]);
        wtr.serialize(Row {
            id: 1,
            value1: 10,
            value2: 20,
        })
        .unwrap();
        wtr.serialize(Row {
            id: 2,
            value1: 30,
            value2: 40,
        })
        .unwrap();
        wtr.serialize(Row {
            id: 3,
            value1: 25,
            value2: 35,
        })
        .unwrap();

        String::from_utf8(wtr.into_inner().unwrap()).unwrap()
    }

    #[test]
    fn test_parse_reader() {
        let cursor = Cursor::new(build_test_set());
        let reader = BufReader::new(cursor);

        let result = parse_reader(reader, true).unwrap();

        assert_eq!(result.len(), 3);

        match result.first().unwrap().1.as_ref() {
            Some(stat) => {
                assert_eq!(stat.min, Some(1));
                assert_eq!(stat.max, Some(3));
                assert_eq!(stat.sum, 6);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(2.));
            }
            None => panic!("Stat should be initialized"),
        }

        match result.get(1).unwrap().1.as_ref() {
            Some(stat) => {
                assert_eq!(stat.min, Some(10));
                assert_eq!(stat.max, Some(30));
                assert_eq!(stat.sum, 65);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(25.));
            }
            None => panic!("Stat should be initialized"),
        }

        match result.get(2).unwrap().1.as_ref() {
            Some(stat) => {
                assert_eq!(stat.min, Some(20));
                assert_eq!(stat.max, Some(40));
                assert_eq!(stat.sum, 95);
                assert_eq!(stat.count, 3);
                assert_eq!(stat.median_approach.calculate().unwrap(), Some(35.));
            }
            None => panic!("Stat should be initialized"),
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
            parse_file(PathBuf::from(temp_file.path()), DEFAULT_MEMORY_BUDGET).unwrap();

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
}
