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
    parse_reader(reader, is_exact_median)
}

pub fn parse_reader(reader: impl Read, is_exact_median: bool) -> Result<Output> {
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

    let columns = headers
        .into_iter()
        .zip(column_stats.into_iter())
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
