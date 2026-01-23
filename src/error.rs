use std::path::PathBuf;

use sketches_ddsketch::DDSketchError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CsvColError {
    #[error("filed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Column failed to parse number on row {0} and field {1}: {2}")]
    ColumnParse(usize, usize, #[source] lexical_core::Error),

    #[error("DDSketch error: {0}")]
    DDSketch(#[from] DDSketchError),

    #[error("Can't parse CSV")]
    CsvParse(#[from] csv::Error),

    #[error("Thread paniced")]
    ThreadPanic,
}

pub type Result<T> = std::result::Result<T, CsvColError>;
