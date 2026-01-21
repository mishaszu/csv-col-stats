use std::{collections::HashMap, path::PathBuf, thread};

use clap::Parser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CsvColError {
    #[error("filed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Thread paniced")]
    ThreadPanic,
}

type Result<T> = std::result::Result<T, CsvColError>;

#[derive(Debug, Default)]
pub struct Stats<T> {
    min: Option<T>,
    max: Option<T>,
    avg: f64,
    median: f64,
}

#[derive(Debug)]
pub enum NumberStats {
    Int(Stats<i64>),
    Float(Stats<f64>),
}

impl Default for NumberStats {
    fn default() -> Self {
        Self::Int(Stats::<i64>::default())
    }
}

#[derive(Default, Debug)]
pub struct Output(HashMap<String, NumberStats>);

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long)]
    json: bool,

    #[arg(value_name = "FILE", num_args = 1..)]
    files: Vec<PathBuf>,
}

fn parse_columns(file: PathBuf) -> Result<Output> {
    todo!()
}

pub fn run_csv_col_stats() -> Vec<Result<Output>> {
    let args = Args::parse();

    let mut handlers = Vec::new();
    for file in args.files {
        handlers.push(thread::spawn(move || parse_columns(file)));
    }

    let mut result = Vec::new();
    for handler in handlers {
        match handler.join() {
            Ok(output) => result.push(output),
            Err(_) => result.push(Err(CsvColError::ThreadPanic)),
        };
    }

    result
}
