use std::{collections::HashMap, path::PathBuf, thread};

use clap::Parser;

use crate::parser::parse_file;

mod error;
mod parser;

pub use error::{CsvColError, Result};

const DEFAULT_MEMORY_BUDGET: usize = 256 * 1024 * 1024;

#[derive(Debug, Parser)]
struct Args {
    /// Output in json format
    #[arg(short, long)]
    json: bool,

    /// Memory budget in bytes after which approximate median will be used
    #[arg(long, default_value_t=DEFAULT_MEMORY_BUDGET)]
    memory_budget: usize,

    #[arg(value_name = "FILE", num_args = 1..)]
    files: Vec<PathBuf>,
}

pub fn run_csv_col_stats() -> Vec<Result<Output>> {
    let args = Args::parse();

    // TODO: it's naive approach. It should balance budget per file
    let budget_per_file = args.memory_budget / args.files.len();
    let mut handlers = Vec::new();
    for file in args.files {
        handlers.push(thread::spawn(move || parse_file(file, budget_per_file)));
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

#[derive(Debug, Default, PartialEq)]
pub struct Stats {
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub mean: Option<f64>,
    pub median: Option<f64>,
}

pub type Output = HashMap<String, Stats>;
