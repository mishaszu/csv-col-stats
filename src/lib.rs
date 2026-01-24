use std::{collections::HashMap, path::PathBuf};

use clap::Parser;
use serde::Serialize;

mod error;
mod filter;
mod parser;

pub use error::{CsvColError, Result};
pub use parser::parse_file;
use tabled::Tabled;

use crate::filter::Expression;

const DEFAULT_MEMORY_BUDGET: usize = 256 * 1024 * 1024;

#[derive(Debug, Parser)]
pub struct CsvColStatsArgs {
    // Display out as table
    #[arg(short, long)]
    pub table: bool,

    /// Format output to json. Can be display with `jq` for example
    /// Overwrites `table`
    #[arg(short, long)]
    pub json: bool,

    #[arg(short, long, default_value = "id")]
    pub ignore_columns: Vec<String>,

    #[arg(short, long)]
    pub filter: Option<Expression>,

    /// Memory budget in bytes after which approximate median will be used
    #[arg(long, default_value_t=DEFAULT_MEMORY_BUDGET)]
    pub memory_budget: usize,

    #[arg(long)]
    pub approximate_bins: Option<u32>,

    #[arg(value_name = "FILE", num_args = 1..)]
    pub files: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct MedianConfig {
    pub memory_budget: usize,
    pub buckets: Option<u32>,
    pub exact_median: bool,
}

impl Default for MedianConfig {
    fn default() -> Self {
        Self {
            memory_budget: DEFAULT_MEMORY_BUDGET,
            buckets: None,
            exact_median: true,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DataConfig {
    filter: Option<Expression>,
    ignore_columns: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    data_config: DataConfig,
    pub median_config: MedianConfig,
}

impl From<&CsvColStatsArgs> for Config {
    fn from(args: &CsvColStatsArgs) -> Self {
        Self {
            data_config: DataConfig {
                filter: args.filter.clone(),
                ignore_columns: args.ignore_columns.clone(),
            },
            median_config: MedianConfig {
                memory_budget: args.memory_budget,
                buckets: args.approximate_bins,
                exact_median: true,
            },
        }
    }
}

// TODO: implement Display for Stats
#[derive(Debug, Default, PartialEq, Serialize)]
pub struct Stats {
    pub min: Option<i64>,
    pub max: Option<i64>,
    pub mean: Option<f64>,
    pub median: Option<f64>,
}

pub type Output = HashMap<String, Stats>;

#[derive(Tabled)]
pub struct TableView {
    pub column_name: String,
    min: String,
    max: String,
    mean: String,
    median: String,
}

impl From<(String, Stats)> for TableView {
    fn from((column_name, stats): (String, Stats)) -> Self {
        Self {
            column_name,
            min: display_opt_num(&stats.min),
            max: display_opt_num(&stats.max),
            mean: display_opt_num(&stats.mean),
            median: display_opt_num(&stats.median),
        }
    }
}

fn display_opt_num(value: &Option<impl ToString>) -> String {
    value
        .as_ref()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "N/A".to_string())
}
