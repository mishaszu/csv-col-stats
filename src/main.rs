use std::thread;

use clap::Parser;
use csv_col_stats::{Config, CsvColError, CsvColStatsArgs, TableView, parse_file};
use tabled::{
    Table,
    settings::{Alignment, Style, object::Columns},
};

fn main() {
    let args = CsvColStatsArgs::parse();

    // TODO: binary could be optimize to accept either path to files or IO read
    if args.files.is_empty() {
        panic!("No CSV file provided, run with \"-h\" for help");
    }

    // TODO: it's naive approach. It should balance budget per file
    let budget_per_file = args.memory_budget / args.files.len();
    let mut config: Config = Config::from(&args);
    config.median_config.memory_budget = budget_per_file;

    let mut handlers = Vec::new();
    for file in args.files.clone() {
        let config = config.clone();
        handlers.push(thread::spawn(move || parse_file(file, config)));
    }

    let mut result = Vec::new();
    for handler in handlers {
        match handler.join() {
            Ok(output) => result.push(output),
            Err(_) => result.push(Err(CsvColError::ThreadPanic)),
        };
    }

    result.into_iter().enumerate().for_each(|(index, output)| {
        let output = output.unwrap();

        println!("File: {:?}", args.files[index].as_os_str());
        // TODO: multiple file support for correct JSON output requires redesign
        if args.json {
            let serialized_file = serde_json::to_string(&output).unwrap();
            println!("{serialized_file}");
        } else if args.table {
            let mut rows: Vec<TableView> = output
                .into_iter()
                .map(|v| -> TableView { v.into() })
                .collect();
            rows.sort_by(|a, b| a.column_name.cmp(&b.column_name));
            let mut table = Table::new(rows);
            table.with(Style::modern());
            table.modify(Columns::first(), Alignment::right());

            println!("{table}");
        } else {
            println!("{output:?}");
        }
    });
}
