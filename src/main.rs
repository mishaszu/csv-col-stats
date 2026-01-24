use std::thread;

use clap::Parser;
use csv_col_stats::{CsvColError, CsvColStatsArgs, TableView, parse_file};
use tabled::{
    Table,
    settings::{Alignment, Style, object::Columns},
};

fn main() {
    let args = CsvColStatsArgs::parse();

    // TODO: binary could be optimize to accept either path to files or IO read
    if args.files.is_empty() {
        panic!("no csv file provided");
    }

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

    result.into_iter().for_each(|output| {
        let output = output.unwrap();

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
