use csv_col_stats::run_csv_col_stats;

fn main() {
    let result = run_csv_col_stats();

    result.iter().for_each(|output| println!("{output:#?}"));
}
