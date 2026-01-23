use csv_col_stats::run_csv_col_stats;

fn main() {
    let result = run_csv_col_stats();

    result.into_iter().for_each(|output| {
        let output = output.unwrap();
        println!("{output:#?}")
    });
}
