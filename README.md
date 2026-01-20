# CSV Column Stats Parser

This CLI provides basic statistics (**mean, median, min, max**) for each numeric column
in a given CSV file.

---

## Reasoning about the solution

The design balances **CPU-bound parsing** and **I/O costs**.

In the context of this tool:

- The CLI is expected to process **a small number of CSV files** (usually just a few),
  meaning only a few file buffers are open at once.
- With a small number of files, **CSV parsing and aggregation are CPU-bound**, not I/O-bound.
- For few files, a **thread-per-file** model is simple and sufficient.
- If the number of input files grows, concurrency should be **explicitly capped**
  (e.g. using a thread pool or Rayon).
- Because I/O is limited and mostly sequential, introducing an **async** runtime
  (e.g. Tokio) would add complexity without meaningful performance benefits.

---

## Edge cases

- **Numeric type promotion**  
  A column may contain only integer values in early rows and floating-point
  values later. In this case the column must be **promoted from `i64` to `f64`**
  without losing previously accumulated statistics.

- **Early misclassification of column types**  
  Determining whether a column is numeric based only on the first data row
  (after the header) can be misleading if that row contains malformed,
  missing, or non-numeric values. Column type detection should therefore be
  resilient to sparse or invalid early rows.


---

## Problems

- **Calculating avg (mean)**
  Mean can be computed in one pass using `sum` and `count` (`mean = sum / count`).
  2 problems might occure:

  - **Overflow / range limits**
    - For integer columns, summing into `i64` can overflow. 
    - For floating-point columns, the running sum can overflow to `+∞/-∞`
      if values are very large or the dataset is huge; this should be detected
      (`sum.is_finite()`).

  - **Floating-point accuracy**
    Naive summation can accumulate rounding error. 

- **Calculating median**
  Exact median requires knowledge of the global ordering of values.
  It is possible in a single pass only if we store values (or an equivalent data structure),
  which can be expensive for very large files.

  Approaches:
  - **Exact median in memory (store + sort)**  
    Simplest and fully accurate but expensive in terms of memory. With multiple columns and multiple files approach like external sort could be used but might be disk heavy.
  - **Two-heap streaming median (exact)**  
    More elegant approach then simple exact median, can be completed in one go but for large files also could require external memory managment.
  - **Approximate median (streaming)**  
    Uses bounded memory with a controllable accuracy/size trade-off

---

## Considerations

This is a **playground / learning project**.  
The architecture is intentionally simple and optimized for clarity, with the goal of
building a complete, working solution within a few hours.

---

## Approach

### Concurrency model
- Use `std::thread` with a **simple, uncapped thread-per-file approach**
- Threads are joined after spawning; all parsing runs in parallel
- The model can later be replaced with a capped pool or Rayon if needed

### CSV parsing
- Use the [`csv`](https://docs.rs/csv) crate
- Read records using [`ByteRecord`](https://docs.rs/csv/latest/csv/struct.ByteRecord.html)
  to avoid per-row `String` allocations
- `Reader::from_reader(BufReader<File>)` is used to stream data incrementally
  (the entire file is not loaded into memory)
- Multiline quoted fields are handled by the CSV parser itself

### Numeric parsing
- Attempt to parse numeric fields dynamically (`i64` or `f64`)
- For simplicity, use `str::parse::<i64>()` / `str::parse::<f64>()`
- (optimizatoin) For large files or performance-critical use cases, faster alternatives
  like `lexical-core` or `fast-float` could be substituted

### Calculating Mean
- naive approach for mean calculation based on sum / count
- (optimization) handle overflow
- (optimization) floating point precision

### Calculating Median
- to fit solution in few hours I pick tdigest for approximate median
- (optimization) use configurable memory cap to use two-heap streaming for exact median and switch to approximate median after exceeding the cap
- (optimization) use external memory handling for two-heap streaming median and make it configurable for exact or approximate median

### Output
- default: Debug output
- `serde` & `serde_json` for `--json` flag

### Other library picks:
- `clap` for handling args
- `thiserror` for simplifying error handling
- (optimization) `indicatif` for progress bars
- (optimization) `tempfile` for external memory handling

### Other tools:
- (optimization) `criterion` for benchmarking
- (optimization) `flamegraph` for profiling
