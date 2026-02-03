# Polars vs DuckDB vs Pandas vs Daft: Parquet Loading Benchmark

This project benchmarks the performance of loading Parquet files with Polars, DuckDB, Pandas, and Daft. It measures load time and memory usage for 100k and 1M rows of synthetic e-commerce data.

## Setup
1. Clone the repo: `git clone https://github.com/sultanaltair96/PolarsVDuckDB.git`
2. Create venv: `python3 -m venv venv`
3. Activate: `source venv/bin/activate`
4. Install deps: `pip install -r requirements.txt`
5. Run data generation: `python scripts/data_generator.py`
6. Run benchmarks: `python scripts/benchmark.py`
7. Analyze: `python scripts/analyzer.py`

## Results
- Detailed timings: `results/benchmark_timings_detailed.parquet`
- Aggregated medians: `results/benchmark_aggregated.parquet`
- Chart: `results/benchmark_chart.png`

Preliminary findings: Polars is fastest, followed by Daft and Pandas. DuckDB is slower but SQL-friendly.

## Medium Article Hook
This benchmark reveals surprising performance differences in modern data libraries. Polars' Rust core shines for pure loading, while Daft's distributed nature offers scalability. Stay tuned for full analysis!

## Dependencies
- polars
- duckdb
- pandas
- daft
- faker
- pyarrow
- matplotlib
- psutil