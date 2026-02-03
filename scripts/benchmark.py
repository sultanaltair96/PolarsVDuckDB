import time
import psutil
import gc
from pathlib import Path
import pandas as pd
import polars as pl
import duckdb
import daft

def get_memory_usage():
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024  # MB

def benchmark_library(library_name, file_path, iterations=10):
    results = []
    for i in range(iterations):
        gc.collect()
        start_mem = get_memory_usage()
        start_time = time.perf_counter()

        try:
            if library_name == 'pandas':
                df = pd.read_parquet(file_path)
            elif library_name == 'polars':
                df = pl.read_parquet(file_path)
            elif library_name == 'duckdb':
                conn = duckdb.connect()
                df = conn.execute(f"SELECT * FROM read_parquet('{file_path}')").df()
                conn.close()
            elif library_name == 'daft':
                df = daft.read_parquet(file_path).collect().to_pandas()

            load_time = time.perf_counter() - start_time
            peak_mem = get_memory_usage()
            memory_used = peak_mem - start_mem

            results.append({
                'iteration': i + 1,
                'library': library_name,
                'file': Path(file_path).name,
                'load_time_sec': load_time,
                'memory_mb': memory_used
            })
            print(f"{library_name} iteration {i+1}: {load_time:.2f}s, {memory_used:.2f}MB")

        except Exception as e:
            print(f"Error in {library_name} iteration {i+1}: {e}")
            results.append({
                'iteration': i + 1,
                'library': library_name,
                'file': Path(file_path).name,
                'load_time_sec': None,
                'memory_mb': None
            })

    return results

def main():
    Path("results").mkdir(exist_ok=True)
    files = ["data/sales_100k.parquet", "data/sales_1000k.parquet"]
    libraries = ['pandas', 'polars', 'duckdb', 'daft']
    all_results = []

    for file_path in files:
        if not Path(file_path).exists():
            print(f"File {file_path} not found, skipping.")
            continue
        for lib in libraries:
            print(f"Benchmarking {lib} on {file_path}")
            results = benchmark_library(lib, file_path)
            all_results.extend(results)

    df_results = pd.DataFrame(all_results)
    df_results.to_parquet("results/benchmark_timings_detailed.parquet", index=False)
    print("Benchmark results saved to results/benchmark_timings_detailed.parquet")

if __name__ == "__main__":
    main()