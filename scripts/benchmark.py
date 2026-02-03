import time
import psutil
import gc
from pathlib import Path
import pandas as pd
import polars as pl
import duckdb
import daft
import argparse

def get_memory_usage():
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024  # MB

def benchmark_library(library_name, file_path, iterations=5):  # Reduced to 5 for speed
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
            memory_used = max(0, peak_mem - start_mem)  # Ensure non-negative

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

def main(file_path):
    Path("results").mkdir(exist_ok=True)
    libraries = ['pandas', 'polars', 'duckdb', 'daft']
    all_results = []

    if not Path(file_path).exists():
        print(f"File {file_path} not found.")
        return

    for lib in libraries:
        print(f"Benchmarking {lib} on {file_path}")
        results = benchmark_library(lib, file_path)
        all_results.extend(results)

    # Save with file name
    file_name = Path(file_path).stem  # e.g., sales_100k
    output_path = f"results/benchmark_{file_name}_detailed.parquet"
    df_results = pd.DataFrame(all_results)
    df_results.to_parquet(output_path, index=False)
    print(f"Benchmark results saved to {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark libraries on a Parquet file")
    parser.add_argument("--file", type=str, required=True, help="Path to the Parquet file")
    args = parser.parse_args()
    main(args.file)