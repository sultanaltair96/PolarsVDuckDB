import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def analyze_results():
    df = pd.read_parquet("results/benchmark_timings_detailed.parquet")

    # Filter out errors
    df = df.dropna()

    # Group by library and file, compute medians
    aggregated = df.groupby(['library', 'file']).agg({
        'load_time_sec': 'median',
        'memory_mb': 'median'
    }).reset_index()
    aggregated.columns = ['library', 'file', 'median_load_time_sec', 'median_memory_mb']
    aggregated.to_parquet("results/benchmark_aggregated.parquet", index=False)

    # Plot bar chart for load times
    plt.figure(figsize=(10, 6))
    for file in df['file'].unique():
        subset = aggregated[aggregated['file'] == file]
        plt.bar(subset['library'] + f" ({file})", subset['median_load_time_sec'], label=file)

    plt.xlabel('Library and Dataset')
    plt.ylabel('Median Load Time (seconds)')
    plt.title('Benchmark: Median Load Times by Library')
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig("results/benchmark_chart.png")
    plt.show()

    print("Analysis complete. Aggregated results saved to results/benchmark_aggregated.parquet")
    print("Chart saved to results/benchmark_chart.png")

if __name__ == "__main__":
    analyze_results()