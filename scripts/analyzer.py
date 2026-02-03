import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import glob
import plotly.express as px
import plotly.graph_objects as go

def analyze_results():
    Path("results").mkdir(exist_ok=True)
    # Load all benchmark detailed files
    pattern = "results/benchmark_sales_*_detailed.parquet"
    files = glob.glob(pattern)
    if not files:
        print("No benchmark files found.")
        return

    all_data = []
    for file in files:
        df = pd.read_parquet(file)
        # Extract num_rows from file name, e.g., sales_100k -> 100000
        file_name = Path(file).stem
        num_rows_str = file_name.split('_')[2]  # e.g., 100000
        if 'k' in num_rows_str:
            num_rows = int(num_rows_str.replace('k', '')) * 1000
        elif 'm' in num_rows_str.lower():
            num_rows = int(num_rows_str.replace('m', '').replace('M', '')) * 1000000
        else:
            num_rows = int(num_rows_str)
        df['num_rows'] = num_rows
        all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.dropna()  # Remove errors

    # Aggregate: median per library and num_rows
    aggregated = combined_df.groupby(['library', 'num_rows']).agg({
        'load_time_sec': 'median',
        'memory_mb': 'median'
    }).reset_index()
    aggregated.columns = ['library', 'num_rows', 'median_load_time_sec', 'median_memory_mb']
    aggregated.to_parquet("results/benchmark_aggregated.parquet", index=False)

    # Generate Plotly charts for scaling
    # Line chart: load time vs num_rows per library
    fig_time = px.line(aggregated, x='num_rows', y='median_load_time_sec', color='library',
                       title='Median Load Time vs Number of Rows', markers=True)
    fig_time.update_layout(xaxis_title='Number of Rows', yaxis_title='Median Load Time (sec)', xaxis_type='log')
    fig_time.write_html("results/scaling_load_time_chart.html")

    # Line chart: memory vs num_rows per library
    fig_mem = px.line(aggregated, x='num_rows', y='median_memory_mb', color='library',
                      title='Median Memory Usage vs Number of Rows', markers=True)
    fig_mem.update_layout(xaxis_title='Number of Rows', yaxis_title='Median Memory (MB)', xaxis_type='log')
    fig_mem.write_html("results/scaling_memory_chart.html")

    # Bar chart for final scale (max num_rows)
    max_rows = aggregated['num_rows'].max()
    final_agg = aggregated[aggregated['num_rows'] == max_rows]
    fig_bar = px.bar(final_agg, x='library', y='median_load_time_sec',
                      title=f'Benchmark at {max_rows} Rows: Median Load Times', color='library')
    fig_bar.update_layout(yaxis_title='Median Load Time (sec)')
    fig_bar.write_html("results/final_benchmark_chart.html")

    print("Analysis complete. Aggregated results and charts saved.")

if __name__ == "__main__":
    analyze_results()