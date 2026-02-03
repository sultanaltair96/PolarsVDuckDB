from jinja2 import Template
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

def generate_report():
    # Load aggregated data
    agg_df = pd.read_parquet("results/benchmark_aggregated.parquet")

    # Summary metrics
    max_rows = agg_df['num_rows'].max()
    final_data = agg_df[agg_df['num_rows'] == max_rows]
    avg_time = final_data['median_load_time_sec'].mean()
    avg_mem = final_data['median_memory_mb'].mean()

    # Generate charts as HTML
    # Scaling load time
    fig_time = px.line(agg_df, x='num_rows', y='median_load_time_sec', color='library',
                       title='Scaling: Median Load Time vs Rows', markers=True)
    fig_time.update_layout(xaxis_title='Rows', yaxis_title='Time (sec)', xaxis_type='log')
    chart_time_html = fig_time.to_html(full_html=False, include_plotlyjs='cdn')

    # Scaling memory
    fig_mem = px.line(agg_df, x='num_rows', y='median_memory_mb', color='library',
                      title='Scaling: Median Memory vs Rows', markers=True)
    fig_mem.update_layout(xaxis_title='Rows', yaxis_title='Memory (MB)', xaxis_type='log')
    chart_mem_html = fig_mem.to_html(full_html=False, include_plotlyjs='cdn')

    # Final bar chart
    fig_bar = px.bar(final_data, x='library', y='median_load_time_sec',
                      title=f'Final Benchmark at {max_rows:,} Rows', color='library')
    fig_bar.update_layout(yaxis_title='Time (sec)')
    chart_bar_html = fig_bar.to_html(full_html=False, include_plotlyjs='cdn')

    # Table HTML
    table_html = agg_df.to_html(index=False, classes='table-auto border-collapse border border-gray-400 dark:border-gray-600 w-full text-sm')

    # Jinja template
    template_str = """
<!DOCTYPE html>
<html lang="en" class="light" x-data="{ darkMode: false, filterLib: '' }" x-init="$watch('darkMode', val => document.documentElement.classList.toggle('dark', val))">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Benchmark Report</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="//unpkg.com/alpinejs" defer></script>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .animate-fade-in { animation: fadeIn 1s ease-in; }
        @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
        @media print {
            nav, button, .no-print { display: none !important; }
            body { font-size: 12px; }
        }
        .dark { background-color: #1a202c; color: #e2e8f0; }
        table tr:hover { background-color: #f7fafc; }
        .dark table tr:hover { background-color: #2d3748; }
    </style>
</head>
<body class="bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100 min-h-screen animate-fade-in">
    <header class="bg-blue-600 text-white p-4 shadow-lg">
        <h1 class="text-3xl font-bold">Polars vs DuckDB vs Pandas vs Daft: Parquet Loading Benchmark</h1>
        <div class="flex justify-between items-center mt-2">
            <p class="text-sm">Stress test up to {{ max_rows_str }} rows</p>
            <button @click="darkMode = !darkMode" class="bg-gray-800 hover:bg-gray-700 px-4 py-2 rounded transition-colors no-print" aria-label="Toggle Dark Mode">Toggle Dark Mode</button>
        </div>
    </header>
    <nav class="sticky top-0 bg-white dark:bg-gray-800 shadow p-4 no-print z-10">
        <ul class="flex space-x-4">
            <li><a href="#summary" class="text-blue-600 hover:underline">Summary</a></li>
            <li><a href="#scaling" class="text-blue-600 hover:underline">Scaling Analysis</a></li>
            <li><a href="#final" class="text-blue-600 hover:underline">Final Results</a></li>
            <li><a href="#table" class="text-blue-600 hover:underline">Detailed Table</a></li>
        </ul>
    </nav>
    <main class="container mx-auto p-6 space-y-8">
        <section id="summary" class="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 class="text-2xl font-semibold mb-4">Executive Summary</h2>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div class="bg-blue-100 dark:bg-blue-900 p-4 rounded">
                    <h3 class="font-bold">Max Rows Tested</h3>
                    <p class="text-2xl">{{ max_rows_str }}</p>
                </div>
                <div class="bg-green-100 dark:bg-green-900 p-4 rounded">
                    <h3 class="font-bold">Avg Load Time</h3>
                    <p class="text-2xl">{{ "%.2f"|format(avg_time) }}s</p>
                </div>
                <div class="bg-yellow-100 dark:bg-yellow-900 p-4 rounded">
                    <h3 class="font-bold">Avg Memory</h3>
                    <p class="text-2xl">{{ "%.2f"|format(avg_mem) }} MB</p>
                </div>
            </div>
            <p class="mt-4">This report benchmarks data loading performance across scales. Polars excels in speed, while memory usage varies.</p>
        </section>
        <section id="scaling" class="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 class="text-2xl font-semibold mb-4">Scaling Analysis</h2>
            <div class="mb-6">
                {{ chart_time_html | safe }}
            </div>
            <div>
                {{ chart_mem_html | safe }}
            </div>
        </section>
        <section id="final" class="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 class="text-2xl font-semibold mb-4">Final Benchmark Results</h2>
            {{ chart_bar_html | safe }}
        </section>
        <section id="table" class="bg-white dark:bg-gray-800 p-6 rounded-lg shadow-lg">
            <h2 class="text-2xl font-semibold mb-4">Detailed Results Table</h2>
            <input x-model="filterLib" type="text" placeholder="Filter by library..." class="border p-2 rounded w-full mb-4 dark:bg-gray-700 dark:border-gray-600">
            <div class="overflow-x-auto">
                <table class="w-full text-left" x-show="!filterLib || row.library.toLowerCase().includes(filterLib.toLowerCase())" x-for="row in {{ table_data | tojson }}" :key="row.index">
                    <thead class="bg-gray-100 dark:bg-gray-700">
                        <tr>
                            <th class="border px-4 py-2">Library</th>
                            <th class="border px-4 py-2">Rows</th>
                            <th class="border px-4 py-2">Median Time (s)</th>
                            <th class="border px-4 py-2">Median Memory (MB)</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr x-for="row in [row]" class="hover:bg-gray-50 dark:hover:bg-gray-600">
                            <td class="border px-4 py-2" x-text="row.library"></td>
                            <td class="border px-4 py-2" x-text="row.num_rows"></td>
                            <td class="border px-4 py-2" x-text="row.median_load_time_sec.toFixed(2)"></td>
                            <td class="border px-4 py-2" x-text="row.median_memory_mb.toFixed(2)"></td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </section>
    </main>
    <footer class="bg-gray-200 dark:bg-gray-800 text-center p-4 text-sm">
        <p>Generated on {{ date }} | Data loading benchmark for large-scale analysis</p>
    </footer>
</body>
</html>
    """

    template = Template(template_str)
    table_data = agg_df.to_dict('records')
    rendered = template.render(
        max_rows_str=f"{max_rows:,}",
        avg_time=avg_time,
        avg_mem=avg_mem,
        chart_time_html=chart_time_html,
        chart_mem_html=chart_mem_html,
        chart_bar_html=chart_bar_html,
        table_data=table_data,
        date=pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    )

    with open("results/benchmark_report_polished.html", 'w') as f:
        f.write(rendered)

    print("Polished HTML report generated: results/benchmark_report_polished.html")

if __name__ == "__main__":
    generate_report()