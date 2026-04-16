"""
run_all_experiments.py — 一键跑完所有实验，生成 demo 和 final report 所需的全部数据

Usage:
    python run_all_experiments.py                  # 全流程（需要 MySQL）
    python run_all_experiments.py --no-mysql        # 跳过 MySQL
    python run_all_experiments.py --skip-download   # 用缓存数据

实验清单:
    Exp 1: 小数据集 benchmark (5 stocks × 10y, ~12K rows)
    Exp 2: 大数据集 benchmark (30 stocks × 15y, ~100K rows)
    Exp 3: Storage size comparison
    Exp 4: DuckDB JSON profiling (operator-level timing)
    Exp 5: EXPLAIN ANALYZE report
"""

import argparse
import json
import csv
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

console = Console()

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR    = PROJECT_DIR / "data"
OUTPUT_DIR  = PROJECT_DIR / "output"


def main():
    parser = argparse.ArgumentParser(description="Run all experiments for DSCI 551 project")
    parser.add_argument("--no-mysql", action="store_true", help="Skip MySQL")
    parser.add_argument("--skip-download", action="store_true", help="Use cached data")
    parser.add_argument("--runs", type=int, default=7, help="Runs per query (default=7)")
    args = parser.parse_args()

    from downloader import fetch_stock_data, DEFAULT_SYMBOLS, EXTENDED_SYMBOLS
    from db_run import (
        setup_duckdb, setup_mysql, run_benchmark, save_results,
        compare_storage_sizes, run_json_profiling, run_scaling_experiment,
    )
    from explain_analyzer import generate_explain_report

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    n_runs = args.runs
    scaling_results = []

    # ==============================================================
    #  Experiment 1: Small dataset (5 stocks × 10y)
    # ==============================================================
    console.print(Rule("[bold] Experiment 1: Small Dataset (5 stocks × 10y) [/bold]", style="cyan"))

    df_small = fetch_stock_data(DEFAULT_SYMBOLS, "2015-01-01", "2025-01-01", DATA_DIR)
    console.print(f"[green]  {len(df_small):,} rows, {df_small['Symbol'].nunique()} stocks[/green]")

    duckdb_file_small = str(DATA_DIR / "stock_analytics_small.duckdb")
    duck_small = setup_duckdb(df_small, duckdb_file_small)

    mysql_engine = None
    if not args.no_mysql:
        mysql_engine = setup_mysql(df_small)
        if mysql_engine is None:
            console.print("[yellow]MySQL unavailable, continuing DuckDB only[/yellow]")

    results_small, ts_small = run_benchmark(
        duck_small, mysql_engine, n_runs=n_runs, output_dir=OUTPUT_DIR,
    )
    save_results(results_small, f"small_{ts_small}", OUTPUT_DIR)

    # Scaling data
    scaling_results.extend(
        run_scaling_experiment(duck_small, mysql_engine, n_runs=n_runs)
    )

    # ==============================================================
    #  Experiment 2: Large dataset (30 stocks × 15y)
    # ==============================================================
    console.print(Rule("[bold] Experiment 2: Large Dataset (30 stocks × 15y) [/bold]", style="cyan"))

    df_large = fetch_stock_data(EXTENDED_SYMBOLS, "2010-01-01", "2025-01-01", DATA_DIR)
    console.print(f"[green]  {len(df_large):,} rows, {df_large['Symbol'].nunique()} stocks[/green]")

    duckdb_file_large = str(DATA_DIR / "stock_analytics_large.duckdb")
    duck_large = setup_duckdb(df_large, duckdb_file_large)

    if not args.no_mysql:
        mysql_engine = setup_mysql(df_large)

    results_large, ts_large = run_benchmark(
        duck_large, mysql_engine, n_runs=n_runs, output_dir=OUTPUT_DIR,
    )
    save_results(results_large, f"large_{ts_large}", OUTPUT_DIR)

    # Scaling data
    scaling_results.extend(
        run_scaling_experiment(duck_large, mysql_engine, n_runs=n_runs)
    )

    # ==============================================================
    #  Experiment 3: Storage size comparison
    # ==============================================================
    console.print(Rule("[bold] Experiment 3: Storage Size Comparison [/bold]", style="cyan"))

    compare_storage_sizes(DATA_DIR, mysql_engine)

    # ==============================================================
    #  Experiment 4: DuckDB JSON profiling
    # ==============================================================
    console.print(Rule("[bold] Experiment 4: DuckDB JSON Profiling [/bold]", style="cyan"))

    # Profile on the large dataset for more meaningful results
    run_json_profiling(duck_large, output_dir=OUTPUT_DIR)

    # ==============================================================
    #  Experiment 5: EXPLAIN ANALYZE report
    # ==============================================================
    console.print(Rule("[bold] Experiment 5: EXPLAIN ANALYZE Report [/bold]", style="cyan"))

    generate_explain_report(duck_large, mysql_engine, output_dir=OUTPUT_DIR)

    # ==============================================================
    #  Save scaling results
    # ==============================================================
    console.print(Rule("[bold] Scaling Experiment Summary [/bold]", style="cyan"))

    scaling_path = OUTPUT_DIR / "scaling_results.csv"
    if scaling_results:
        with open(scaling_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=scaling_results[0].keys())
            writer.writeheader()
            writer.writerows(scaling_results)
        console.print(f"[green]  Scaling results → {scaling_path}[/green]")

        # Print scaling comparison table
        table = Table(title="Speedup Scaling: Small vs Large Dataset", show_lines=True)
        table.add_column("Query", style="bold")
        table.add_column("Small (~12K)\nSpeedup", justify="right")
        table.add_column("Large (~100K)\nSpeedup", justify="right")
        table.add_column("Trend", justify="center")

        # Group by query_id
        from collections import defaultdict
        by_query = defaultdict(list)
        for r in scaling_results:
            by_query[r["query_id"]].append(r)

        for qid, entries in by_query.items():
            entries.sort(key=lambda x: x["rows"])
            if len(entries) >= 2:
                sp_small = entries[0]["speedup"]
                sp_large = entries[-1]["speedup"]
                sp_s = f"{sp_small}x" if sp_small else "N/A"
                sp_l = f"{sp_large}x" if sp_large else "N/A"
                if sp_small and sp_large:
                    trend = "↑ DuckDB better" if sp_large > sp_small else "↓ or ≈"
                else:
                    trend = "N/A"
                table.add_row(qid, sp_s, sp_l, trend)

        console.print(table)

    # ==============================================================
    #  Done
    # ==============================================================
    console.print(Panel(
        "[bold green]All experiments complete![/bold green]\n\n"
        f"Output directory: {OUTPUT_DIR}/\n\n"
        "Generated files:\n"
        "  benchmark_results.csv     — cumulative benchmark data\n"
        "  benchmark_small_*.json    — small dataset benchmark\n"
        "  benchmark_large_*.json    — large dataset benchmark\n"
        "  scaling_results.csv       — scaling comparison data\n"
        "  profile_Q*.json           — DuckDB operator-level profiling\n"
        "  explain_report.txt        — EXPLAIN ANALYZE comparison\n\n"
        "Next steps:\n"
        "  1. python demo.py          — test the live demo\n"
        "  2. streamlit run dashboard.py  — test the web dashboard\n"
        "  3. Use output/ data for final report charts",
        title="Done",
        border_style="green",
        expand=False,
    ))


if __name__ == "__main__":
    main()
