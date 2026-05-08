"""
main.py — CLI entry point
Stock Market Analytics Dashboard: DuckDB vs MySQL Benchmark

Usage:
    # Default 5 stocks, full pipeline
    python main.py

    # Extended pool (30 stocks) for a larger dataset
    python main.py --symbols extended --start 2010-01-01

    # Custom symbols
    python main.py --symbols AAPL MSFT TSLA --start 2020-01-01 --end 2025-01-01

    # Only run benchmarks (skip download, use cache)
    python main.py --skip-download --runs 7

    # Only run EXPLAIN
    python main.py --skip-download --skip-benchmark --explain-only

    # Skip MySQL (DuckDB only)
    python main.py --no-mysql

    # Pick specific queries
    python main.py --queries Q1_50day_MA Q4_full_scan_narrow Q6_wide_projection
"""

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from downloader import fetch_stock_data, DEFAULT_SYMBOLS, EXTENDED_SYMBOLS
from db_run import (
    setup_duckdb, setup_mysql,
    run_benchmark, save_results,
    BENCHMARK_QUERIES,
)
from explain_analyzer import generate_explain_report

console = Console()


def parse_args():
    parser = argparse.ArgumentParser(
        description="DuckDB vs MySQL — Stock Analytics Benchmark CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                                    # default 5 stocks, full pipeline
  python main.py --symbols extended                 # 30 stocks, larger dataset
  python main.py --symbols AAPL TSLA --runs 10      # custom symbols, 10 runs
  python main.py --skip-download --explain-only     # only generate EXPLAIN report
  python main.py --no-mysql                         # skip MySQL
  python main.py --queries Q1_50day_MA Q6_wide_projection   # specific queries
        """,
    )

    # Data config
    data_group = parser.add_argument_group("Data config")
    data_group.add_argument(
        "--symbols", nargs="+", default=None,
        help="Ticker list, or 'default' (5 stocks) / 'extended' (30 stocks)",
    )
    data_group.add_argument("--start", default="2015-01-01", help="Start date (YYYY-MM-DD)")
    data_group.add_argument("--end",   default="2025-01-01", help="End date (YYYY-MM-DD)")

    # Benchmark config
    bench_group = parser.add_argument_group("Benchmark config")
    bench_group.add_argument(
        "--runs", type=int, default=5,
        help="Runs per query (first one is warm-up and discarded; median taken; default=5)",
    )
    bench_group.add_argument(
        "--queries", nargs="+", default=None,
        help=f"Query IDs to run (defaults to all). Available: {list(BENCHMARK_QUERIES.keys())}",
    )

    # Flow control
    flow_group = parser.add_argument_group("Flow control")
    flow_group.add_argument("--skip-download",  action="store_true", help="Skip download, use cached data")
    flow_group.add_argument("--skip-benchmark", action="store_true", help="Skip benchmarks")
    flow_group.add_argument("--skip-explain",   action="store_true", help="Skip EXPLAIN report")
    flow_group.add_argument("--explain-only",   action="store_true", help="Only run EXPLAIN (equivalent to --skip-benchmark)")
    flow_group.add_argument("--no-mysql",       action="store_true", help="Skip MySQL; benchmark DuckDB only")

    # Paths
    path_group = parser.add_argument_group("Paths")
    path_group.add_argument("--data-dir",   default="data",   help="Data directory (default ./data)")
    path_group.add_argument("--output-dir", default="output", help="Output directory (default ./output)")

    return parser.parse_args()


def resolve_symbols(symbols_arg: list[str] | None) -> list[str]:
    """Convert CLI symbols arg into a concrete ticker list."""
    if symbols_arg is None:
        return DEFAULT_SYMBOLS

    if len(symbols_arg) == 1:
        kw = symbols_arg[0].lower()
        if kw == "default":
            return DEFAULT_SYMBOLS
        elif kw == "extended":
            return EXTENDED_SYMBOLS

    return [s.upper() for s in symbols_arg]


def main():
    args = parse_args()

    data_dir   = Path(args.data_dir).resolve()
    output_dir = Path(args.output_dir).resolve()
    duckdb_file = str(data_dir / "stock_analytics.duckdb")

    symbols = resolve_symbols(args.symbols)

    # ── Banner ──
    console.print(Panel(
        "[bold]Stock Market Analytics Dashboard[/bold]\n"
        "DuckDB (Columnar + Vectorized) vs MySQL (Row-Based)\n"
        f"\n  Symbols:  {len(symbols)} stocks — {symbols[:5]}{'...' if len(symbols) > 5 else ''}"
        f"\n  Period:   {args.start} -> {args.end}"
        f"\n  Runs:     {args.runs} per query"
        f"\n  MySQL:    {'SKIP' if args.no_mysql else 'ON'}"
        f"\n  Data:     {data_dir}"
        f"\n  Output:   {output_dir}",
        title="DSCI 551 Project — Benchmark CLI",
        expand=False,
    ))

    # ── Step 1: Load data ──
    if args.skip_download:
        console.print("\n[yellow][*] Skipping download, trying local cache...[/yellow]")
    df = fetch_stock_data(symbols, args.start, args.end, data_dir)
    console.print(f"[green]  Dataset: {len(df)} rows x {df.shape[1]} cols, {df['Symbol'].nunique()} stocks[/green]")

    # ── Step 2: Initialize databases ──
    duckdb_con = setup_duckdb(df, duckdb_file)

    mysql_engine = None
    if not args.no_mysql:
        mysql_engine = setup_mysql(df)
        if mysql_engine is None:
            console.print("[yellow]  MySQL unavailable, will only benchmark DuckDB[/yellow]")

    # ── Step 3: Benchmark ──
    if not args.skip_benchmark and not args.explain_only:
        console.print("\n[cyan]{'='*50}[/cyan]")
        console.print("[bold cyan]  Phase: Benchmark[/bold cyan]")
        console.print(f"[cyan]{'='*50}[/cyan]")

        results, session_ts = run_benchmark(
            duckdb_con, mysql_engine,
            n_runs=args.runs,
            query_ids=args.queries,
            output_dir=output_dir,
        )
        save_results(results, session_ts, output_dir)
    else:
        console.print("\n[yellow][*] Skipping benchmark[/yellow]")

    # ── Step 4: EXPLAIN ANALYZE ──
    if not args.skip_explain:
        console.print(f"\n[cyan]{'='*50}[/cyan]")
        console.print("[bold cyan]  Phase: EXPLAIN ANALYZE[/bold cyan]")
        console.print(f"[cyan]{'='*50}[/cyan]")

        explain_queries = args.queries if args.queries else None
        generate_explain_report(duckdb_con, mysql_engine, explain_queries, output_dir)
    else:
        console.print("\n[yellow][*] Skipping EXPLAIN report[/yellow]")

    # ── Done ──
    console.print(Panel(
        f"[bold green]All done![/bold green]\n"
        f"  Output: {output_dir}/",
        expand=False,
    ))


if __name__ == "__main__":
    main()
