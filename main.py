"""
main.py — CLI 入口
Stock Market Analytics Dashboard: DuckDB vs MySQL Benchmark

Usage:
    # 默认 5 只股票全流程
    python main.py

    # 下载 30 只股票，扩大数据量
    python main.py --symbols extended --start 2010-01-01

    # 自定义股票
    python main.py --symbols AAPL MSFT TSLA --start 2020-01-01 --end 2025-01-01

    # 只跑 benchmark（跳过下载，用缓存）
    python main.py --skip-download --runs 7

    # 只跑 EXPLAIN
    python main.py --skip-download --skip-benchmark --explain-only

    # 跳过 MySQL（只测 DuckDB）
    python main.py --no-mysql

    # 指定运行哪些查询
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
示例:
  python main.py                                    # 默认 5 只股票，全流程
  python main.py --symbols extended                 # 30 只股票，大数据量
  python main.py --symbols AAPL TSLA --runs 10      # 自定义股票，10 次运行
  python main.py --skip-download --explain-only     # 只生成 EXPLAIN 报告
  python main.py --no-mysql                         # 跳过 MySQL
  python main.py --queries Q1_50day_MA Q6_wide_projection   # 只跑指定查询
        """,
    )

    # 数据配置
    data_group = parser.add_argument_group("数据配置")
    data_group.add_argument(
        "--symbols", nargs="+", default=None,
        help="股票代码列表，或 'default'(5只) / 'extended'(30只)",
    )
    data_group.add_argument("--start", default="2015-01-01", help="起始日期 (YYYY-MM-DD)")
    data_group.add_argument("--end",   default="2025-01-01", help="结束日期 (YYYY-MM-DD)")

    # Benchmark 配置
    bench_group = parser.add_argument_group("Benchmark 配置")
    bench_group.add_argument(
        "--runs", type=int, default=5,
        help="每条查询运行次数 (第 1 次 warm-up 丢弃, 取后面中位数, 默认=5)",
    )
    bench_group.add_argument(
        "--queries", nargs="+", default=None,
        help=f"运行哪些查询 ID (默认全部), 可选: {list(BENCHMARK_QUERIES.keys())}",
    )

    # 流程控制
    flow_group = parser.add_argument_group("流程控制")
    flow_group.add_argument("--skip-download",  action="store_true", help="跳过下载，直接用缓存数据")
    flow_group.add_argument("--skip-benchmark", action="store_true", help="跳过 benchmark")
    flow_group.add_argument("--skip-explain",   action="store_true", help="跳过 EXPLAIN 报告")
    flow_group.add_argument("--explain-only",   action="store_true", help="只跑 EXPLAIN (等价于 --skip-benchmark)")
    flow_group.add_argument("--no-mysql",       action="store_true", help="跳过 MySQL，只测 DuckDB")

    # 路径
    path_group = parser.add_argument_group("路径")
    path_group.add_argument("--data-dir",   default="data",   help="数据目录 (默认 ./data)")
    path_group.add_argument("--output-dir", default="output", help="输出目录 (默认 ./output)")

    return parser.parse_args()


def resolve_symbols(symbols_arg: list[str] | None) -> list[str]:
    """将 CLI 参数转换为股票代码列表"""
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
        f"\n  Period:   {args.start} → {args.end}"
        f"\n  Runs:     {args.runs} per query"
        f"\n  MySQL:    {'SKIP' if args.no_mysql else 'ON'}"
        f"\n  Data:     {data_dir}"
        f"\n  Output:   {output_dir}",
        title="DSCI 551 Project — Benchmark CLI",
        expand=False,
    ))

    # ── Step 1: 下载数据 ──
    if args.skip_download:
        console.print("\n[yellow][*] 跳过下载，尝试读取缓存...[/yellow]")
    df = fetch_stock_data(symbols, args.start, args.end, data_dir)
    console.print(f"[green]  数据规模: {len(df)} 行 × {df.shape[1]} 列, {df['Symbol'].nunique()} 只股票[/green]")

    # ── Step 2: 初始化数据库 ──
    duckdb_con = setup_duckdb(df, duckdb_file)

    mysql_engine = None
    if not args.no_mysql:
        mysql_engine = setup_mysql(df)
        if mysql_engine is None:
            console.print("[yellow]  MySQL 不可用，将只对比 DuckDB[/yellow]")

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
        console.print("\n[yellow][*] 跳过 Benchmark[/yellow]")

    # ── Step 4: EXPLAIN ANALYZE ──
    if not args.skip_explain:
        console.print(f"\n[cyan]{'='*50}[/cyan]")
        console.print("[bold cyan]  Phase: EXPLAIN ANALYZE[/bold cyan]")
        console.print(f"[cyan]{'='*50}[/cyan]")

        explain_queries = args.queries if args.queries else None
        generate_explain_report(duckdb_con, mysql_engine, explain_queries, output_dir)
    else:
        console.print("\n[yellow][*] 跳过 EXPLAIN 报告[/yellow]")

    # ── Done ──
    console.print(Panel(
        f"[bold green]✓ 全部完成![/bold green]\n"
        f"  查看输出: {output_dir}/",
        expand=False,
    ))


if __name__ == "__main__":
    main()
