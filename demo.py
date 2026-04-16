"""
demo.py — DSCI 551 Final Demo Script
Stock Market Analytics with DuckDB: Columnar Storage & Vectorized Execution

Usage:
    python demo.py                  # 全流程 demo (需要 MySQL)
    python demo.py --no-mysql       # 跳过 MySQL (仅 DuckDB)
    python demo.py --skip-download  # 跳过下载，用缓存

演示流程 (5-10 min):
    Step 0: 项目介绍 (口述)
    Step 1: 数据加载 — Parquet (DuckDB) vs CSV (MySQL)
    Step 2: Columnar Storage 演示 — Narrow vs Wide Scan
    Step 3: Vectorized Execution 演示 — Window Functions
    Step 4: OLTP 对比 — 单行查找 (MySQL 优势场景)
    Step 5: 完整 Benchmark 汇总
    Step 6: EXPLAIN ANALYZE 对比
"""

import argparse
import time
import statistics
import sys
from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.rule import Rule
from rich import box

console = Console()

# ── 路径配置 ──
PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR    = PROJECT_DIR / "data"
DUCKDB_FILE = str(DATA_DIR / "stock_analytics.duckdb")  # setup_duckdb will create/overwrite

# ── MySQL 配置 ──
MYSQL_USER     = "root"
MYSQL_PASSWORD = "password"
MYSQL_HOST     = "127.0.0.1"
MYSQL_PORT     = "3306"
MYSQL_DB       = "stock_db"


# ============================================================
#  工具函数
# ============================================================
def pause(msg: str = "按 Enter 继续下一步..."):
    """演示暂停点，让 presenter 控制节奏"""
    console.print(f"\n[dim italic]  >>> {msg}[/dim italic]")
    input()


def timed_query(con_or_engine, sql: str, db_type: str, n_runs: int = 5) -> tuple:
    """多次运行取中位数，返回 (df, median_ms)"""
    times = []
    last_df = None
    for _ in range(n_runs):
        t0 = time.perf_counter()
        if db_type == "duckdb":
            last_df = con_or_engine.execute(sql).fetchdf()
        else:
            last_df = pd.read_sql(sql, con_or_engine)
        times.append((time.perf_counter() - t0) * 1000)
    # 丢弃第一次 warm-up
    median_ms = round(statistics.median(times[1:]), 3)
    return last_df, median_ms, times


def print_explain_side_by_side(duck_con, mysql_engine, sql: str, title: str):
    """打印 DuckDB 和 MySQL 的 EXPLAIN 对比"""
    console.print(f"\n[bold cyan]--- {title}: EXPLAIN ---[/bold cyan]")

    # DuckDB EXPLAIN
    console.print("\n[green]DuckDB EXPLAIN:[/green]")
    try:
        plan = duck_con.execute(f"EXPLAIN {sql}").fetchdf()
        console.print(plan.to_string(index=False))
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")

    # MySQL EXPLAIN
    if mysql_engine:
        console.print("\n[yellow]MySQL EXPLAIN:[/yellow]")
        try:
            plan = pd.read_sql(f"EXPLAIN {sql}", mysql_engine)
            console.print(plan.to_string(index=False))
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")


# ============================================================
#  Demo 查询集
# ============================================================

# -- Columnar Storage 演示 --
Q_NARROW = """
    SELECT COUNT(*), ROUND(AVG(Close), 4), SUM(Volume)
    FROM stock_data
"""

Q_WIDE = """
    SELECT * FROM stock_data ORDER BY Date DESC LIMIT 5000
"""

# -- Vectorized Execution 演示 --
Q_WINDOW_MA50 = """
    SELECT Date, Symbol, Close,
           AVG(Close) OVER (
               PARTITION BY Symbol ORDER BY Date
               ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
           ) AS MA_50
    FROM stock_data
    ORDER BY Date DESC LIMIT 100
"""

Q_WINDOW_STDDEV = """
    SELECT Date, Symbol, Close,
           ROUND(STDDEV(Close) OVER (
               PARTITION BY Symbol ORDER BY Date
               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
           ), 4) AS rolling_std_20
    FROM stock_data
    ORDER BY Date DESC LIMIT 100
"""

Q_AGGREGATION = """
    SELECT YEAR(Date) AS Year, Symbol,
           ROUND(AVG(Close), 2)  AS avg_close,
           ROUND(MAX(High), 2)   AS year_high,
           ROUND(MIN(Low), 2)    AS year_low,
           SUM(Volume)           AS total_volume
    FROM stock_data
    GROUP BY YEAR(Date), Symbol
    ORDER BY Year, Symbol
"""

# -- OLTP 对比 (MySQL 优势场景) --
Q_POINT_LOOKUP = """
    SELECT * FROM stock_data
    WHERE Symbol = 'AAPL' AND Date = '2023-06-15'
"""

Q_SMALL_RANGE = """
    SELECT * FROM stock_data
    WHERE Symbol = 'AAPL' AND Date BETWEEN '2023-01-01' AND '2023-01-31'
"""

# -- Daily Volatility (Column Pruning) --
Q_VOLATILITY = """
    SELECT Date, Symbol,
           ROUND((High - Low) / Close * 100, 4) AS daily_volatility_pct
    FROM stock_data
    ORDER BY daily_volatility_pct DESC
    LIMIT 20
"""


# ============================================================
#  Demo Steps
# ============================================================
def step0_intro():
    console.print(Rule("[bold white] DSCI 551 Final Project Demo [/bold white]", style="cyan"))
    console.print(Panel(
        "[bold]Stock Market Analytics with DuckDB[/bold]\n"
        "A Study of Columnar Storage and Vectorized Execution\n"
        "\n"
        "[dim]Members: Hanwen Yang, Jialiang Lou[/dim]\n"
        "\n"
        "Project Goals:\n"
        "  1. Analyze DuckDB internals: columnar storage + vectorized execution\n"
        "  2. Build a stock analytics app that leverages these internals\n"
        "  3. Compare with MySQL (row-based + tuple-at-a-time) to show differences",
        title="[bold cyan]Project Overview[/bold cyan]",
        border_style="cyan",
        expand=False,
    ))
    console.print(
        "\n[bold]Demo Outline:[/bold]\n"
        "  Step 1 → Data loading & schema\n"
        "  Step 2 → Columnar Storage: Narrow vs Wide scan\n"
        "  Step 3 → Vectorized Execution: Window functions & aggregation\n"
        "  Step 4 → OLTP queries: Where MySQL shines\n"
        "  Step 5 → Full benchmark summary\n"
        "  Step 6 → EXPLAIN ANALYZE comparison\n"
    )


def step1_data(duck_con, mysql_engine):
    console.print(Rule("[bold] Step 1: Data Loading & Schema [/bold]", style="green"))

    # 展示数据规模
    row_count = duck_con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    sym_count = duck_con.execute("SELECT COUNT(DISTINCT Symbol) FROM stock_data").fetchone()[0]
    date_range = duck_con.execute(
        "SELECT MIN(Date), MAX(Date) FROM stock_data"
    ).fetchone()

    console.print(Panel(
        f"Table: [bold]stock_data[/bold]\n"
        f"Schema: (Date DATE, Symbol VARCHAR, Open DOUBLE, High DOUBLE,\n"
        f"         Low DOUBLE, Close DOUBLE, Volume BIGINT) — 7 columns\n"
        f"\n"
        f"Rows:     [bold]{row_count:,}[/bold]\n"
        f"Stocks:   [bold]{sym_count}[/bold]\n"
        f"Period:   {date_range[0]} → {date_range[1]}\n"
        f"\n"
        f"Data source: Yahoo Finance (OHLCV daily)\n"
        f"DuckDB:  embedded in-process, columnar storage\n"
        f"MySQL:   row-based InnoDB, composite index on (Symbol, Date)",
        title="Dataset",
        expand=False,
    ))

    # 展示前几行
    sample = duck_con.execute(
        "SELECT * FROM stock_data WHERE Symbol='AAPL' ORDER BY Date DESC LIMIT 5"
    ).fetchdf()
    console.print("\n[bold]Sample data (AAPL, most recent):[/bold]")
    console.print(sample.to_string(index=False))


def step2_columnar(duck_con, mysql_engine, n_runs: int):
    console.print(Rule("[bold] Step 2: Columnar Storage — Narrow vs Wide Scan [/bold]", style="green"))

    console.print(Panel(
        "[bold]Key Insight:[/bold] DuckDB stores data column-by-column.\n"
        "When a query only needs 2-3 of 7 columns, DuckDB skips the rest → less I/O.\n"
        "MySQL stores data row-by-row → must read all 7 columns even if only 2 are needed.",
        border_style="blue", expand=False,
    ))

    # -- Q_NARROW: 只读 Close + Volume --
    console.print("\n[bold]Query A: Narrow Scan[/bold] — reads only Close + Volume (2/7 cols)")
    console.print(f"[dim]{Q_NARROW.strip()}[/dim]")

    _, duck_narrow, _ = timed_query(duck_con, Q_NARROW, "duckdb", n_runs)
    mysql_narrow = -1
    if mysql_engine:
        _, mysql_narrow, _ = timed_query(mysql_engine, Q_NARROW, "mysql", n_runs)

    # -- Q_WIDE: 读全部 7 列 --
    console.print("\n[bold]Query B: Wide Scan[/bold] — reads all 7 columns (SELECT *)")
    console.print(f"[dim]{Q_WIDE.strip()}[/dim]")

    _, duck_wide, _ = timed_query(duck_con, Q_WIDE, "duckdb", n_runs)
    mysql_wide = -1
    if mysql_engine:
        _, mysql_wide, _ = timed_query(mysql_engine, Q_WIDE, "mysql", n_runs)

    # -- 对比表 --
    table = Table(title="Columnar Storage: Narrow vs Wide", box=box.ROUNDED, show_lines=True)
    table.add_column("Query", style="bold")
    table.add_column("Cols Read")
    table.add_column("DuckDB (ms)", justify="right", style="green")
    table.add_column("MySQL (ms)", justify="right", style="yellow")
    table.add_column("Speedup", justify="right", style="magenta")

    def speedup_str(d, m):
        if d > 0 and m > 0:
            return f"{m/d:.1f}x"
        return "N/A"

    table.add_row("Narrow (AVG, SUM)", "2 / 7", f"{duck_narrow:.1f}",
                  f"{mysql_narrow:.1f}" if mysql_narrow > 0 else "N/A",
                  speedup_str(duck_narrow, mysql_narrow))
    table.add_row("Wide (SELECT *)", "7 / 7", f"{duck_wide:.1f}",
                  f"{mysql_wide:.1f}" if mysql_wide > 0 else "N/A",
                  speedup_str(duck_wide, mysql_wide))
    console.print(table)

    console.print(
        "\n[bold]Takeaway:[/bold] DuckDB's narrow scan is much faster because columnar storage\n"
        "only reads the needed columns. The advantage shrinks on SELECT * (wide scan)\n"
        "because all columns must be read and reconstructed into rows."
    )

    # -- Volatility (Column Pruning demo) --
    console.print("\n[bold]Bonus: Daily Volatility[/bold] — reads High, Low, Close (3/7 cols)")
    df_vol, duck_vol, _ = timed_query(duck_con, Q_VOLATILITY, "duckdb", n_runs)
    console.print(f"  DuckDB: {duck_vol:.1f} ms")
    console.print(df_vol.head(5).to_string(index=False))


def step3_vectorized(duck_con, mysql_engine, n_runs: int):
    console.print(Rule("[bold] Step 3: Vectorized Execution — Window & Aggregation [/bold]", style="green"))

    console.print(Panel(
        "[bold]Key Insight:[/bold] DuckDB processes data in batches of 2048 tuples.\n"
        "Window functions and aggregations run on entire vectors at once → SIMD-friendly.\n"
        "MySQL processes one tuple at a time → higher function call overhead.",
        border_style="blue", expand=False,
    ))

    queries = [
        ("50-Day Moving Avg (Window)", Q_WINDOW_MA50, "STREAMING_WINDOW"),
        ("20-Day Rolling StdDev (Window)", Q_WINDOW_STDDEV, "STREAMING_WINDOW"),
        ("Annual Summary (GROUP BY)", Q_AGGREGATION, "HASH_GROUP_BY"),
    ]

    table = Table(title="Vectorized Execution Benchmark", box=box.ROUNDED, show_lines=True)
    table.add_column("Query", style="bold")
    table.add_column("DuckDB Operator")
    table.add_column("DuckDB (ms)", justify="right", style="green")
    table.add_column("MySQL (ms)", justify="right", style="yellow")
    table.add_column("Speedup", justify="right", style="magenta")

    for label, sql, operator in queries:
        console.print(f"\n[bold]{label}[/bold]")
        console.print(f"[dim]{sql.strip()[:120]}...[/dim]")

        df_duck, duck_ms, _ = timed_query(duck_con, sql, "duckdb", n_runs)
        mysql_ms = -1
        if mysql_engine:
            _, mysql_ms, _ = timed_query(mysql_engine, sql, "mysql", n_runs)

        sp = f"{mysql_ms/duck_ms:.1f}x" if duck_ms > 0 and mysql_ms > 0 else "N/A"
        table.add_row(label, operator, f"{duck_ms:.1f}",
                      f"{mysql_ms:.1f}" if mysql_ms > 0 else "N/A", sp)

        # 展示前 3 行结果
        if df_duck is not None and not df_duck.empty:
            console.print(df_duck.head(3).to_string(index=False))

    console.print(table)

    console.print(
        "\n[bold]Takeaway:[/bold] Window functions show the largest speedup because DuckDB's\n"
        "STREAMING_WINDOW operator processes entire chunks without materializing\n"
        "intermediate results, while MySQL uses temporary tables + filesort."
    )


def step4_oltp(duck_con, mysql_engine, n_runs: int):
    console.print(Rule("[bold] Step 4: OLTP Queries — Where MySQL Shines [/bold]", style="green"))

    console.print(Panel(
        "[bold]Key Insight:[/bold] Row-based storage excels at point lookups.\n"
        "MySQL can use its B-tree index on (Symbol, Date) to find a single row\n"
        "without scanning any other data. DuckDB must scan column segments.",
        border_style="blue", expand=False,
    ))

    oltp_queries = [
        ("Point Lookup (1 row)", Q_POINT_LOOKUP),
        ("Small Range Scan (1 month)", Q_SMALL_RANGE),
    ]

    table = Table(title="OLTP: Point Lookup & Range Scan", box=box.ROUNDED, show_lines=True)
    table.add_column("Query", style="bold")
    table.add_column("DuckDB (ms)", justify="right", style="green")
    table.add_column("MySQL (ms)", justify="right", style="yellow")
    table.add_column("Winner", justify="center")

    for label, sql in oltp_queries:
        console.print(f"\n[bold]{label}[/bold]")
        console.print(f"[dim]{sql.strip()}[/dim]")

        df_duck, duck_ms, _ = timed_query(duck_con, sql, "duckdb", n_runs)
        mysql_ms = -1
        if mysql_engine:
            _, mysql_ms, _ = timed_query(mysql_engine, sql, "mysql", n_runs)

        if duck_ms > 0 and mysql_ms > 0:
            winner = "[yellow]MySQL[/yellow]" if mysql_ms < duck_ms else "[green]DuckDB[/green]"
        else:
            winner = "N/A"

        table.add_row(label, f"{duck_ms:.1f}",
                      f"{mysql_ms:.1f}" if mysql_ms > 0 else "N/A", winner)

        if df_duck is not None and not df_duck.empty:
            console.print(df_duck.to_string(index=False))

    console.print(table)

    console.print(
        "\n[bold]Takeaway:[/bold] For point lookups and small range scans, MySQL's B-tree\n"
        "index provides direct access. This demonstrates that columnar storage is\n"
        "optimized for OLAP, not OLTP workloads — each architecture has its strengths."
    )


def step5_summary(duck_con, mysql_engine, n_runs: int):
    console.print(Rule("[bold] Step 5: Full Benchmark Summary [/bold]", style="green"))

    all_queries = [
        ("Q1: 50-Day MA",           Q_WINDOW_MA50,    "Vectorized Window",    "3/7"),
        ("Q2: Daily Volatility",    Q_VOLATILITY,     "Column Pruning",       "3/7"),
        ("Q3: Annual Summary",      Q_AGGREGATION,    "Vectorized Aggregation","4/7"),
        ("Q4: Narrow Aggregate",    Q_NARROW,         "Narrow Columnar I/O",  "2/7"),
        ("Q5: Rolling StdDev",      Q_WINDOW_STDDEV,  "Vec. Window + StdDev", "3/7"),
        ("Q6: Wide SELECT *",       Q_WIDE,           "Wide Scan (control)",  "7/7"),
        ("Q7: Point Lookup",        Q_POINT_LOOKUP,   "OLTP (MySQL advantage)","7/7"),
        ("Q8: Small Range Scan",    Q_SMALL_RANGE,    "OLTP (MySQL advantage)","7/7"),
    ]

    table = Table(
        title="Complete Benchmark: DuckDB vs MySQL",
        box=box.DOUBLE_EDGE, show_lines=True,
    )
    table.add_column("Query", style="bold", min_width=22)
    table.add_column("Focus", min_width=20)
    table.add_column("Cols", justify="center")
    table.add_column("DuckDB (ms)", justify="right", style="green")
    table.add_column("MySQL (ms)", justify="right", style="yellow")
    table.add_column("Speedup", justify="right", style="bold magenta")

    for label, sql, focus, cols in all_queries:
        _, duck_ms, _ = timed_query(duck_con, sql, "duckdb", n_runs)
        mysql_ms = -1
        if mysql_engine:
            _, mysql_ms, _ = timed_query(mysql_engine, sql, "mysql", n_runs)

        if duck_ms > 0 and mysql_ms > 0:
            ratio = mysql_ms / duck_ms
            sp = f"{ratio:.1f}x" if ratio >= 1 else f"[yellow]{ratio:.2f}x (MySQL wins)[/yellow]"
        else:
            sp = "N/A"

        table.add_row(label, focus, cols, f"{duck_ms:.1f}",
                      f"{mysql_ms:.1f}" if mysql_ms > 0 else "N/A", sp)

    console.print(table)

    console.print(Panel(
        "[bold]Summary:[/bold]\n"
        "  • OLAP queries (Q1-Q5): DuckDB is significantly faster\n"
        "    — Columnar storage reduces I/O for narrow projections\n"
        "    — Vectorized execution accelerates window functions & aggregation\n"
        "  • Wide scan (Q6): DuckDB advantage shrinks (must reconstruct rows)\n"
        "  • OLTP queries (Q7-Q8): MySQL can be competitive or faster\n"
        "    — B-tree index gives direct row access\n"
        "    — Row-based storage avoids tuple reconstruction overhead",
        border_style="cyan", expand=False,
    ))


def step6_explain(duck_con, mysql_engine):
    console.print(Rule("[bold] Step 6: EXPLAIN ANALYZE — Execution Plan Comparison [/bold]", style="green"))

    console.print(
        "[bold]Showing execution plans to verify internal behavior:[/bold]\n"
        "  DuckDB: SEQ_SCAN with column pruning, STREAMING_WINDOW, HASH_GROUP_BY\n"
        "  MySQL:  Full table scan, Using temporary, Using filesort\n"
    )

    demos = [
        ("Narrow Scan (Column Pruning)", Q_NARROW),
        ("Window Function (Vectorized)", Q_WINDOW_MA50),
        ("Aggregation (Hash Group By)", Q_AGGREGATION),
    ]

    for title, sql in demos:
        print_explain_side_by_side(duck_con, mysql_engine, sql.strip(), title)
        pause()


# ============================================================
#  Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="DSCI 551 Demo Script")
    parser.add_argument("--no-mysql", action="store_true", help="Skip MySQL")
    parser.add_argument("--skip-download", action="store_true", help="Use cached data")
    parser.add_argument("--runs", type=int, default=5, help="Runs per query (default=5)")
    parser.add_argument("--auto", action="store_true", help="No pauses between steps")
    args = parser.parse_args()

    n_runs = args.runs

    # ── Step 0: Intro ──
    step0_intro()
    if not args.auto:
        pause("Press Enter to start Step 1: Data Loading...")

    # ── 加载数据 ──
    from downloader import fetch_stock_data, DEFAULT_SYMBOLS, EXTENDED_SYMBOLS
    from db_run import setup_duckdb, setup_mysql

    symbols = DEFAULT_SYMBOLS
    if not args.skip_download:
        console.print("[cyan]Downloading / loading data...[/cyan]")

    df = fetch_stock_data(symbols, data_dir=DATA_DIR)
    console.print(f"[green]Loaded {len(df):,} rows, {df['Symbol'].nunique()} stocks[/green]")

    # 初始化 DuckDB
    duck_con = setup_duckdb(df, DUCKDB_FILE)

    # 初始化 MySQL
    mysql_engine = None
    if not args.no_mysql:
        mysql_engine = setup_mysql(df)
        if mysql_engine is None:
            console.print("[yellow]MySQL unavailable, continuing with DuckDB only[/yellow]")

    # ── Step 1: Data ──
    step1_data(duck_con, mysql_engine)
    if not args.auto:
        pause("Press Enter for Step 2: Columnar Storage...")

    # ── Step 2: Columnar ──
    step2_columnar(duck_con, mysql_engine, n_runs)
    if not args.auto:
        pause("Press Enter for Step 3: Vectorized Execution...")

    # ── Step 3: Vectorized ──
    step3_vectorized(duck_con, mysql_engine, n_runs)
    if not args.auto:
        pause("Press Enter for Step 4: OLTP Queries...")

    # ── Step 4: OLTP ──
    step4_oltp(duck_con, mysql_engine, n_runs)
    if not args.auto:
        pause("Press Enter for Step 5: Full Benchmark...")

    # ── Step 5: Summary ──
    step5_summary(duck_con, mysql_engine, n_runs)
    if not args.auto:
        pause("Press Enter for Step 6: EXPLAIN ANALYZE...")

    # ── Step 6: EXPLAIN ──
    step6_explain(duck_con, mysql_engine)

    # ── Closing ──
    console.print(Rule("[bold white] Thank You! [/bold white]", style="cyan"))
    console.print(Panel(
        "[bold]Key Takeaways:[/bold]\n\n"
        "1. [green]Columnar Storage[/green] → reads only needed columns\n"
        "   DuckDB scans 2-3 of 7 cols, MySQL reads entire rows\n\n"
        "2. [green]Vectorized Execution[/green] → processes 2048 tuples per batch\n"
        "   Window functions see 5-30x speedup over MySQL's tuple-at-a-time\n\n"
        "3. [yellow]Trade-off[/yellow] → Row-based storage better for OLTP point lookups\n"
        "   Each architecture suits different workload types\n\n"
        "[dim]Database: DuckDB | Application: Stock Market Analytics Dashboard[/dim]",
        title="[bold cyan]Conclusion[/bold cyan]",
        border_style="cyan",
        expand=False,
    ))


if __name__ == "__main__":
    main()
