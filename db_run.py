"""
db_run.py — DuckDB vs MySQL performance benchmark.
Handles database initialization, query execution, repeated runs with
median aggregation, and persisting results to disk.
"""

import csv
import json
import os
import time
import statistics
import warnings
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Date, Float, BigInteger

warnings.filterwarnings("ignore")

# ── Optional .env support ──
# If python-dotenv is installed and a .env file exists at project root,
# auto-load MYSQL_* / MONGO_* variables. Safe no-op when either is missing.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

console = Console()

# ============================================================
#  MySQL config — env vars take priority, with local defaults
# ============================================================
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "password")
MYSQL_HOST     = os.getenv("MYSQL_HOST",     "127.0.0.1")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3306")
MYSQL_DB       = os.getenv("MYSQL_DB",       "stock_db")


# ============================================================
#  Benchmark query catalog
#  Each query targets one canonical OLAP scenario; the `mapping`
#  field corresponds directly to the "Internals -> Application"
#  section of the midterm report.
# ============================================================
BENCHMARK_QUERIES = {
    "Q1_50day_MA": {
        "description": "50-day moving average (window function)",
        "focus": "Vectorized Execution",
        "mapping": (
            "DuckDB: STREAMING_WINDOW operator batches AVG over 2048-tuple chunks "
            "and benefits from SIMD acceleration.\n"
            "MySQL: iterates the window frame row-by-row, paying function-call overhead per row."
        ),
        "sql": """
            SELECT
                Date, Symbol, Close,
                AVG(Close) OVER (
                    PARTITION BY Symbol
                    ORDER BY Date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                ) AS MA_50
            FROM stock_data
            ORDER BY Date DESC
            LIMIT 100
        """,
    },
    "Q2_daily_volatility": {
        "description": "Daily volatility — narrow projection (3 cols)",
        "focus": "Columnar Storage — Column Pruning",
        "mapping": (
            "DuckDB: reads only the High/Low/Close column segments, "
            "skipping Open/Volume/Symbol/Date — about 57% less I/O.\n"
            "MySQL: row store must fetch the whole tuple and discard unused columns."
        ),
        "sql": """
            SELECT
                Date,
                Symbol,
                ROUND((High - Low) / Close * 100, 4) AS daily_volatility_pct
            FROM stock_data
            ORDER BY daily_volatility_pct DESC
            LIMIT 100
        """,
    },
    "Q3_annual_summary": {
        "description": "Annual summary (GROUP BY + multi-aggregate)",
        "focus": "Vectorized Aggregation",
        "mapping": (
            "DuckDB: PERFECT_HASH_GROUP_BY / HASH_GROUP_BY operators "
            "hash + aggregate batches of values inside each vector chunk.\n"
            "MySQL: row-by-row read -> hash -> update aggregate state; cache misses dominate."
        ),
        "sql": """
            SELECT
                YEAR(Date)   AS Year,
                Symbol,
                ROUND(AVG(Close),  2) AS avg_close,
                ROUND(MAX(High),   2) AS year_high,
                ROUND(MIN(Low),    2) AS year_low,
                SUM(Volume)           AS total_volume
            FROM stock_data
            GROUP BY YEAR(Date), Symbol
            ORDER BY Year, Symbol
        """,
    },
    "Q4_full_scan_narrow": {
        "description": "Full-table aggregate — narrow (Close + Volume only)",
        "focus": "Columnar Storage I/O — Narrow Scan",
        "mapping": (
            "DuckDB: scans only the Close and Volume column segments; "
            "compressed I/O is tiny.\n"
            "MySQL: full row scan reading all 7 columns."
        ),
        "sql": """
            SELECT COUNT(*), ROUND(AVG(Close), 4), SUM(Volume)
            FROM stock_data
        """,
    },
    "Q5_rolling_stddev": {
        "description": "20-day rolling stddev (volatility indicator)",
        "focus": "Vectorized Window + STDDEV",
        "mapping": (
            "DuckDB: STREAMING_WINDOW computes STDDEV in chunked batches, "
            "avoiding repeated window-frame traversal.\n"
            "MySQL: re-scans the 20-row frame for every row to compute the stddev."
        ),
        "sql": """
            SELECT
                Date, Symbol, Close,
                ROUND(
                    STDDEV(Close) OVER (
                        PARTITION BY Symbol
                        ORDER BY Date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ), 4
                ) AS rolling_std_20
            FROM stock_data
            ORDER BY Date DESC
            LIMIT 100
        """,
    },
    "Q6_wide_projection": {
        "description": "SELECT * — wide-scan control experiment",
        "focus": "Columnar Storage — Wide Scan (column-store disadvantage)",
        "mapping": (
            "DuckDB: must read every column and reconstruct tuples; "
            "the columnar advantage disappears and can be slightly slower than a row store.\n"
            "MySQL: native row layout makes SELECT * essentially free.\n"
            "Pairs with Q4's narrow scan to show when a column store wins vs loses."
        ),
        "sql": """
            SELECT *
            FROM stock_data
            ORDER BY Date DESC
            LIMIT 5000
        """,
    },
    "Q7_point_lookup": {
        "description": "Point Lookup — single row by Symbol + Date",
        "focus": "OLTP — B-tree Index (MySQL advantage)",
        "mapping": (
            "DuckDB: No B-tree index; must scan column segments even for 1 row.\n"
            "MySQL: B-tree index on (Symbol, Date) gives O(log n) direct access.\n"
            "This shows row-based + B-tree excels at OLTP point queries."
        ),
        "sql": """
            SELECT *
            FROM stock_data
            WHERE Symbol = 'AAPL' AND Date = '2023-06-15'
        """,
    },
    "Q8_small_range": {
        "description": "Small Range Scan — 1 month for 1 stock",
        "focus": "OLTP — B-tree Range Scan (MySQL advantage)",
        "mapping": (
            "DuckDB: Scans row groups, uses zone maps to skip irrelevant groups.\n"
            "MySQL: B-tree range scan on (Symbol, Date) reads only matching rows.\n"
            "Small range scans favor indexed row-based storage."
        ),
        "sql": """
            SELECT *
            FROM stock_data
            WHERE Symbol = 'AAPL'
              AND Date BETWEEN '2023-01-01' AND '2023-01-31'
        """,
    },
    "Q9_single_stock_filter": {
        "description": "Single-stock full history filter",
        "focus": "OLTP — Selective Filter (MySQL advantage)",
        "mapping": (
            "DuckDB: Scans all row groups for Symbol column, filters in batch.\n"
            "MySQL: Uses index on Symbol to skip ~80% of rows (5-stock dataset).\n"
            "Selective filters on indexed columns favor row-based storage."
        ),
        "sql": """
            SELECT Date, Close, Volume
            FROM stock_data
            WHERE Symbol = 'AAPL'
            ORDER BY Date
        """,
    },
}


# ============================================================
#  Database initialization
# ============================================================
def setup_duckdb(df: pd.DataFrame, duckdb_file: str) -> duckdb.DuckDBPyConnection:
    """Initialize DuckDB: create table + indexes."""
    console.print(f"\n[cyan][*] Initializing DuckDB ({duckdb_file})...[/cyan]")
    t0 = time.perf_counter()

    con = duckdb.connect(duckdb_file)
    con.execute("DROP TABLE IF EXISTS stock_data")
    con.execute("CREATE TABLE stock_data AS SELECT * FROM df")
    con.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data(Symbol)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_date   ON stock_data(Date)")

    count = con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    elapsed = time.perf_counter() - t0
    console.print(f"[green][+] DuckDB ready: {count:,} rows, load time {elapsed:.3f}s[/green]")
    return con


def setup_mysql(df: pd.DataFrame):
    """Initialize MySQL: create table + indexes (for a fair comparison)."""
    console.print(f"\n[cyan][*] Connecting to WSL MySQL ({MYSQL_HOST}:{MYSQL_PORT})...[/cyan]")

    try:
        root_engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/"
        )
        with root_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
    except Exception as e:
        err_msg = str(e)
        console.print(f"[red][!] MySQL connection failed: {e}[/red]")
        if "cryptography" in err_msg:
            console.print("[yellow]    -> MySQL 8 auth needs the cryptography package:[/yellow]")
            console.print("[yellow]      pip install cryptography   (or: uv add cryptography)[/yellow]")
        elif "Access denied" in err_msg:
            console.print("[yellow]    -> Wrong password — edit .env to set MYSQL_PASSWORD[/yellow]")
        else:
            console.print("[yellow]    -> Make sure MySQL is running: sudo service mysql start[/yellow]")
        return None

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )

    t0 = time.perf_counter()
    try:
        df.to_sql(
            "stock_data", engine, if_exists="replace", index=False,
            chunksize=5000, method="multi",
            dtype={
                "Symbol": String(10),      # VARCHAR(10) so we can index it (TEXT can't be indexed)
                "Date":   Date(),
                "Open":   Float(),
                "High":   Float(),
                "Low":    Float(),
                "Close":  Float(),
                "Volume": BigInteger(),
            },
        )

        # Build the same indexes on MySQL to keep the comparison fair
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_symbol (Symbol)"))
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_date (Date)"))
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_sym_date (Symbol, Date)"))
            conn.commit()

        elapsed = time.perf_counter() - t0
        console.print(f"[green][+] MySQL ready (with indexes), load time {elapsed:.3f}s[/green]")
    except Exception as e:
        console.print(f"[red][!] MySQL load failed: {e}[/red]")
        return None

    return engine


# ============================================================
#  Query execution — multiple runs with median aggregation
# ============================================================
def run_query_once(engine_or_con, sql: str, db_type: str) -> tuple[pd.DataFrame | None, float]:
    """Run a query once. Returns (DataFrame, elapsed_ms)."""
    try:
        t0 = time.perf_counter()
        if db_type == "duckdb":
            result = engine_or_con.execute(sql).fetchdf()
        else:
            result = pd.read_sql(sql, engine_or_con)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return result, round(elapsed_ms, 3)
    except Exception as e:
        console.print(f"[red]  [{db_type}] query failed: {e}[/red]")
        return None, -1.0


def run_query(engine_or_con, sql: str, db_type: str, n_runs: int = 5) -> tuple[pd.DataFrame | None, float, list[float]]:
    """
    Run a query n_runs times. The first run is treated as warm-up and discarded;
    the median of the remaining (n_runs-1) runs is reported.
    Returns (last_df, median_ms, list_of_all_times).
    """
    all_times = []
    last_df = None

    for i in range(n_runs):
        df, ms = run_query_once(engine_or_con, sql, db_type)
        all_times.append(ms)
        if df is not None:
            last_df = df

    # Drop the warm-up run, take median of the rest
    valid_times = [t for t in all_times[1:] if t >= 0]
    if valid_times:
        median_ms = round(statistics.median(valid_times), 3)
    else:
        median_ms = -1.0

    return last_df, median_ms, all_times


# ============================================================
#  Run the full benchmark suite
# ============================================================
def run_benchmark(
    duckdb_con,
    mysql_engine,
    n_runs: int = 5,
    query_ids: list[str] | None = None,
    output_dir: Path = Path.cwd() / "output",
) -> tuple[list[dict], str]:
    """
    Run the benchmark. Returns (results_list, session_timestamp).

    Parameters
    ----------
    n_runs : int
        Runs per query (first run is the warm-up).
    query_ids : list[str] | None
        Which queries to run; None means all.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results: list[dict] = []

    queries = BENCHMARK_QUERIES
    if query_ids:
        queries = {k: v for k, v in BENCHMARK_QUERIES.items() if k in query_ids}

    console.print(Panel(
        f"[bold cyan]DuckDB vs MySQL — Performance Benchmark[/bold cyan]\n"
        f"Session: {session_ts} | Runs per query: {n_runs} (1 warm-up + {n_runs-1} measured)\n"
        f"Queries: {list(queries.keys())}",
        expand=False,
    ))

    # Rich summary table
    table = Table(title="Benchmark Results (Median)", show_lines=True)
    table.add_column("Query ID",      style="bold")
    table.add_column("Description",   style="dim")
    table.add_column("Focus")
    table.add_column("DuckDB (ms)",   justify="right", style="green")
    table.add_column("MySQL (ms)",    justify="right", style="yellow")
    table.add_column("Speedup",       justify="right", style="bold magenta")

    for qid, meta in queries.items():
        console.print(f"\n[bold]▶ {qid}[/bold] — {meta['description']}")
        console.print(f"  Focus: {meta['focus']}")

        # DuckDB
        duck_df, duck_ms, duck_times = run_query(duckdb_con, meta["sql"], "duckdb", n_runs)
        console.print(f"  [green]DuckDB[/green] times: {duck_times} → median={duck_ms}ms")

        # MySQL
        if mysql_engine:
            mysql_df, mysql_ms, mysql_times = run_query(mysql_engine, meta["sql"], "mysql", n_runs)
            console.print(f"  [yellow]MySQL [/yellow] times: {mysql_times} → median={mysql_ms}ms")
        else:
            mysql_df, mysql_ms, mysql_times = None, -1.0, []

        # Speedup
        if duck_ms > 0 and mysql_ms > 0:
            speedup = round(mysql_ms / duck_ms, 1)
            speedup_str = f"{speedup}x"
        else:
            speedup = None
            speedup_str = "N/A"

        table.add_row(
            qid,
            meta["description"],
            meta["focus"],
            f"{duck_ms:.1f}" if duck_ms >= 0 else "ERROR",
            f"{mysql_ms:.1f}" if mysql_ms >= 0 else "SKIP",
            speedup_str,
        )

        # Preview first 3 rows of the result
        if duck_df is not None and not duck_df.empty:
            console.print(duck_df.head(3).to_string(index=False))

        results.append({
            "session":        session_ts,
            "query_id":       qid,
            "description":    meta["description"],
            "focus":          meta["focus"],
            "duckdb_median_ms": duck_ms,
            "duckdb_all_ms":  duck_times,
            "mysql_median_ms":  mysql_ms,
            "mysql_all_ms":   mysql_times,
            "speedup":        speedup_str,
            "n_runs":         n_runs,
        })

    console.print(table)
    return results, session_ts


# ============================================================
#  Persist results
# ============================================================
def save_results(results: list[dict], session_ts: str, output_dir: Path = Path.cwd() / "output"):
    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV — append mode
    csv_path = output_dir / "benchmark_results.csv"
    flat_results = []
    for r in results:
        flat = {k: v for k, v in r.items() if k not in ("duckdb_all_ms", "mysql_all_ms")}
        flat["duckdb_all_ms"] = json.dumps(r["duckdb_all_ms"])
        flat["mysql_all_ms"]  = json.dumps(r["mysql_all_ms"])
        flat_results.append(flat)

    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=flat_results[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(flat_results)

    # JSON snapshot for this session
    json_path = output_dir / f"benchmark_{session_ts}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    console.print(f"\n[green][+] Results saved:[/green]")
    console.print(f"    CSV  (cumulative) -> {csv_path}")
    console.print(f"    JSON (this run)   -> {json_path}")

    return csv_path, json_path


# ============================================================
#  Storage size comparison
# ============================================================
def compare_storage_sizes(data_dir: Path, mysql_engine=None):
    """Compare file sizes: Parquet vs CSV vs DuckDB vs MySQL InnoDB."""
    console.print("\n[bold cyan]Storage Size Comparison[/bold cyan]")

    table = Table(title="Storage Format Comparison", show_lines=True)
    table.add_column("Format", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Notes")

    # Parquet files
    for p in sorted(data_dir.glob("*.parquet")):
        size_kb = p.stat().st_size / 1024
        table.add_row("Parquet", f"{size_kb:.1f} KB",
                       f"{p.name} — columnar + compressed (Snappy)")

    # CSV files
    for p in sorted(data_dir.glob("*.csv")):
        size_kb = p.stat().st_size / 1024
        table.add_row("CSV", f"{size_kb:.1f} KB",
                       f"{p.name} — plain text, no compression")

    # DuckDB file
    for p in sorted(data_dir.glob("*.duckdb")):
        size_kb = p.stat().st_size / 1024
        table.add_row("DuckDB", f"{size_kb:.1f} KB",
                       f"{p.name} — columnar + per-column compression")

    # MySQL InnoDB tablespace
    if mysql_engine:
        try:
            result = pd.read_sql(
                "SELECT ROUND(data_length/1024, 1) AS data_kb, "
                "ROUND(index_length/1024, 1) AS index_kb "
                "FROM information_schema.tables "
                f"WHERE table_schema='{MYSQL_DB}' AND table_name='stock_data'",
                mysql_engine,
            )
            if not result.empty:
                data_kb = result.iloc[0]["data_kb"]
                idx_kb = result.iloc[0]["index_kb"]
                table.add_row("MySQL InnoDB Data", f"{data_kb} KB",
                               "Row-based storage (B-tree clustered)")
                table.add_row("MySQL InnoDB Index", f"{idx_kb} KB",
                               "Secondary B-tree indexes")
                table.add_row("MySQL Total", f"{data_kb + idx_kb} KB",
                               "Data + indexes combined")
        except Exception as e:
            table.add_row("MySQL", "N/A", f"Error: {e}")

    console.print(table)


# ============================================================
#  DuckDB JSON Profiling
# ============================================================
def run_json_profiling(con, query_ids: list[str] | None = None,
                       output_dir: Path = Path.cwd() / "output"):
    """Run DuckDB queries with JSON profiling to get operator-level timing."""
    output_dir.mkdir(parents=True, exist_ok=True)

    queries = BENCHMARK_QUERIES
    if query_ids:
        queries = {k: v for k, v in BENCHMARK_QUERIES.items() if k in query_ids}

    console.print("\n[bold cyan]DuckDB JSON Profiling (operator-level timing)[/bold cyan]")

    for qid, meta in queries.items():
        sql = meta["sql"].strip()
        profile_path = output_dir / f"profile_{qid}.json"

        try:
            con.execute("PRAGMA enable_profiling='json'")
            con.execute(f"PRAGMA profile_output='{profile_path}'")
            con.execute(sql).fetchall()
            con.execute("PRAGMA disable_profiling")
            console.print(f"  [green]✓[/green] {qid} → {profile_path.name}")
        except Exception as e:
            console.print(f"  [red]✗[/red] {qid}: {e}")

    console.print(f"\n[green]Profiling output saved to {output_dir}/profile_*.json[/green]")
    console.print("[dim]Open these JSON files to see per-operator timing breakdown.[/dim]")


# ============================================================
#  Scaling experiment — speedup at different dataset sizes
# ============================================================
def run_scaling_experiment(
    duckdb_con, mysql_engine,
    query_ids: list[str] | None = None,
    n_runs: int = 5,
) -> list[dict]:
    """Run a single benchmark pass and return results for scaling comparison."""
    queries = BENCHMARK_QUERIES
    if query_ids:
        queries = {k: v for k, v in BENCHMARK_QUERIES.items() if k in query_ids}

    row_count = duckdb_con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    results = []

    for qid, meta in queries.items():
        sql = meta["sql"].strip()

        _, duck_ms, _ = run_query(duckdb_con, sql, "duckdb", n_runs)
        mysql_ms = -1.0
        if mysql_engine:
            _, mysql_ms, _ = run_query(mysql_engine, sql, "mysql", n_runs)

        speedup = round(mysql_ms / duck_ms, 2) if duck_ms > 0 and mysql_ms > 0 else None

        results.append({
            "rows": row_count,
            "query_id": qid,
            "duckdb_ms": duck_ms,
            "mysql_ms": mysql_ms,
            "speedup": speedup,
        })

    return results
