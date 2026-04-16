"""
mongo_run.py — MongoDB comparison for the DSCI 551 final report.

Adds a third storage model (document store) to the DuckDB vs MySQL
benchmark. Three representative queries are run so that the final
report has direct MongoDB numbers to contrast against the relational
engines:

    M1  narrow projection / aggregation  (compare with Q4)
    M2  annual group-by                   (compare with Q3)
    M3  point lookup by (Symbol, Date)    (compare with Q7)

Usage:
    python mongo_run.py                    # load data + run 3 queries
    python mongo_run.py --compare-all      # also run DuckDB + MySQL equivalents
    python mongo_run.py --skip-load        # reuse data already in MongoDB

Requirements:
    pip install pymongo python-dotenv
    MongoDB running locally (or set MONGO_URI in .env).
"""

import argparse
import os
import statistics
import time
import warnings
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# ── Optional .env support ──
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

warnings.filterwarnings("ignore")
console = Console()

# ============================================================
#  Configuration
# ============================================================
MONGO_URI        = os.getenv("MONGO_URI",        "mongodb://127.0.0.1:27017")
MONGO_DB         = os.getenv("MONGO_DB",         "stock_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "stock_data")

PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR    = PROJECT_DIR / "data"


# ============================================================
#  MongoDB queries — aggregation-pipeline equivalents of Q3/Q4/Q7
# ============================================================
MONGO_QUERIES = {
    "M1_narrow_aggregate": {
        "description": "Full-collection aggregate — AVG(Close), SUM(Volume)",
        "compare_with": "Q4_full_scan_narrow",
        "focus": "Narrow aggregation (document-store must still read full BSON docs)",
        "mapping": (
            "DuckDB: reads only Close + Volume column segments (2 / 7).\n"
            "MySQL: reads full InnoDB rows, discards unused cols.\n"
            "MongoDB: reads full BSON documents from WiredTiger — there is no\n"
            "         column pruning, so even a 'narrow' aggregate touches every\n"
            "         field on disk. This is the clearest demonstration of why\n"
            "         column-store wins on analytical scans."
        ),
        "pipeline": [
            {"$group": {
                "_id": None,
                "count":     {"$sum": 1},
                "avg_close": {"$avg": "$Close"},
                "sum_volume":{"$sum": "$Volume"},
            }},
        ],
    },
    "M2_annual_summary": {
        "description": "Annual group-by per stock — avg/max/min/sum",
        "compare_with": "Q3_annual_summary",
        "focus": "Document-store GROUP BY (uses aggregation pipeline hash stage)",
        "mapping": (
            "DuckDB: HASH_GROUP_BY on vector batches of (Year, Symbol).\n"
            "MySQL: row-by-row hash, Using temporary.\n"
            "MongoDB: $group stage, which for large groups spills to disk\n"
            "         (allowDiskUse=true). No vectorization; the engine walks\n"
            "         the index (or collection) one doc at a time."
        ),
        "pipeline": [
            {"$group": {
                "_id": {
                    "Year":   {"$year": "$Date"},
                    "Symbol": "$Symbol",
                },
                "avg_close":   {"$avg": "$Close"},
                "year_high":   {"$max": "$High"},
                "year_low":    {"$min": "$Low"},
                "total_volume":{"$sum": "$Volume"},
            }},
            {"$sort": {"_id.Year": 1, "_id.Symbol": 1}},
        ],
    },
    "M3_point_lookup": {
        "description": "Point lookup — single document by (Symbol, Date)",
        "compare_with": "Q7_point_lookup",
        "focus": "Index-backed OLTP read — MongoDB's sweet spot",
        "mapping": (
            "DuckDB: must scan row groups (no primary-key B-tree).\n"
            "MySQL: O(log n) via clustered B-tree on (Symbol, Date).\n"
            "MongoDB: O(log n) via WiredTiger B-tree index on {Symbol:1,Date:1}.\n"
            "         Competitive with MySQL — both use B-tree indexes, the\n"
            "         main difference is BSON parsing vs row decoding overhead."
        ),
        # find_one uses the {Symbol:1, Date:1} index directly.
        "filter": {
            "Symbol": "AAPL",
            # Date is filled in at runtime from a row actually present in the data.
            "Date":   None,
        },
    },
}


# ============================================================
#  Load data
# ============================================================
def load_parquet_to_mongo(client, df: pd.DataFrame):
    """Drop existing collection, bulk-insert the DataFrame, create indexes."""
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]

    console.print(f"\n[cyan][*] Loading {len(df):,} rows into MongoDB "
                  f"{MONGO_DB}.{MONGO_COLLECTION}[/cyan]")

    coll.drop()

    # Pandas dates → python datetime for BSON.
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])

    # Insert in 5k-row chunks (matches the MySQL chunk size for fairness).
    batch = 5000
    total = len(df)
    t0 = time.perf_counter()
    for start in range(0, total, batch):
        chunk = df.iloc[start:start + batch].to_dict("records")
        coll.insert_many(chunk, ordered=False)
    elapsed = time.perf_counter() - t0
    console.print(f"[green][+] Inserted {total:,} docs in {elapsed:.2f}s"
                  f" ({total / elapsed:,.0f} docs/sec)[/green]")

    # Mirror the MySQL indexes for fair OLTP comparison.
    coll.create_index("Symbol")
    coll.create_index("Date")
    coll.create_index([("Symbol", 1), ("Date", 1)])
    console.print(f"[green][+] Created 3 indexes on {MONGO_COLLECTION}[/green]")

    return coll


# ============================================================
#  Benchmark helpers
# ============================================================
def _time_pipeline(coll, pipeline, n_runs: int):
    """Run an aggregation pipeline n_runs times, drop warm-up, median ms."""
    times = []
    last = None
    for _ in range(n_runs):
        t0 = time.perf_counter()
        last = list(coll.aggregate(pipeline, allowDiskUse=True))
        times.append((time.perf_counter() - t0) * 1000)
    measured = times[1:] if len(times) > 1 else times
    return last, round(statistics.median(measured), 3), times


def _time_find_one(coll, filt, n_runs: int):
    times = []
    last = None
    for _ in range(n_runs):
        t0 = time.perf_counter()
        last = coll.find_one(filt)
        times.append((time.perf_counter() - t0) * 1000)
    measured = times[1:] if len(times) > 1 else times
    return last, round(statistics.median(measured), 3), times


def run_mongo_benchmark(coll, n_runs: int = 5) -> list[dict]:
    """Run M1–M3 against the collection and return a result list."""
    results = []

    # Resolve the point-lookup date dynamically — pick a real row in the data.
    any_aapl = coll.find_one({"Symbol": "AAPL"}, sort=[("Date", -1)])
    if any_aapl is not None:
        MONGO_QUERIES["M3_point_lookup"]["filter"]["Date"] = any_aapl["Date"]

    table = Table(title="MongoDB Benchmark (Median ms)", show_lines=True)
    table.add_column("Query ID",   style="bold")
    table.add_column("Description", style="dim")
    table.add_column("Median (ms)", justify="right", style="green")
    table.add_column("Compares With", style="cyan")

    for qid, meta in MONGO_QUERIES.items():
        console.print(f"\n[bold]▶ {qid}[/bold] — {meta['description']}")

        if "pipeline" in meta:
            last, median_ms, all_ms = _time_pipeline(coll, meta["pipeline"], n_runs)
        else:
            last, median_ms, all_ms = _time_find_one(coll, meta["filter"], n_runs)

        console.print(f"  times: {all_ms} → median={median_ms}ms")
        if last:
            # Only print a compact preview — avoid flooding the demo console.
            preview = last if isinstance(last, list) else [last]
            console.print(f"  first result: {str(preview[0])[:200]}")

        table.add_row(
            qid, meta["description"], f"{median_ms:.2f}", meta["compare_with"],
        )

        results.append({
            "query_id":    qid,
            "description": meta["description"],
            "median_ms":   median_ms,
            "all_ms":      all_ms,
            "compare_with":meta["compare_with"],
            "focus":       meta["focus"],
        })

    console.print(table)
    return results


# ============================================================
#  Optional: run the same 3 questions against DuckDB + MySQL
# ============================================================
def run_cross_engine_comparison(df: pd.DataFrame, mongo_results: list[dict],
                                 n_runs: int = 5):
    """Side-by-side table vs DuckDB and MySQL, for the final report."""
    from db_run import setup_duckdb, setup_mysql, run_query, BENCHMARK_QUERIES

    console.print(Panel(
        "[bold]Cross-engine comparison[/bold] — same 3 questions on DuckDB + MySQL.\n"
        "MongoDB numbers are reused from the preceding run.",
        expand=False,
    ))

    duck_file = str(DATA_DIR / "stock_analytics_mongo_compare.duckdb")
    duck = setup_duckdb(df, duck_file)
    mysql = setup_mysql(df)

    # Mapping from MongoDB query ID → equivalent DuckDB/MySQL query ID.
    pairs = [(m["query_id"], m["compare_with"], m["median_ms"]) for m in mongo_results]

    table = Table(title="DuckDB vs MySQL vs MongoDB (Median ms)", show_lines=True)
    table.add_column("Question",           style="bold")
    table.add_column("DuckDB (ms)",        justify="right", style="green")
    table.add_column("MySQL (ms)",         justify="right", style="yellow")
    table.add_column("MongoDB (ms)",       justify="right", style="magenta")
    table.add_column("Winner",             justify="center")

    for mid, relational_qid, mongo_ms in pairs:
        sql = BENCHMARK_QUERIES[relational_qid]["sql"]
        _, duck_ms, _ = run_query(duck, sql, "duckdb", n_runs)
        mysql_ms = -1.0
        if mysql is not None:
            _, mysql_ms, _ = run_query(mysql, sql, "mysql", n_runs)

        all_times = [
            ("DuckDB",  duck_ms),
            ("MySQL",   mysql_ms),
            ("MongoDB", mongo_ms),
        ]
        valid = [(name, t) for name, t in all_times if t > 0]
        winner = min(valid, key=lambda p: p[1])[0] if valid else "N/A"

        table.add_row(
            f"{mid}  (≈ {relational_qid})",
            f"{duck_ms:.1f}"  if duck_ms  > 0 else "N/A",
            f"{mysql_ms:.1f}" if mysql_ms > 0 else "N/A",
            f"{mongo_ms:.1f}" if mongo_ms > 0 else "N/A",
            winner,
        )

    console.print(table)


# ============================================================
#  Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="MongoDB benchmark — DSCI 551")
    parser.add_argument("--skip-load",    action="store_true",
                        help="Reuse data already loaded in MongoDB")
    parser.add_argument("--compare-all",  action="store_true",
                        help="Also run DuckDB + MySQL equivalents side-by-side")
    parser.add_argument("--runs",         type=int, default=5,
                        help="Runs per query (first is warm-up, default=5)")
    args = parser.parse_args()

    # ── MongoDB connection ──
    try:
        from pymongo import MongoClient
    except ImportError:
        console.print("[red][!] pymongo not installed. Run: pip install pymongo[/red]")
        return

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    except Exception as e:
        console.print(f"[red][!] Cannot connect to MongoDB at {MONGO_URI}[/red]")
        console.print(f"[red]    {e}[/red]")
        console.print("[yellow]    Try: sudo service mongod start[/yellow]")
        return

    console.print(Panel(
        f"[bold]MongoDB Benchmark[/bold]\n"
        f"URI:        {MONGO_URI}\n"
        f"Database:   {MONGO_DB}\n"
        f"Collection: {MONGO_COLLECTION}\n"
        f"Runs:       {args.runs} (first discarded as warm-up)",
        expand=False,
    ))

    # ── Load data (unless told to reuse existing) ──
    from downloader import fetch_stock_data, DEFAULT_SYMBOLS

    df = fetch_stock_data(DEFAULT_SYMBOLS, data_dir=DATA_DIR)
    console.print(f"[green]  Dataset: {len(df):,} rows, "
                  f"{df['Symbol'].nunique()} stocks[/green]")

    if args.skip_load:
        coll = client[MONGO_DB][MONGO_COLLECTION]
        existing = coll.estimated_document_count()
        if existing == 0:
            console.print("[yellow][*] --skip-load set but collection is empty, "
                          "loading anyway[/yellow]")
            coll = load_parquet_to_mongo(client, df)
        else:
            console.print(f"[green][+] Reusing {existing:,} documents already in "
                          f"MongoDB[/green]")
    else:
        coll = load_parquet_to_mongo(client, df)

    # ── Benchmark ──
    mongo_results = run_mongo_benchmark(coll, n_runs=args.runs)

    # ── Optional: cross-engine comparison ──
    if args.compare_all:
        run_cross_engine_comparison(df, mongo_results, n_runs=args.runs)

    console.print("\n[green][+] Done. These numbers belong in the Final Report § 6 "
                  "(Comparison with MySQL AND MongoDB).[/green]")


if __name__ == "__main__":
    main()
