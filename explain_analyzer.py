"""
explain_analyzer.py — EXPLAIN ANALYZE plan analysis.
Generates a side-by-side execution-plan comparison between DuckDB and MySQL,
written to disk for direct inclusion in the midterm / final report.
"""

import pandas as pd
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from db_run import BENCHMARK_QUERIES

console = Console()


# ============================================================
#  Annotations for known DuckDB operators.
#  Used to auto-tag the key operators in EXPLAIN output.
# ============================================================
DUCKDB_OPERATOR_NOTES = {
    "SEQ_SCAN":               "Sequential scan — column store reads only the referenced columns (column pruning).",
    "FILTER":                 "Filter — vectorized batched comparison; SIMD-accelerated.",
    "HASH_GROUP_BY":          "Hash group-by aggregation — vectorized hash + aggregate per chunk.",
    "PERFECT_HASH_GROUP_BY":  "Perfect hash group-by — optimized path for low-cardinality keys.",
    "STREAMING_WINDOW":       "Streaming window — chunked computation, no materialization of intermediate frames.",
    "ORDER_BY":               "Order-by — vectorized comparison + merge sort.",
    "PROJECTION":             "Projection — emits only the required columns after pruning.",
    "UNGROUPED_AGGREGATE":    "Ungrouped aggregate — vectorized COUNT/SUM/AVG over the whole input.",
    "TOP_N":                  "Top-N optimization — heap sort for the first N rows; avoids full sort.",
    "HASH_JOIN":              "Hash join — vectorized batched probes.",
    "CHUNK_SCAN":             "Internal chunk scan — processes 2048 tuples per batch.",
}


def explain_duckdb(con, sql: str, query_id: str) -> str:
    """Run DuckDB EXPLAIN ANALYZE and return formatted text + operator notes."""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"DuckDB EXPLAIN ANALYZE: {query_id}")
    lines.append(f"{'='*60}\n")

    try:
        # First the logical plan (easier to read)
        logical = con.execute(f"EXPLAIN {sql}").fetchdf()
        lines.append("--- Logical Plan ---")
        plan_text = logical.to_string(index=False)
        lines.append(plan_text)
        lines.append("")

        # Tag the key operators we recognize
        lines.append("--- Operator Notes ---")
        found_ops = []
        for op, note in DUCKDB_OPERATOR_NOTES.items():
            if op.lower() in plan_text.lower():
                found_ops.append(f"  - {op}: {note}")
        if found_ops:
            lines.extend(found_ops)
        else:
            lines.append("  (no known operator detected)")
        lines.append("")

        # Physical plan with actual runtime
        analyze = con.execute(f"EXPLAIN ANALYZE {sql}").fetchdf()
        lines.append("--- Physical Plan with Timing ---")
        lines.append(analyze.to_string(index=False))
        lines.append("")

    except Exception as e:
        lines.append(f"[ERROR] DuckDB EXPLAIN failed: {e}\n")

    return "\n".join(lines)


def explain_mysql(engine, sql: str, query_id: str) -> str:
    """Run MySQL EXPLAIN and return formatted text + key-metric interpretation."""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"MySQL EXPLAIN: {query_id}")
    lines.append(f"{'='*60}\n")

    if engine is None:
        lines.append("[SKIP] MySQL not connected\n")
        return "\n".join(lines)

    try:
        plan = pd.read_sql(f"EXPLAIN {sql}", engine)
        lines.append(plan.to_string(index=False))
        lines.append("")

        # Auto-interpret the important columns
        lines.append("--- Key Metrics ---")
        for _, row in plan.iterrows():
            scan_type = str(row.get("type", ""))
            extra     = str(row.get("Extra", ""))
            rows_est  = row.get("rows", "?")

            if scan_type == "ALL":
                lines.append(f"  - type=ALL: full table scan, ~{rows_est} rows estimated")
            elif scan_type == "index":
                lines.append(f"  - type=index: full index scan, ~{rows_est} rows estimated")
            elif scan_type in ("ref", "range"):
                lines.append(f"  - type={scan_type}: index used, ~{rows_est} rows estimated")

            if "Using filesort" in extra:
                lines.append("  - Using filesort: extra sort required (not in index order)")
            if "Using temporary" in extra:
                lines.append("  - Using temporary: temporary table required")
            if "Using where" in extra:
                lines.append("  - Using where: extra filter applied after the storage engine returns rows")

        lines.append("")

    except Exception as e:
        lines.append(f"[ERROR] MySQL EXPLAIN failed: {e}\n")

    return "\n".join(lines)


def generate_explain_report(
    duckdb_con,
    mysql_engine,
    query_ids: list[str] | None = None,
    output_dir: Path = Path.cwd() / "output",
) -> Path:
    """
    Generate an EXPLAIN comparison report for the given queries and save it
    as a text file.

    Parameters
    ----------
    query_ids : list[str] | None
        Query IDs to analyze; None means all.
    output_dir : Path
        Output directory.

    Returns
    -------
    Path  Path to the report file.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if query_ids is None:
        query_ids = list(BENCHMARK_QUERIES.keys())

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("  EXPLAIN ANALYZE Comparison Report")
    report_lines.append("  DuckDB (Columnar + Vectorized) vs MySQL (Row-Based)")
    report_lines.append("=" * 70)
    report_lines.append("")

    for qid in query_ids:
        if qid not in BENCHMARK_QUERIES:
            console.print(f"[yellow]  Skipping unknown query_id: {qid}[/yellow]")
            continue

        meta = BENCHMARK_QUERIES[qid]
        sql  = meta["sql"].strip()

        report_lines.append(f"\n{'#'*60}")
        report_lines.append(f"# {qid}: {meta['description']}")
        report_lines.append(f"# Focus: {meta['focus']}")
        report_lines.append(f"{'#'*60}")
        report_lines.append(f"\nSQL:\n{sql}\n")

        # Mapping
        report_lines.append("--- Internals -> Application Mapping ---")
        report_lines.append(meta.get("mapping", "(mapping not provided)"))
        report_lines.append("")

        # DuckDB
        console.print(f"  [cyan]EXPLAIN[/cyan] {qid} — DuckDB...")
        report_lines.append(explain_duckdb(duckdb_con, sql, qid))

        # MySQL
        console.print(f"  [cyan]EXPLAIN[/cyan] {qid} — MySQL...")
        report_lines.append(explain_mysql(mysql_engine, sql, qid))

        report_lines.append("\n")

    # Write the report file
    report_path = output_dir / "explain_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    console.print(f"\n[green][+] EXPLAIN report saved -> {report_path}[/green]")
    return report_path
