"""
explain_analyzer.py — EXPLAIN ANALYZE 执行计划分析
生成 DuckDB 与 MySQL 的执行计划对比报告，直接用于 Midterm Report
"""

import pandas as pd
from pathlib import Path
from rich.console import Console
from rich.panel import Panel

from db_run import BENCHMARK_QUERIES

console = Console()


# ============================================================
#  已知 DuckDB Operator 的中文注解
#  用于自动标注 EXPLAIN 输出中的关键 operator
# ============================================================
DUCKDB_OPERATOR_NOTES = {
    "SEQ_SCAN":               "顺序扫描 — 列存下只读取被引用的列 (Column Pruning)",
    "FILTER":                 "过滤 — 向量化批量比较，利用 SIMD 加速",
    "HASH_GROUP_BY":          "哈希分组聚合 — 向量化批量 hash + aggregate",
    "PERFECT_HASH_GROUP_BY":  "完美哈希分组 — 低基数 key 的优化路径",
    "STREAMING_WINDOW":       "流式窗口函数 — 按 chunk 批量计算，无需物化中间结果",
    "ORDER_BY":               "排序 — 向量化比较 + 合并排序",
    "PROJECTION":             "投影 — 列裁剪后直接输出所需列",
    "UNGROUPED_AGGREGATE":    "无分组聚合 — 向量化 COUNT/SUM/AVG",
    "TOP_N":                  "Top-N 优化 — 堆排序取前 N 行，避免全量排序",
    "HASH_JOIN":              "哈希连接 — 向量化 probe 端批量查找",
    "CHUNK_SCAN":             "内部 chunk 扫描 — 每次处理 2048 tuples",
}


def explain_duckdb(con, sql: str, query_id: str) -> str:
    """
    执行 DuckDB EXPLAIN ANALYZE，返回格式化文本 + operator 注解。
    """
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"DuckDB EXPLAIN ANALYZE: {query_id}")
    lines.append(f"{'='*60}\n")

    try:
        # 先跑普通 EXPLAIN（逻辑计划，更好看）
        logical = con.execute(f"EXPLAIN {sql}").fetchdf()
        lines.append("--- Logical Plan ---")
        plan_text = logical.to_string(index=False)
        lines.append(plan_text)
        lines.append("")

        # 标注关键 operator
        lines.append("--- Operator 注解 ---")
        found_ops = []
        for op, note in DUCKDB_OPERATOR_NOTES.items():
            if op.lower() in plan_text.lower():
                found_ops.append(f"  • {op}: {note}")
        if found_ops:
            lines.extend(found_ops)
        else:
            lines.append("  (未识别到已知 operator)")
        lines.append("")

        # EXPLAIN ANALYZE（含实际运行时间）
        analyze = con.execute(f"EXPLAIN ANALYZE {sql}").fetchdf()
        lines.append("--- Physical Plan with Timing ---")
        lines.append(analyze.to_string(index=False))
        lines.append("")

    except Exception as e:
        lines.append(f"[ERROR] DuckDB EXPLAIN 失败: {e}\n")

    return "\n".join(lines)


def explain_mysql(engine, sql: str, query_id: str) -> str:
    """
    执行 MySQL EXPLAIN，返回格式化文本 + 关键指标解读。
    """
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"MySQL EXPLAIN: {query_id}")
    lines.append(f"{'='*60}\n")

    if engine is None:
        lines.append("[SKIP] MySQL 未连接\n")
        return "\n".join(lines)

    try:
        plan = pd.read_sql(f"EXPLAIN {sql}", engine)
        lines.append(plan.to_string(index=False))
        lines.append("")

        # 自动解读关键字段
        lines.append("--- 关键指标解读 ---")
        for _, row in plan.iterrows():
            scan_type = str(row.get("type", ""))
            extra     = str(row.get("Extra", ""))
            rows_est  = row.get("rows", "?")

            if scan_type == "ALL":
                lines.append(f"  • type=ALL: 全表扫描 (Full Table Scan)，预估扫描 {rows_est} 行")
            elif scan_type == "index":
                lines.append(f"  • type=index: 索引全扫描，预估 {rows_est} 行")
            elif scan_type in ("ref", "range"):
                lines.append(f"  • type={scan_type}: 使用索引，预估 {rows_est} 行")

            if "Using filesort" in extra:
                lines.append("  • Using filesort: 需要额外排序（非索引顺序）")
            if "Using temporary" in extra:
                lines.append("  • Using temporary: 使用临时表")
            if "Using where" in extra:
                lines.append("  • Using where: 在存储引擎返回后再次过滤")

        lines.append("")

    except Exception as e:
        lines.append(f"[ERROR] MySQL EXPLAIN 失败: {e}\n")

    return "\n".join(lines)


def generate_explain_report(
    duckdb_con,
    mysql_engine,
    query_ids: list[str] | None = None,
    output_dir: Path = Path.cwd() / "output",
) -> Path:
    """
    对指定查询生成 EXPLAIN 对比报告，保存为文本文件。

    Parameters
    ----------
    query_ids : list[str] | None
        要分析的查询 ID，None = 全部
    output_dir : Path
        输出目录

    Returns
    -------
    Path  报告文件路径
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if query_ids is None:
        query_ids = list(BENCHMARK_QUERIES.keys())

    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("  EXPLAIN ANALYZE 对比报告")
    report_lines.append("  DuckDB (Columnar + Vectorized) vs MySQL (Row-Based)")
    report_lines.append("=" * 70)
    report_lines.append("")

    for qid in query_ids:
        if qid not in BENCHMARK_QUERIES:
            console.print(f"[yellow]  跳过未知 query_id: {qid}[/yellow]")
            continue

        meta = BENCHMARK_QUERIES[qid]
        sql  = meta["sql"].strip()

        report_lines.append(f"\n{'#'*60}")
        report_lines.append(f"# {qid}: {meta['description']}")
        report_lines.append(f"# Focus: {meta['focus']}")
        report_lines.append(f"{'#'*60}")
        report_lines.append(f"\nSQL:\n{sql}\n")

        # Mapping 说明
        report_lines.append("--- Internals → Application Mapping ---")
        report_lines.append(meta.get("mapping", "(mapping 未填写)"))
        report_lines.append("")

        # DuckDB
        console.print(f"  [cyan]EXPLAIN[/cyan] {qid} — DuckDB...")
        report_lines.append(explain_duckdb(duckdb_con, sql, qid))

        # MySQL
        console.print(f"  [cyan]EXPLAIN[/cyan] {qid} — MySQL...")
        report_lines.append(explain_mysql(mysql_engine, sql, qid))

        report_lines.append("\n")

    # 写文件
    report_path = output_dir / "explain_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    console.print(f"\n[green][+] EXPLAIN 报告已保存 → {report_path}[/green]")
    return report_path
