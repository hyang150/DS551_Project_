"""
db_run.py — DuckDB vs MySQL 性能对比 Benchmark
包含数据库初始化、查询执行、多次运行取中位数、结果保存
"""

import csv
import json
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

console = Console()

# ============================================================
#  MySQL 配置 — 按你本地 WSL 环境修改
# ============================================================
MYSQL_USER     = "root"
MYSQL_PASSWORD = "password"
MYSQL_HOST     = "127.0.0.1"
MYSQL_PORT     = "3306"
MYSQL_DB       = "stock_db"


# ============================================================
#  Benchmark 查询集合
#  每条查询代表一种典型的 OLAP 分析场景
#  mapping 字段直接对应 midterm report 的 "Internals → Application" 映射
# ============================================================
BENCHMARK_QUERIES = {
    "Q1_50day_MA": {
        "description": "50日均线 (Window Function)",
        "focus": "Vectorized Execution",
        "mapping": (
            "DuckDB: STREAMING_WINDOW operator 按 2048-tuple chunk 批量计算 AVG，"
            "利用 SIMD 指令加速。\n"
            "MySQL: 逐行迭代 window frame，每行独立计算，function call overhead 高。"
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
        "description": "每日波动率 — Narrow Projection (只读 3 列)",
        "focus": "Columnar Storage — Column Pruning",
        "mapping": (
            "DuckDB: 列存只读 High/Low/Close 三列的 column segment，"
            "跳过 Open/Volume/Symbol/Date，I/O 减少 ~57%。\n"
            "MySQL: 行存必须读取整行再丢弃不需要的列。"
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
        "description": "年度汇总统计 (GROUP BY + 多聚合)",
        "focus": "Vectorized Aggregation",
        "mapping": (
            "DuckDB: PERFECT_HASH_GROUP_BY / HASH_GROUP_BY operator，"
            "对 vector chunk 内数据批量 hash + aggregate。\n"
            "MySQL: 逐行读取 → hash → 更新聚合状态，cache miss 率高。"
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
        "description": "全表聚合 — Narrow (只读 Close + Volume)",
        "focus": "Columnar Storage I/O — Narrow Scan",
        "mapping": (
            "DuckDB: 只扫描 Close 和 Volume 两列的 column segment，"
            "压缩后 I/O 极小。\n"
            "MySQL: 全行扫描，读取所有 7 列数据。"
        ),
        "sql": """
            SELECT COUNT(*), ROUND(AVG(Close), 4), SUM(Volume)
            FROM stock_data
        """,
    },
    "Q5_rolling_stddev": {
        "description": "20日滚动标准差 (波动率指标)",
        "focus": "Vectorized Window + STDDEV",
        "mapping": (
            "DuckDB: STREAMING_WINDOW 内 STDDEV 按 chunk 批量计算，"
            "避免重复遍历 window frame。\n"
            "MySQL: 每行重新扫描 20 行 frame 计算标准差。"
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
        "description": "全列投影 SELECT * — Wide Scan 对照实验",
        "focus": "Columnar Storage — Wide Scan (列存劣势场景)",
        "mapping": (
            "DuckDB: 需要读取所有列并重组行 (tuple reconstruction)，"
            "列存优势消失，甚至可能比行存略慢。\n"
            "MySQL: 行存天然按行组织，SELECT * 无额外开销。\n"
            "对照 Q4 的 narrow scan，说明列存适用场景。"
        ),
        "sql": """
            SELECT *
            FROM stock_data
            ORDER BY Date DESC
            LIMIT 5000
        """,
    },
}


# ============================================================
#  数据库初始化
# ============================================================
def setup_duckdb(df: pd.DataFrame, duckdb_file: str) -> duckdb.DuckDBPyConnection:
    """初始化 DuckDB，建表 + 索引"""
    console.print(f"\n[cyan][*] 初始化 DuckDB ({duckdb_file})...[/cyan]")
    t0 = time.perf_counter()

    con = duckdb.connect(duckdb_file)
    con.execute("DROP TABLE IF EXISTS stock_data")
    con.execute("CREATE TABLE stock_data AS SELECT * FROM df")
    con.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data(Symbol)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_date   ON stock_data(Date)")

    count = con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    elapsed = time.perf_counter() - t0
    console.print(f"[green][+] DuckDB 就绪: {count} 行, 导入耗时 {elapsed:.3f}s[/green]")
    return con


def setup_mysql(df: pd.DataFrame):
    """初始化 MySQL，建表 + 索引（保证公平对比）"""
    console.print(f"\n[cyan][*] 连接 WSL MySQL ({MYSQL_HOST}:{MYSQL_PORT})...[/cyan]")

    try:
        root_engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/"
        )
        with root_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
    except Exception as e:
        console.print(f"[red][!] MySQL 连接失败: {e}[/red]")
        console.print("[yellow]    请确认: sudo service mysql start[/yellow]")
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
                "Symbol": String(10),      # VARCHAR(10)，避免 TEXT 无法建索引
                "Date":   Date(),
                "Open":   Float(),
                "High":   Float(),
                "Low":    Float(),
                "Close":  Float(),
                "Volume": BigInteger(),
            },
        )

        # ★ 关键改进：给 MySQL 也建索引，保证公平对比
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_symbol (Symbol)"))
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_date (Date)"))
            conn.execute(text("ALTER TABLE stock_data ADD INDEX idx_sym_date (Symbol, Date)"))
            conn.commit()

        elapsed = time.perf_counter() - t0
        console.print(f"[green][+] MySQL 就绪 (含索引), 导入耗时 {elapsed:.3f}s[/green]")
    except Exception as e:
        console.print(f"[red][!] MySQL 写入失败: {e}[/red]")
        return None

    return engine


# ============================================================
#  查询执行 — 支持多次运行取中位数
# ============================================================
def run_query_once(engine_or_con, sql: str, db_type: str) -> tuple[pd.DataFrame | None, float]:
    """执行单次查询，返回 (DataFrame, 毫秒耗时)"""
    try:
        t0 = time.perf_counter()
        if db_type == "duckdb":
            result = engine_or_con.execute(sql).fetchdf()
        else:
            result = pd.read_sql(sql, engine_or_con)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return result, round(elapsed_ms, 3)
    except Exception as e:
        console.print(f"[red]  [{db_type}] 查询失败: {e}[/red]")
        return None, -1.0


def run_query(engine_or_con, sql: str, db_type: str, n_runs: int = 5) -> tuple[pd.DataFrame | None, float, list[float]]:
    """
    执行 n_runs 次查询，第一次 warm-up 丢弃，取后 (n_runs-1) 次中位数。
    返回 (最后一次 DataFrame, 中位数 ms, 全部耗时列表)
    """
    all_times = []
    last_df = None

    for i in range(n_runs):
        df, ms = run_query_once(engine_or_con, sql, db_type)
        all_times.append(ms)
        if df is not None:
            last_df = df

    # 丢弃第一次 (warm-up)，取后面的中位数
    valid_times = [t for t in all_times[1:] if t >= 0]
    if valid_times:
        median_ms = round(statistics.median(valid_times), 3)
    else:
        median_ms = -1.0

    return last_df, median_ms, all_times


# ============================================================
#  运行全套 Benchmark
# ============================================================
def run_benchmark(
    duckdb_con,
    mysql_engine,
    n_runs: int = 5,
    query_ids: list[str] | None = None,
    output_dir: Path = Path.cwd() / "output",
) -> tuple[list[dict], str]:
    """
    运行 benchmark，返回 (results_list, session_timestamp)。

    Parameters
    ----------
    n_runs : int
        每条查询运行次数（第一次 warm-up）
    query_ids : list[str] | None
        指定运行哪些查询，None = 全部
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

    # Rich 表格
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

        # 预览前 3 行
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
#  保存结果
# ============================================================
def save_results(results: list[dict], session_ts: str, output_dir: Path = Path.cwd() / "output"):
    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV — 追加模式
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

    # JSON 快照
    json_path = output_dir / f"benchmark_{session_ts}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    console.print(f"\n[green][+] 结果已保存:[/green]")
    console.print(f"    CSV  (累计) → {csv_path}")
    console.print(f"    JSON (本次) → {json_path}")

    return csv_path, json_path
