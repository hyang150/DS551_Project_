"""
Stock Market Analytics Dashboard
DuckDB vs MySQL Performance Benchmark

对应 DSCI 551 Project: Inside DuckDB — Columnar Storage & Vectorized Execution
Author: Hanwen Yang / Jialiang Lou
"""

import csv
import json
import time
import warnings
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import yfinance as yf
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

console = Console()

# ============================================================
#  目录配置 — 运行时自动创建
# ============================================================
BASE_DIR   = Path.cwd()          # 始终是你 cd 到的那个目录（即 main.py 所在位置）
DATA_DIR   = BASE_DIR / "data"   # 存放原始数据
OUTPUT_DIR = BASE_DIR / "output" # 存放每次 benchmark 结果

DATA_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
#  数据库配置 — 按你本地 WSL MySQL 环境修改
# ============================================================
MYSQL_USER     = "root"
MYSQL_PASSWORD = "password"   # WSL 内 MySQL root 密码
MYSQL_HOST     = "127.0.0.1"  # WSL 本机直连用 127.0.0.1
MYSQL_PORT     = "3306"
MYSQL_DB       = "stock_db"

DUCKDB_FILE = str(DATA_DIR / "stock_analytics.duckdb")

# ============================================================
#  数据获取配置
# ============================================================
STOCK_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]   # 多股票拉宽数据量
START_DATE    = "2015-01-01"
END_DATE      = "2025-01-01"


# ============================================================
#  Benchmark 查询集合
#  每条查询代表一种典型的 OLAP 分析场景
# ============================================================
BENCHMARK_QUERIES = {
    "Q1_50day_MA": {
        "description": "50日均线 (Window Function)",
        "focus": "Vectorized Execution",
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
        "description": "每日波动率 (High-Low / Close)",
        "focus": "Columnar Scan — 只读 High/Low/Close 三列",
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
    "Q4_full_scan": {
        "description": "全表扫描 — 行 vs 列存储 I/O 压力测试",
        "focus": "Columnar Storage I/O",
        "sql": """
            SELECT COUNT(*), ROUND(AVG(Close), 4), SUM(Volume)
            FROM stock_data
        """,
    },
    "Q5_rolling_stddev": {
        "description": "20日滚动标准差 (波动率指标)",
        "focus": "Vectorized Window + STDDEV",
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
}


# ============================================================
#  1. 数据获取 & 本地缓存 (Parquet)
# ============================================================
def fetch_stock_data(symbols: list[str], start: str, end: str) -> pd.DataFrame:
    parquet_path = DATA_DIR / f"{'_'.join(symbols)}_{start[:4]}_{end[:4]}.parquet"

    # 命中本地缓存则跳过下载
    if parquet_path.exists():
        console.print(f"[green][+] 读取本地缓存: {parquet_path}[/green]")
        return pd.read_parquet(parquet_path)

    console.print(f"[cyan][*] 从 Yahoo Finance 下载 {symbols} ({start} ~ {end})...[/cyan]")

    frames = []
    for sym in symbols:
        df = yf.download(sym, start=start, end=end, auto_adjust=True, progress=False)
        df.reset_index(inplace=True)

        # 压平 MultiIndex（yfinance ≥0.2.x 可能返回多层列名）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df["Symbol"] = sym
        df = df[["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]]
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        frames.append(df)
        console.print(f"  [{sym}] {len(df)} 行")

    combined = pd.concat(frames, ignore_index=True)
    combined.sort_values(["Symbol", "Date"], inplace=True)

    # Parquet：列式存储，读取速度比 CSV 快 5-10x，也是 DuckDB 原生支持的格式
    # CSV：方便你用 Excel / 文本编辑器直接查看原始数据
    combined.to_parquet(parquet_path, index=False)

    csv_path = DATA_DIR / f"{'_'.join(symbols)}_{start[:4]}_{end[:4]}.csv"
    combined.to_csv(csv_path, index=False)

    console.print(f"[green][+] 数据已保存至 data/ 目录 (共 {len(combined)} 行)[/green]")
    console.print(f"    Parquet → {parquet_path.name}  (DuckDB 加载用)")
    console.print(f"    CSV     → {csv_path.name}      (人工查看用)")
    return combined


# ============================================================
#  2. 初始化 DuckDB
# ============================================================
def setup_duckdb(df: pd.DataFrame) -> duckdb.DuckDBPyConnection:
    console.print(f"\n[cyan][*] 初始化 DuckDB ({DUCKDB_FILE})...[/cyan]")
    t0 = time.perf_counter()

    con = duckdb.connect(DUCKDB_FILE)
    con.execute("DROP TABLE IF EXISTS stock_data")
    con.execute("CREATE TABLE stock_data AS SELECT * FROM df")

    # 建索引加速后续 Symbol / Date 过滤
    con.execute("CREATE INDEX IF NOT EXISTS idx_symbol ON stock_data(Symbol)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_date   ON stock_data(Date)")

    count = con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
    elapsed = time.perf_counter() - t0
    console.print(f"[green][+] DuckDB 就绪: {count} 行, 导入耗时 {elapsed:.3f}s[/green]")
    return con


# ============================================================
#  3. 初始化 MySQL
# ============================================================
def setup_mysql(df: pd.DataFrame):
    console.print(f"\n[cyan][*] 连接 WSL MySQL ({MYSQL_HOST}:{MYSQL_PORT})...[/cyan]")

    try:
        root_engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/"
        )
        with root_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}"))
        console.print(f"  [-] 数据库 '{MYSQL_DB}' 确认存在")
    except Exception as e:
        console.print(f"[red][!] MySQL 连接失败: {e}[/red]")
        console.print("[yellow]    请确认 WSL 内 MySQL 服务已启动: sudo service mysql start[/yellow]")
        return None

    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )

    t0 = time.perf_counter()
    try:
        df.to_sql("stock_data", engine, if_exists="replace", index=False,
                  chunksize=5000, method="multi")
        elapsed = time.perf_counter() - t0
        console.print(f"[green][+] MySQL 就绪, 导入耗时 {elapsed:.3f}s[/green]")
    except Exception as e:
        console.print(f"[red][!] MySQL 写入失败: {e}[/red]")
        return None

    return engine


# ============================================================
#  4. 执行单条 Benchmark 查询，返回 (result_df, elapsed_ms)
# ============================================================
def run_query(engine_or_con, sql: str, db_type: str) -> tuple[pd.DataFrame | None, float]:
    """
    db_type: "duckdb" | "mysql"
    返回 (DataFrame, 毫秒耗时)
    """
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


# ============================================================
#  5. 运行全套 Benchmark & 保存结果
# ============================================================
def run_benchmark(duckdb_con, mysql_engine) -> list[dict]:
    session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    results: list[dict] = []

    console.print(Panel(
        "[bold cyan]DuckDB vs MySQL — Performance Benchmark[/bold cyan]\n"
        f"Session: {session_ts}",
        expand=False
    ))

    # Rich 结果表
    table = Table(title="Benchmark Results", show_lines=True)
    table.add_column("Query ID",      style="bold")
    table.add_column("Description",   style="dim")
    table.add_column("Focus")
    table.add_column("DuckDB (ms)",   justify="right", style="green")
    table.add_column("MySQL (ms)",    justify="right", style="yellow")
    table.add_column("Speedup",       justify="right", style="bold magenta")

    for qid, meta in BENCHMARK_QUERIES.items():
        console.print(f"\n[bold]▶ {qid}[/bold] — {meta['description']}")

        duck_df, duck_ms = run_query(duckdb_con, meta["sql"], "duckdb")
        mysql_df, mysql_ms = (
            run_query(mysql_engine, meta["sql"], "mysql")
            if mysql_engine
            else (None, -1.0)
        )

        if duck_ms > 0 and mysql_ms > 0:
            speedup = f"{mysql_ms / duck_ms:.1f}x"
        else:
            speedup = "N/A"

        table.add_row(
            qid,
            meta["description"],
            meta["focus"],
            f"{duck_ms:.1f}" if duck_ms >= 0 else "ERROR",
            f"{mysql_ms:.1f}" if mysql_ms >= 0 else "SKIP",
            speedup,
        )

        # 预览前 3 行结果（仅 DuckDB）
        if duck_df is not None and not duck_df.empty:
            console.print(duck_df.head(3).to_string(index=False))

        results.append({
            "session":      session_ts,
            "query_id":     qid,
            "description":  meta["description"],
            "focus":        meta["focus"],
            "duckdb_ms":    duck_ms,
            "mysql_ms":     mysql_ms,
            "speedup":      speedup,
        })

    console.print(table)
    return results, session_ts


# ============================================================
#  6. 保存 Benchmark 结果到 output/
# ============================================================
def save_results(results: list[dict], session_ts: str):
    # CSV（追加模式，方便多次跑结果合并分析）
    csv_path = OUTPUT_DIR / "benchmark_results.csv"
    write_header = not csv_path.exists()
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(results)

    # 本次完整 JSON 快照
    json_path = OUTPUT_DIR / f"benchmark_{session_ts}.json"
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2)

    console.print(f"\n[green][+] 结果已保存:[/green]")
    console.print(f"    CSV  (累计) → {csv_path}")
    console.print(f"    JSON (本次) → {json_path}")


# ============================================================
#  7. EXPLAIN ANALYZE 辅助 — Midterm Report 必用
# ============================================================
def explain_query(duckdb_con, mysql_engine, query_id: str = "Q1_50day_MA"):
    if query_id not in BENCHMARK_QUERIES:
        console.print(f"[red]未知 query_id: {query_id}[/red]")
        return

    sql = BENCHMARK_QUERIES[query_id]["sql"].strip()
    explain_path = OUTPUT_DIR / f"explain_{query_id}.txt"

    lines = [f"=== EXPLAIN ANALYZE: {query_id} ===\n", f"SQL:\n{sql}\n\n"]

    # DuckDB EXPLAIN
    try:
        duck_plan = duckdb_con.execute(f"EXPLAIN ANALYZE {sql}").fetchdf()
        lines.append("--- DuckDB EXPLAIN ANALYZE ---\n")
        lines.append(duck_plan.to_string(index=False))
        lines.append("\n\n")
    except Exception as e:
        lines.append(f"DuckDB EXPLAIN 失败: {e}\n")

    # MySQL EXPLAIN
    if mysql_engine:
        try:
            mysql_plan = pd.read_sql(f"EXPLAIN {sql}", mysql_engine)
            lines.append("--- MySQL EXPLAIN ---\n")
            lines.append(mysql_plan.to_string(index=False))
            lines.append("\n")
        except Exception as e:
            lines.append(f"MySQL EXPLAIN 失败: {e}\n")

    with open(explain_path, "w") as f:
        f.writelines(lines)

    console.print(f"[green][+] EXPLAIN 报告 → {explain_path}[/green]")


# ============================================================
#  Main
# ============================================================
def main():
    console.print(Panel(
        "[bold]Stock Analytics Dashboard[/bold]\n"
        "DuckDB Columnar + Vectorized  vs  MySQL Row-Based\n"
        f"Data dir:   {DATA_DIR}\n"
        f"Output dir: {OUTPUT_DIR}",
        title="DSCI 551 Project",
        expand=False,
    ))

    # Step 1: 获取数据
    df = fetch_stock_data(STOCK_SYMBOLS, START_DATE, END_DATE)

    # Step 2: 初始化两个数据库
    duckdb_con  = setup_duckdb(df)
    mysql_engine = setup_mysql(df)   # 若 MySQL 不可用则返回 None，程序继续运行

    # Step 3: 跑 Benchmark
    results, session_ts = run_benchmark(duckdb_con, mysql_engine)

    # Step 4: 保存结果
    save_results(results, session_ts)

    # Step 5: 生成 EXPLAIN ANALYZE 报告 (Midterm 必备)
    console.print("\n[cyan][*] 生成 EXPLAIN ANALYZE 报告...[/cyan]")
    for qid in ["Q1_50day_MA", "Q3_annual_summary", "Q4_full_scan"]:
        explain_query(duckdb_con, mysql_engine, qid)

    console.print("\n[bold green]✓ 全部完成![/bold green]")


if __name__ == "__main__":
    main()