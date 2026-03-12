# Stock Market Analytics Dashboard

**DSCI 551 Course Project — Inside DuckDB: Columnar Storage & Vectorized Query Execution**

DuckDB (Columnar + Vectorized) vs MySQL (Row-Based) 性能对比 Benchmark

Authors: Hanwen Yang / Jialiang Lou

---

## 项目结构

```
.
├── main.py               # CLI 入口，控制全流程
├── downloader.py          # Yahoo Finance 数据下载 & Parquet/CSV 缓存
├── db_run.py              # DuckDB/MySQL 初始化 + Benchmark 运行 + 结果保存
├── explain_analyzer.py    # EXPLAIN ANALYZE 执行计划对比报告
├── data/                  # (自动创建) 原始数据 & DuckDB 文件
│   ├── 5stocks_2015_2025.parquet
│   ├── 5stocks_2015_2025.csv
│   └── stock_analytics.duckdb
├── output/                # (自动创建) Benchmark 结果 & EXPLAIN 报告
│   ├── benchmark_results.csv
│   ├── benchmark_20260312_143000.json
│   └── explain_report.txt
└── README.md
```

---

## 环境要求

- Python 3.10+
- WSL (Ubuntu) — 推荐，MySQL 和 DuckDB 都在本地运行
- MySQL 8.0（可选，不装也能跑 DuckDB 单独测试）

---

## 安装依赖

### 方式一：uv（推荐）

```bash
uv init          # 如果还没有 pyproject.toml
uv add duckdb pandas yfinance pyarrow rich sqlalchemy pymysql
```

### 方式二：pip

```bash
pip install duckdb pandas yfinance pyarrow rich sqlalchemy pymysql
```

---

## MySQL 配置（可选）

如果要运行 DuckDB vs MySQL 对比，需要先启动 MySQL：

```bash
# WSL 内启动 MySQL
sudo service mysql start

# 确认能连上
mysql -u root -p -e "SELECT 1"
```

如果你的 MySQL 密码不是 `password`，修改 `db_run.py` 顶部：

```python
MYSQL_USER     = "root"
MYSQL_PASSWORD = "your_password"   # ← 改这里
MYSQL_HOST     = "127.0.0.1"
MYSQL_PORT     = "3306"
MYSQL_DB       = "stock_db"
```

> 不装 MySQL 也能跑！加 `--no-mysql` 即可，程序只测 DuckDB。

---

## 运行方式

### 1. 最简单 — 默认全流程

```bash
python main.py
```

这会依次执行：
1. 下载 5 只股票（AAPL, MSFT, GOOGL, AMZN, NVDA）10 年数据
2. 导入 DuckDB + MySQL
3. 运行 6 条 Benchmark 查询（每条 5 次，取中位数）
4. 生成 EXPLAIN ANALYZE 对比报告
5. 保存结果到 `output/`

### 2. 扩大数据量 — 30 只股票

```bash
python main.py --symbols extended --start 2010-01-01
```

30 只股票 × 15 年 ≈ 100,000+ 行，DuckDB 的优势会更明显。

### 3. 自定义股票列表

```bash
python main.py --symbols AAPL TSLA NVDA META --start 2020-01-01 --end 2025-01-01
```

### 4. 不装 MySQL，只测 DuckDB

```bash
python main.py --no-mysql
```

### 5. 增加测试次数（更稳定的数据）

```bash
python main.py --runs 10
```

每条查询跑 10 次，丢弃第 1 次 warm-up，取后面 9 次的中位数。

### 6. 只跑指定查询

```bash
python main.py --queries Q1_50day_MA Q4_full_scan_narrow Q6_wide_projection
```

可选查询 ID：
| Query ID | 说明 | 测试重点 |
|---|---|---|
| `Q1_50day_MA` | 50日均线 (Window) | Vectorized Execution |
| `Q2_daily_volatility` | 每日波动率 (3列) | Column Pruning |
| `Q3_annual_summary` | 年度汇总 (GROUP BY) | Vectorized Aggregation |
| `Q4_full_scan_narrow` | 全表聚合 (2列) | Columnar I/O |
| `Q5_rolling_stddev` | 20日滚动标准差 | Vectorized Window |
| `Q6_wide_projection` | SELECT * 全列 | 列存劣势对照实验 |

### 7. 只生成 EXPLAIN 报告（跳过 Benchmark）

```bash
python main.py --skip-benchmark --explain-only
```

### 8. 用缓存数据重新跑（不重新下载）

```bash
python main.py --skip-download --runs 7
```

---

## CLI 完整参数

```
python main.py --help
```

```
数据配置:
  --symbols       股票列表，或 'default'(5只) / 'extended'(30只)
  --start         起始日期 (默认 2015-01-01)
  --end           结束日期 (默认 2025-01-01)

Benchmark 配置:
  --runs          每条查询运行次数 (默认 5, 第1次warm-up丢弃)
  --queries       指定查询 ID 列表

流程控制:
  --skip-download   跳过下载，用缓存
  --skip-benchmark  跳过 benchmark
  --skip-explain    跳过 EXPLAIN 报告
  --explain-only    只跑 EXPLAIN
  --no-mysql        跳过 MySQL

路径:
  --data-dir      数据目录 (默认 ./data)
  --output-dir    输出目录 (默认 ./output)
```

---

## 输出文件说明

运行后 `output/` 目录下会生成：

| 文件 | 用途 |
|---|---|
| `benchmark_results.csv` | 累计 benchmark 结果（每次运行追加），可直接用来画图 |
| `benchmark_YYYYMMDD_HHMMSS.json` | 单次运行的完整 JSON 快照（含每次运行的详细耗时） |
| `explain_report.txt` | DuckDB vs MySQL EXPLAIN ANALYZE 对比报告，含 operator 注解 |

---

## Midterm Report 推荐流程

```bash
# Step 1: 大数据量跑一次完整 benchmark
python main.py --symbols extended --start 2010-01-01 --runs 7

# Step 2: 检查结果
cat output/benchmark_results.csv
cat output/explain_report.txt

# Step 3: 如果 MySQL 没装，先只跑 DuckDB 拿到 EXPLAIN
python main.py --symbols extended --start 2010-01-01 --no-mysql --runs 7
```

报告需要从 `output/` 中提取：
- `benchmark_results.csv` → 画 DuckDB vs MySQL 柱状图
- `explain_report.txt` → 截取关键 EXPLAIN 输出 + operator 注解贴进报告
- JSON 文件 → 每条查询的逐次耗时，用于分析方差

---

## 常见问题

**Q: MySQL 连接失败？**
```bash
sudo service mysql start          # 启动 MySQL
mysql -u root -p                  # 确认能登录
```
或者直接 `--no-mysql` 跳过。

**Q: yfinance 下载失败？**
检查网络连接。已下载的数据会缓存为 Parquet，下次自动跳过。删除 `data/` 下的 `.parquet` 文件可强制重新下载。

**Q: 数据量太小看不出差异？**
用 `--symbols extended --start 2000-01-01` 拉到 10 万行以上。