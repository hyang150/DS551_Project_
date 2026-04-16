# Stock Market Analytics Dashboard

**DSCI 551 Course Project — Inside DuckDB: Columnar Storage & Vectorized Query Execution**

DuckDB (Columnar + Vectorized) vs MySQL (Row-Based) vs MongoDB (Document) Benchmark

Authors: Hanwen Yang / Jialiang Lou

---

## 🚀 TA Quick Start (3 commands)

**All required data files are already committed under `data/`. No internet needed.**

```bash
# 1. Install dependencies
pip install -r requirements.txt   # or: uv sync

# 2. Launch interactive dashboard (recommended for grading)
streamlit run dashboard.py

# 3. Or run the scripted live demo (5–10 min walkthrough)
python demo.py --skip-download --no-mysql
```

> MySQL / MongoDB are **optional**. Without them the demo automatically falls
> back to DuckDB-only mode using the pre-packaged `data/stock_analytics.duckdb`.
>
> For the full three-engine comparison see the [Full Setup](#full-setup-optional)
> section below.

---

## 项目结构

```
.
├── main.py                  # CLI 入口 — DuckDB vs MySQL benchmark
├── demo.py                  # 现场 Demo 脚本 (5-10 min 带 pause)
├── dashboard.py             # Streamlit 交互式 Dashboard
├── run_all_experiments.py   # 一键跑全部实验 (small + large + profiling)
│
├── downloader.py            # Yahoo Finance 数据下载 & Parquet/CSV 缓存
├── db_run.py                # DuckDB / MySQL 初始化 + Benchmark 运行
├── mongo_run.py             # MongoDB 初始化 + Benchmark (final report 对比)
├── explain_analyzer.py      # EXPLAIN ANALYZE 执行计划对比报告
│
├── schema/                  # 独立 Schema 脚本 (TA 可手动执行)
│   ├── duckdb_schema.sql
│   ├── mysql_schema.sql
│   └── mongodb_schema.js
│
├── data/                    # ★ 预打包数据集 (入 git, 无需下载)
│   ├── 5stocks_2015_2025.parquet      (~500KB, demo 主数据)
│   ├── 5stocks_2015_2025.csv          (MySQL LOAD DATA 用)
│   └── stock_analytics.duckdb         (预烤 DuckDB 文件, demo 兜底)
│
├── output/                  # Benchmark 结果 & EXPLAIN 报告
│   ├── benchmark_results.csv
│   ├── benchmark_YYYYMMDD_HHMMSS.json
│   └── explain_report.txt
│
├── docs/
│   ├── DEMO_SCRIPT.md         # Demo 演讲稿 + 时间轴
│   └── MAPPING_CHEATSHEET.md  # Q1-Q9 internals→app 映射卡
│
├── .env.example             # MySQL/MongoDB 密码配置模板
└── README.md
```

---

## 环境要求

- Python 3.10+
- WSL (Ubuntu) 推荐 (MySQL / DuckDB / MongoDB 都在本地跑)
- MySQL 8.0（可选）
- MongoDB 6.0+（可选，final report 对比用）

---

## 安装依赖

### 方式一：uv（推荐）

```bash
uv sync
```

### 方式二：pip

```bash
pip install duckdb pandas yfinance pyarrow rich sqlalchemy pymysql \
            streamlit plotly pymongo python-dotenv
```

---

## 配置（可选）

默认 MySQL 密码 `password`、主机 `127.0.0.1:3306`。如果你的环境不同：

```bash
cp .env.example .env
# 编辑 .env 填入你的 MySQL/MongoDB 密码
```

程序会自动读取 `.env`。**没有 .env 文件也能跑** —— 直接加 `--no-mysql`。

---

## Full Setup (optional)

### MySQL

```bash
# WSL 内启动 MySQL
sudo service mysql start

# 确认能连上
mysql -u root -p -e "SELECT 1"
```

### MongoDB

```bash
# 启动 MongoDB (Ubuntu)
sudo service mongod start
mongosh --eval "db.runCommand({ ping: 1 })"
```

---

## 运行方式

### 1. Streamlit Dashboard (交互式，推荐 demo)

```bash
streamlit run dashboard.py
```

浏览器打开后，侧边栏选择查询和 runs 数，点 "Run Benchmark"。
包含 4 个标签页：Dataset / Benchmark / EXPLAIN Plans / Architecture。

### 2. 脚本式 Demo (5–10 min, 适合 Zoom 演示)

```bash
python demo.py --skip-download          # 使用缓存数据
python demo.py --skip-download --auto   # 无 pause, 一口气跑完
python demo.py --skip-download --no-mysql  # 没装 MySQL 也能跑
```

### 3. CLI Benchmark (开发 / 批处理)

```bash
# 默认 5 只股票全流程
python main.py

# 大数据集 — 30 只股票 × 15 年 (~100K 行)
python main.py --symbols extended --start 2010-01-01

# 自定义股票
python main.py --symbols AAPL TSLA NVDA META --start 2020-01-01

# 不装 MySQL，只测 DuckDB
python main.py --no-mysql

# 增加测试次数（更稳定的数据）
python main.py --runs 10
```

### 4. 一键跑完所有实验（生成 final report 所需的全部数据）

```bash
python run_all_experiments.py --runs 7
```

这会产出：
- `output/benchmark_small.csv` (5 stocks × 10y)
- `output/benchmark_large.csv` (30 stocks × 15y)
- `output/scaling_results.csv` (speedup vs rows)
- `output/profile_Q*.json` (DuckDB operator-level profiling)
- `output/explain_report.txt` (DuckDB vs MySQL EXPLAIN 对比)

### 5. 只跑指定查询

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
| `Q6_wide_projection` | SELECT * 全列 | 列存劣势对照 |
| `Q7_point_lookup` | 单行查找 | OLTP (MySQL 优势) |
| `Q8_small_range` | 1 个月范围扫描 | OLTP (MySQL 优势) |
| `Q9_single_stock_filter` | 单股全历史过滤 | OLTP (MySQL 优势) |

### 6. MongoDB 对比（final report 用）

```bash
python mongo_run.py                 # 加载 + 3 条等价查询
python mongo_run.py --compare-all   # 与 DuckDB / MySQL 横向对比
```

---

## 手动执行 Schema（可选）

如果你想单独初始化数据库而不走 Python：

```bash
# DuckDB
duckdb data/stock_analytics.duckdb < schema/duckdb_schema.sql

# MySQL
mysql -u root -p < schema/mysql_schema.sql

# MongoDB
mongosh stock_db < schema/mongodb_schema.js
```

---

## 输出文件说明

运行后 `output/` 目录下会生成：

| 文件 | 用途 |
|---|---|
| `benchmark_results.csv` | 累计 benchmark 结果（每次运行追加） |
| `benchmark_YYYYMMDD_HHMMSS.json` | 单次运行 JSON 快照（含每次耗时） |
| `explain_report.txt` | DuckDB vs MySQL EXPLAIN ANALYZE 对比 + operator 注解 |
| `profile_Q*.json` | DuckDB 操作符级 profiling（JSON format） |
| `scaling_results.csv` | 不同数据规模下的 speedup 对比 |

---

## 常见问题

**Q: yfinance 下载失败？**
不需要下载 —— `data/` 下已打包好数据。程序会自动用缓存。
若要强制下载：删除 `data/*.parquet` 后运行 `python main.py`。

**Q: MySQL 连不上？**
加 `--no-mysql` 跳过。Dashboard / Demo 自动回退到 DuckDB-only 模式。

**Q: 想测更大数据量？**
`python main.py --symbols extended --start 2000-01-01` 拉到 10 万行以上。

**Q: Dashboard 启动报错 "DuckDB file not found"？**
先跑 `python main.py --skip-download --no-mysql` 生成 `data/stock_analytics.duckdb`。
或使用已打包的版本（默认）。

---

## Demo 速查

现场演示流程详见 [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md)。
Q1-Q9 internals→app 映射速查见 [`docs/MAPPING_CHEATSHEET.md`](docs/MAPPING_CHEATSHEET.md)。
