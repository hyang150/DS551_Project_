# Stock Market Analytics Dashboard

**DSCI 551 — DuckDB (columnar + vectorized) vs MySQL benchmark** on Yahoo Finance OHLCV data, with a documentary comparison against MongoDB in the final report.

**Authors:** Hanwen Yang / Jialiang Lou

---

## TA Quick Start

All datasets are pre-committed under `data/`. No internet, MySQL, or MongoDB needed.

```bash
uv sync                                                  # install deps
uv run streamlit run dashboard.py                        # interactive UI (recommended)
uv run python demo.py --skip-download --no-mysql         # 5-min scripted demo
```

For the full three-engine numbers, see [Optional MySQL setup](#optional-mysql-setup).

---

## Setup

### 1. Dependencies (uv)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh        # macOS / Linux / WSL
uv sync                                                # creates .venv/, installs everything
```

Run any command via `uv run python ...`. If you'd rather drop the prefix, activate once: `source .venv/bin/activate`.

### Optional MySQL setup

Ubuntu's MySQL `root` uses the `auth_socket` plugin and **rejects password logins** — setting `MYSQL_PASSWORD` in `.env` won't help (you'll get error 1698, not 1045). Create a dedicated user:

```bash
sudo apt-get install -y mysql-server && sudo service mysql start
sudo mysql <<'EOF'
CREATE USER 'dsci551'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'dsci551'@'localhost';
FLUSH PRIVILEGES;
EOF

cat > .env <<'EOF'
MYSQL_USER=dsci551
MYSQL_PASSWORD=password
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DB=stock_db
EOF
```

Verify: `mysql -u dsci551 -ppassword -e "SELECT VERSION();"`.

### Optional MongoDB

Not required for grading — the final report's MongoDB comparison is documentary (BSON, WiredTiger, aggregation pipeline). To run empirical Mongo numbers anyway, install `mongodb-org`, start `mongod`, then `uv run python mongo_run.py --compare-all`.

---

## Running

| Command | Use case |
|---|---|
| `uv run streamlit run dashboard.py` | Interactive UI — Dataset / Benchmark / EXPLAIN / Architecture tabs |
| `uv run python demo.py --skip-download --auto` | Scripted 5–10 min walkthrough (Zoom demo) |
| `uv run python main.py [--no-mysql] [--symbols extended] [--runs N]` | CLI benchmark, one dataset |
| `uv run python run_all_experiments.py --runs 7` | **Full experiment sweep — regenerates everything in `output/`** |

Append `--no-mysql` to any command to fall back to DuckDB-only.

---

## Outputs

| File in `output/` | Purpose |
|---|---|
| `benchmark_results.csv` | Per-query DuckDB & MySQL median ms + speedup (cumulative log) |
| `scaling_results.csv` | Speedup at small (~12K rows) vs large (~100K rows) |
| `explain_report.txt` | DuckDB EXPLAIN ANALYZE vs MySQL plans for all 9 queries |
| `profile_Q*.json` | DuckDB operator-level timing tree |
| `benchmark_{small,large}_*.json` | Per-run snapshots |

Schema and benchmark queries are embedded in `db_run.py` (`setup_duckdb`, `setup_mysql`, `BENCHMARK_QUERIES`) — no separate SQL files; running any entry point auto-creates tables and loads data.

---

## Headline Results (DuckDB speedup over MySQL)

| Query | ~12K rows | ~100K rows | What it shows |
|---|---:|---:|---|
| Q1 50-day MA (window) | 22.6× | **77.8×** | Vectorized window |
| Q2 daily volatility | 4.2× | 8.8× | Column pruning |
| Q3 annual GROUP BY | 3.4× | 15.3× | Vectorized aggregation |
| Q4 narrow full-scan | 6.1× | 28.0× | Columnar I/O |
| Q5 rolling stddev | 10.7× | 39.3× | Window + SIMD |
| Q6 SELECT * (wide) | 8.0× | 10.3× | Wide scan — gap narrows |
| Q7 point lookup | 1.3× | **0.91×** | MySQL B-tree wins (OLTP) |
| Q8 small range | 1.5× | **0.72×** | MySQL B-tree wins (OLTP) |
| Q9 single-stock filter | 7.6× | 7.2× | Selective scan |

Source: `output/scaling_results.csv`.

---

## Common Errors

| Symptom | Cause + Fix |
|---|---|
| `ModuleNotFoundError: No module named 'yfinance'` | You ran bare `python` instead of `uv run python` (uv venv vs system Python). Fix: prefix with `uv run` or `source .venv/bin/activate` first. |
| `(1698, "Access denied for user 'root'@'localhost'")` | Ubuntu MySQL `root` uses `auth_socket` — passwords won't work. Create the `dsci551` user (see [Optional MySQL setup](#optional-mysql-setup)). |
| `Unit mysql.service could not be found` | MySQL not installed: `sudo apt-get install -y mysql-server && sudo service mysql start`. |
| `DuckDB file not found` (dashboard) | Generate it: `uv run python main.py --skip-download --no-mysql`, or `cp data/stock_analytics_small.duckdb data/stock_analytics.duckdb`. |
| `yfinance` download fails | You don't need it — `data/` is pre-packaged. To force fresh fetch: `rm data/*.parquet && uv run python main.py`. |

---

## Secret Keys & Credentials

No external API keys are required. The only secret-shaped values are local DB credentials read from `.env`:

| Variable | Default if unset |
|---|---|
| `MYSQL_USER` / `MYSQL_PASSWORD` | `root` / `password` |
| `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_DB` | `127.0.0.1` / `3306` / `stock_db` |
| `MONGO_URI` / `MONGO_DB` | `mongodb://127.0.0.1:27017` / `stock_db` |

**Never commit `.env`** — it is git-ignored. See `.env.example` for the template.

---

## Demo

Live demo speaking script: [`DEMO_TALKING_SCRIPT.md`](DEMO_TALKING_SCRIPT.md).
