# Stock Market Analytics Dashboard

**DSCI 551 Course Project — Inside DuckDB: Columnar Storage & Vectorized Query Execution**

DuckDB (Columnar + Vectorized) vs MySQL (Row-Based) — a head-to-head benchmark
on real Yahoo Finance OHLCV data, with a written architectural comparison
against MongoDB in the final report.

**Authors:** Hanwen Yang / Jialiang Lou

---

## TA Quick Start (3 commands, no MySQL/Mongo required)

All required datasets are committed under `data/`. The dashboard and demo run
in DuckDB-only mode by default, so no database server is needed for grading.

```bash
# 1. Install dependencies
uv sync                              # or: pip install -r requirements.txt

# 2. Launch the interactive dashboard (recommended for grading)
streamlit run dashboard.py

# 3. Or run the scripted live demo (5–10 min walkthrough)
python demo.py --skip-download --no-mysql
```

> If you also want to reproduce the MySQL comparison numbers, jump to
> [Full Setup](#full-setup-mysql-optional) below — it takes ~3 extra minutes.

---

## Repository Layout

```
.
├── main.py                  # CLI entry — DuckDB vs MySQL benchmark
├── demo.py                  # Live demo script (5–10 min, with pauses)
├── dashboard.py             # Streamlit interactive dashboard
├── run_all_experiments.py   # One-shot runner for every experiment
│
├── downloader.py            # Yahoo Finance fetch + Parquet/CSV cache
├── db_run.py                # DuckDB / MySQL setup + benchmark loop
├── mongo_run.py             # Optional MongoDB benchmark (qualitative comparison only in report)
├── explain_analyzer.py      # EXPLAIN ANALYZE comparison report generator
│
├── data/                    # Pre-packaged datasets (committed to git)
│   ├── 5stocks_2015_2025.parquet     # ~550 KB — primary demo data
│   ├── 5stocks_2015_2025.csv         # MySQL LOAD DATA source
│   ├── 30stocks_2010_2025.parquet    # ~5 MB — large dataset (scaling exp.)
│   ├── 30stocks_2010_2025.csv
│   └── stock_analytics*.duckdb       # Pre-baked DuckDB files
│
├── output/                  # Benchmark results & EXPLAIN reports
│   ├── benchmark_results.csv         # Cumulative benchmark log
│   ├── benchmark_small_*.json        # Small-dataset run snapshot
│   ├── benchmark_large_*.json        # Large-dataset run snapshot
│   ├── scaling_results.csv           # Speedup vs row count
│   ├── explain_report.txt            # DuckDB vs MySQL EXPLAIN ANALYZE
│   └── profile_Q*.json               # DuckDB operator-level profiling
│
├── DEMO_TALKING_SCRIPT.md   # 5-min live demo speaking script
├── .env.example             # Template for MySQL / MongoDB credentials
├── pyproject.toml           # uv / hatch project config
├── requirements.txt         # pip-friendly dependency list
└── README.md
```

> Note: schema definitions and benchmark queries are embedded in `db_run.py`
> (function `setup_duckdb`, `setup_mysql`, and `BENCHMARK_QUERIES`). There is
> no separate `schema/` directory — running the Python entry points
> automatically creates tables and loads data.

---

## Environment Requirements

- Python 3.11+
- WSL Ubuntu 22.04 (recommended) or any Linux/macOS shell
- MySQL 8.0 (optional — only required for the cross-engine comparison)
- MongoDB 6.0+ (optional — only required if you want empirical Mongo numbers; the
  final report comparison with MongoDB is documentary, not benchmark-driven)

---

## Install Dependencies

This project uses [`uv`](https://docs.astral.sh/uv/) as its primary
dependency / virtualenv manager. `uv` reads `pyproject.toml` + `uv.lock` and
creates a project-local `.venv/` automatically — no manual `python -m venv`
needed.

### Option A — uv (recommended)

```bash
# 1. install uv (one-time, picks up on PATH after a new shell)
curl -LsSf https://astral.sh/uv/install.sh | sh        # macOS / Linux / WSL
# (Windows PowerShell:  irm https://astral.sh/uv/install.ps1 | iex)

# 2. install the project's deps into a local .venv/
cd DS551_Project_
uv sync

# 3. run any command inside the project's venv
uv run python main.py
uv run python run_all_experiments.py --runs 7
uv run streamlit run dashboard.py
```

> If you prefer not to type `uv run` every time, activate the venv once:
> ```bash
> source .venv/bin/activate            # bash / zsh
> # then: python main.py, streamlit run dashboard.py, etc.
> ```

### Option B — pip

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Option C — explicit pip

```bash
pip install duckdb pandas yfinance pyarrow rich sqlalchemy pymysql \
            streamlit plotly pymongo python-dotenv
```

> **Heads-up:** if you mix `uv run` and bare `python` in the same shell you
> will hit `ModuleNotFoundError`. `uv run` uses the project's `.venv`; bare
> `python` uses your system interpreter. Pick one — see
> [Common Errors](#common-errors) below.

---

## Configuration

Default values (matched by the code) work out of the box for the DuckDB-only
flow. To enable MySQL or MongoDB, copy the template and edit:

```bash
cp .env.example .env
# edit .env to fill in your local credentials
```

The application loads `.env` automatically via `python-dotenv`. **Without an
`.env` file the DuckDB-only path still works** — pass `--no-mysql` to any
script that takes it.

---

## Full Setup (MySQL, optional)

The MySQL comparison adds the row-store baseline (Q7/Q8 OLTP advantage and
the column-pruning loss in Q4/Q6). Skip if you only need the DuckDB demo.

### 1. Install and start MySQL on Ubuntu/WSL

```bash
sudo apt-get update
sudo apt-get install -y mysql-server
sudo service mysql start
```

### 2. Create a dedicated benchmark user

By default, Ubuntu's MySQL `root` account uses the `auth_socket` plugin and
**does not accept password logins**. Create a regular user instead:

```bash
sudo mysql <<'EOF'
CREATE USER 'dsci551'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'dsci551'@'localhost';
FLUSH PRIVILEGES;
EOF
```

### 3. Wire up `.env`

```bash
cat > .env <<'EOF'
MYSQL_USER=dsci551
MYSQL_PASSWORD=password
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DB=stock_db
EOF
```

Sanity-check the connection:

```bash
mysql -u dsci551 -ppassword -e "SELECT VERSION();"
```

### 4. Run the benchmark

```bash
python main.py             # default 5 stocks
# or for the full sweep used in the report:
python run_all_experiments.py --runs 7
```

The Python code creates the `stock_db` database, loads the CSV, and runs
the benchmarks automatically — no manual schema step required.

---

## MongoDB (optional — final report uses qualitative comparison)

Per the project guidelines, the final report includes a written
**Comparison with MySQL and MongoDB** section. The MongoDB part is
based on documented internals (BSON storage, WiredTiger, B-tree indexing,
aggregation pipeline) rather than a benchmark, so installing MongoDB is
**not required** to reproduce the report. If you do want empirical
Mongo numbers:

```bash
# install MongoDB Community Edition on Ubuntu 22.04
sudo apt-get install -y gnupg curl
curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc \
  | sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" \
  | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo service mongod start

# then run:
python mongo_run.py --compare-all
```

---

## Running the Application

### 1. Streamlit Dashboard (interactive, recommended for grading)

```bash
streamlit run dashboard.py
```

Open the printed URL in your browser. The sidebar lets you pick queries and
the number of runs; click **Run Benchmark**. Four tabs are available:
**Dataset / Benchmark / EXPLAIN Plans / Architecture**.

### 2. Scripted Live Demo (5–10 min, Zoom-friendly)

```bash
python demo.py --skip-download                 # use cached data, paced demo
python demo.py --skip-download --auto          # no pauses, run end-to-end
python demo.py --skip-download --no-mysql      # DuckDB-only path
```

### 3. CLI Benchmark (development / batch)

```bash
# default: 5 stocks, full pipeline
python main.py

# large dataset: 30 stocks × 15 years (~100K rows)
python main.py --symbols extended --start 2010-01-01

# custom symbols
python main.py --symbols AAPL TSLA NVDA META --start 2020-01-01

# DuckDB-only (no MySQL)
python main.py --no-mysql

# more runs for tighter median timings
python main.py --runs 10
```

### 4. One-shot full experiment sweep (regenerates every report figure)

```bash
python run_all_experiments.py --runs 7
```

Produces:

- `output/benchmark_small_*.json` (5 stocks × 10y)
- `output/benchmark_large_*.json` (30 stocks × 15y)
- `output/scaling_results.csv` (speedup vs row count)
- `output/profile_Q*.json` (DuckDB operator-level profiling)
- `output/explain_report.txt` (DuckDB vs MySQL EXPLAIN ANALYZE)

### 5. Run a subset of queries

```bash
python main.py --queries Q1_50day_MA Q4_full_scan_narrow Q6_wide_projection
```

Available query IDs:

| Query ID | Description | Internals Tested |
|---|---|---|
| `Q1_50day_MA` | 50-day moving average (window) | Vectorized execution |
| `Q2_daily_volatility` | Per-day volatility (3 cols) | Column pruning |
| `Q3_annual_summary` | Annual GROUP BY summary | Vectorized aggregation |
| `Q4_full_scan_narrow` | Full-table aggregate (2 cols) | Columnar I/O — narrow scan |
| `Q5_rolling_stddev` | 20-day rolling stddev | Vectorized window + STDDEV |
| `Q6_wide_projection` | `SELECT *` (all cols) | Columnar wide-scan disadvantage |
| `Q7_point_lookup` | Single-row lookup | OLTP (MySQL B-tree wins) |
| `Q8_small_range` | 1-month range scan | OLTP (MySQL B-tree wins) |
| `Q9_single_stock_filter` | Single-stock full history | OLTP — selective filter |

### 6. MongoDB cross-engine comparison (optional)

```bash
python mongo_run.py                  # load data + 3 equivalent queries
python mongo_run.py --compare-all    # side-by-side with DuckDB / MySQL
```

---

## Output File Reference

After running, files in `output/` include:

| File | Purpose |
|---|---|
| `benchmark_results.csv` | Cumulative benchmark log (appended each run) |
| `benchmark_*_YYYYMMDD_HHMMSS.json` | Per-run snapshot incl. all timings |
| `explain_report.txt` | DuckDB vs MySQL EXPLAIN ANALYZE + operator notes |
| `profile_Q*.json` | DuckDB operator-level profiling (JSON format) |
| `scaling_results.csv` | Speedup at small (~12K rows) vs large (~100K rows) |

---

## Headline Results (from `output/scaling_results.csv`)

| Query | Small (~12K rows) | Large (~100K rows) | Trend |
|---|---:|---:|---|
| Q1 50-day MA (window) | **22.6×** | **77.8×** | ↑ Vectorized window scales |
| Q2 daily volatility (3 cols) | 4.2× | 8.8× | ↑ Column pruning |
| Q3 annual GROUP BY | 3.4× | 15.3× | ↑ Vectorized aggregation |
| Q4 narrow full-scan | 6.1× | 28.0× | ↑ Columnar I/O |
| Q5 rolling stddev | 10.7× | 39.3× | ↑ Window + SIMD |
| Q6 SELECT * (wide) | 8.0× | 10.3× | DuckDB still wins, gap narrows |
| Q7 point lookup | 1.3× | **0.91×** | ↓ MySQL B-tree wins (OLTP) |
| Q8 small range | 1.5× | **0.72×** | ↓ MySQL B-tree wins (OLTP) |
| Q9 single-stock filter | 7.6× | 7.2× | ↑ DuckDB (selective scan) |

(Numbers are DuckDB-relative speedup over MySQL on the same query.)

---

## Secret Keys & Credentials

This project does **not** ship any credentials. The only secret-shaped values
are the local MySQL / MongoDB passwords supplied by the operator via `.env`:

| Variable | Where it is read | Default if unset |
|---|---|---|
| `MYSQL_USER` | `db_run.py`, `dashboard.py`, `demo.py` | `root` |
| `MYSQL_PASSWORD` | same | `password` |
| `MYSQL_HOST` | same | `127.0.0.1` |
| `MYSQL_PORT` | same | `3306` |
| `MYSQL_DB` | same | `stock_db` |
| `MONGO_URI` | `mongo_run.py` | `mongodb://127.0.0.1:27017` |
| `MONGO_DB` | `mongo_run.py` | `stock_db` |

If your local MySQL uses different credentials, copy `.env.example` to `.env`
and override. **Never commit your `.env` file** — it is git-ignored.

No external API keys are required (Yahoo Finance via `yfinance` is
unauthenticated, and the cached datasets in `data/` mean no network call is
needed for grading).

---

## Dataset Notes

- **Primary demo data:** `data/5stocks_2015_2025.parquet` (5 large-cap stocks,
  ~12K rows). Real OHLCV history pulled from Yahoo Finance.
- **Large dataset:** `data/30stocks_2010_2025.parquet` (~100K rows) used for
  the scaling experiment.
- **DuckDB pre-baked file:** `data/stock_analytics.duckdb` allows the
  dashboard / demo to run without re-loading data.
- The TA does not need internet access — every required dataset is committed
  under `data/`. To regenerate from scratch, delete `data/*.parquet` and run
  `python main.py` (this will fetch fresh data from Yahoo Finance).

---

## Running Without MySQL or MongoDB

You do **not** need MySQL or MongoDB installed to grade or run the project.
DuckDB is the only required engine — it ships as a pure Python wheel
(`pip install duckdb`) with no separate server.

| Engine | Installed? | What still works |
|---|---|---|
| **DuckDB only** | ✅ | Dashboard, demo, all 9 benchmark queries (DuckDB column), EXPLAIN report (DuckDB plans), profile JSONs, all final-report visuals except the cross-engine speedup table |
| **DuckDB + MySQL** | ✅ | Everything above **plus** the MySQL row-store baseline, `output/scaling_results.csv`, OLTP comparison (Q7/Q8) |
| **DuckDB + MySQL + MongoDB** | ✅ | Adds the optional `mongo_run.py --compare-all` empirical numbers (the report's MongoDB section is qualitative either way) |

When MySQL is missing, every entry point accepts `--no-mysql`:
```bash
python main.py --no-mysql
python demo.py --skip-download --no-mysql
streamlit run dashboard.py        # auto-detects and falls back to DuckDB-only
```

When MongoDB is missing: just don't run `mongo_run.py`. Nothing else depends on it.

---

## Common Errors (and what we hit while building this)

### 1. `ModuleNotFoundError: No module named 'yfinance'`

**Cause:** Mixing `uv run python ...` (uses `.venv/`) with bare `python ...`
(uses your system interpreter, which has no project deps). The traceback
points to `downloader.py` line 7 because that is the first import.

**Fix:** pick one path and stick to it.
```bash
# Either: prefix every command with uv run
uv run python run_all_experiments.py --runs 7

# Or: activate the venv once per shell
source .venv/bin/activate
python run_all_experiments.py --runs 7
```

### 2. `uv add yfinance` says "package not found"

You probably typed `yfinace` (missing the `n`). The package is
`yfinance`. Also note: it is already declared in `pyproject.toml`, so
`uv sync` is sufficient — you don't need `uv add` at all.

### 3. MySQL `(1698, "Access denied for user 'root'@'localhost'")`

**Cause:** On Ubuntu/WSL, the default `root` MySQL account uses the
`auth_socket` plugin — it ignores any password and requires you to log in
via `sudo mysql`. Setting `MYSQL_PASSWORD` in `.env` does **not** help;
error 1698 is "this user is not allowed to use a password," not "wrong
password" (that would be error 1045).

**Fix:** create a separate password-based user (one-time, requires sudo):
```bash
sudo mysql <<'EOF'
CREATE USER 'dsci551'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'dsci551'@'localhost';
FLUSH PRIVILEGES;
EOF
```
then point `.env` at it:
```bash
cat > .env <<'EOF'
MYSQL_USER=dsci551
MYSQL_PASSWORD=password
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DB=stock_db
EOF
```
Verify:
```bash
mysql -u dsci551 -ppassword -e "SELECT VERSION();"
```

### 4. `Unit mysql.service could not be found`

MySQL is not installed on this WSL distro yet:
```bash
sudo apt-get update && sudo apt-get install -y mysql-server
sudo service mysql start
```
(Note: WSL's MySQL listens on `127.0.0.1:3306` by default. WSL2 cannot
reach MySQL running on the Windows host via `127.0.0.1` because WSL2 is in
its own network namespace — install MySQL inside WSL itself.)

### 5. Dashboard shows `DuckDB file not found`

The dashboard expects `data/stock_analytics.duckdb`. The committed copy is
already in the repo; if it's missing locally, regenerate it with:
```bash
python main.py --skip-download --no-mysql
```

### 6. `yfinance` download fails / "no internet"

You don't need internet. Pre-packaged Parquet/CSV files live under `data/`.
Scripts auto-detect the cache. To force a fresh fetch, delete
`data/*.parquet` and re-run `python main.py`.

### 7. Need a larger workload than 100K rows

```bash
python main.py --symbols extended --start 2000-01-01     # ~150K+ rows
```

### 8. `data/` or `output/` directory missing

`data/` is committed (canonical 5-stock + 30-stock files). `output/` is
regenerated on demand:
```bash
python run_all_experiments.py --runs 7
```

---

## Demo Quick Reference

The 5-minute live demo speaking script is in
[`DEMO_TALKING_SCRIPT.md`](DEMO_TALKING_SCRIPT.md). It walks through the
Q1–Q9 internals → application mapping using the dashboard and the EXPLAIN
report.
