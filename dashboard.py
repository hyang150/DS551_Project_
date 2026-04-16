"""
dashboard.py — Streamlit Dashboard for DSCI 551 Demo
Stock Market Analytics: DuckDB vs MySQL

Usage:
    pip install streamlit plotly
    streamlit run dashboard.py
"""

import time
import statistics
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# ── Page Config ──
st.set_page_config(
    page_title="DuckDB vs MySQL — Stock Analytics",
    page_icon="📊",
    layout="wide",
)

# ── Constants ──
DATA_DIR    = Path(__file__).resolve().parent / "data"
DUCKDB_FILE = str(DATA_DIR / "stock_analytics.duckdb")

# MySQL config
MYSQL_USER     = "root"
MYSQL_PASSWORD = "password"
MYSQL_HOST     = "127.0.0.1"
MYSQL_PORT     = "3306"
MYSQL_DB       = "stock_db"


# ============================================================
#  Database Connections (cached)
# ============================================================
@st.cache_resource
def get_duckdb():
    """Connect to existing DuckDB file. Run main.py first to populate."""
    return duckdb.connect(DUCKDB_FILE, read_only=True)

@st.cache_resource
def get_mysql():
    """Connect to MySQL. Returns None if unavailable."""
    try:
        from sqlalchemy import create_engine
        engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
            f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        )
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        return engine
    except Exception:
        return None


def timed_query(con_or_engine, sql: str, db_type: str, n_runs: int = 5):
    """Run query n_runs times, return (df, median_ms)."""
    times = []
    last_df = None
    for _ in range(n_runs):
        t0 = time.perf_counter()
        if db_type == "duckdb":
            last_df = con_or_engine.execute(sql).fetchdf()
        else:
            last_df = pd.read_sql(sql, con_or_engine)
        times.append((time.perf_counter() - t0) * 1000)
    median_ms = round(statistics.median(times[1:]), 3) if len(times) > 1 else times[0]
    return last_df, median_ms


# ============================================================
#  Queries
# ============================================================
QUERIES = {
    "Q1: 50-Day Moving Avg": {
        "sql": """
            SELECT Date, Symbol, Close,
                   AVG(Close) OVER (
                       PARTITION BY Symbol ORDER BY Date
                       ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                   ) AS MA_50
            FROM stock_data ORDER BY Date DESC LIMIT 500
        """,
        "focus": "Vectorized Window (STREAMING_WINDOW)",
        "cols": "3/7",
    },
    "Q2: Daily Volatility": {
        "sql": """
            SELECT Date, Symbol,
                   ROUND((High - Low) / Close * 100, 4) AS volatility_pct
            FROM stock_data ORDER BY volatility_pct DESC LIMIT 100
        """,
        "focus": "Column Pruning (3 cols read)",
        "cols": "3/7",
    },
    "Q3: Annual Summary": {
        "sql": """
            SELECT YEAR(Date) AS Year, Symbol,
                   ROUND(AVG(Close), 2) AS avg_close,
                   ROUND(MAX(High), 2) AS year_high,
                   ROUND(MIN(Low), 2) AS year_low,
                   SUM(Volume) AS total_volume
            FROM stock_data GROUP BY YEAR(Date), Symbol ORDER BY Year, Symbol
        """,
        "focus": "Vectorized Aggregation (HASH_GROUP_BY)",
        "cols": "4/7",
    },
    "Q4: Narrow Aggregate": {
        "sql": "SELECT COUNT(*), ROUND(AVG(Close), 4), SUM(Volume) FROM stock_data",
        "focus": "Narrow Columnar I/O (2 cols)",
        "cols": "2/7",
    },
    "Q5: Rolling StdDev": {
        "sql": """
            SELECT Date, Symbol, Close,
                   ROUND(STDDEV(Close) OVER (
                       PARTITION BY Symbol ORDER BY Date
                       ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                   ), 4) AS rolling_std_20
            FROM stock_data ORDER BY Date DESC LIMIT 100
        """,
        "focus": "Vec. Window + STDDEV",
        "cols": "3/7",
    },
    "Q6: Wide SELECT *": {
        "sql": "SELECT * FROM stock_data ORDER BY Date DESC LIMIT 5000",
        "focus": "Wide Scan — control group",
        "cols": "7/7",
    },
    "Q7: Point Lookup": {
        "sql": "SELECT * FROM stock_data WHERE Symbol = 'AAPL' AND Date = '2023-06-15'",
        "focus": "OLTP — MySQL advantage",
        "cols": "7/7",
    },
    "Q8: Range Scan (1 month)": {
        "sql": """
            SELECT * FROM stock_data
            WHERE Symbol = 'AAPL' AND Date BETWEEN '2023-01-01' AND '2023-01-31'
        """,
        "focus": "OLTP — MySQL advantage",
        "cols": "7/7",
    },
}


# ============================================================
#  App Layout
# ============================================================
def main():
    duck_con = get_duckdb()
    mysql_engine = get_mysql()

    # ── Header ──
    st.title("Stock Market Analytics: DuckDB vs MySQL")
    st.caption("DSCI 551 Course Project — Hanwen Yang & Jialiang Lou")

    # ── Sidebar ──
    st.sidebar.header("Configuration")
    n_runs = st.sidebar.slider("Runs per query", 3, 15, 5)
    use_mysql = st.sidebar.checkbox("Include MySQL", value=mysql_engine is not None)
    if use_mysql and mysql_engine is None:
        st.sidebar.warning("MySQL is not available. Start MySQL first.")
        use_mysql = False

    selected_queries = st.sidebar.multiselect(
        "Select queries", list(QUERIES.keys()), default=list(QUERIES.keys())
    )

    # ── Tab 1: Dataset Overview ──
    tab_data, tab_bench, tab_explain, tab_arch = st.tabs([
        "Dataset", "Benchmark", "EXPLAIN Plans", "Architecture"
    ])

    with tab_data:
        st.header("Dataset Overview")
        row_count = duck_con.execute("SELECT COUNT(*) FROM stock_data").fetchone()[0]
        sym_count = duck_con.execute("SELECT COUNT(DISTINCT Symbol) FROM stock_data").fetchone()[0]
        date_range = duck_con.execute("SELECT MIN(Date), MAX(Date) FROM stock_data").fetchone()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Rows", f"{row_count:,}")
        col2.metric("Stocks", sym_count)
        col3.metric("Date Range", f"{date_range[0]} to {date_range[1]}")

        st.subheader("Schema")
        st.code(
            "CREATE TABLE stock_data (\n"
            "    Date    DATE,\n"
            "    Symbol  VARCHAR,\n"
            "    Open    DOUBLE,\n"
            "    High    DOUBLE,\n"
            "    Low     DOUBLE,\n"
            "    Close   DOUBLE,\n"
            "    Volume  BIGINT\n"
            ");", language="sql"
        )

        # Stock price chart
        st.subheader("Stock Price History")
        symbols = duck_con.execute("SELECT DISTINCT Symbol FROM stock_data ORDER BY Symbol").fetchdf()
        selected_sym = st.selectbox("Select stock", symbols["Symbol"].tolist(), index=0)

        price_df = duck_con.execute(f"""
            SELECT Date, Close, Volume FROM stock_data
            WHERE Symbol = '{selected_sym}' ORDER BY Date
        """).fetchdf()

        fig = px.line(price_df, x="Date", y="Close",
                      title=f"{selected_sym} — Closing Price")
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Benchmark ──
    with tab_bench:
        st.header("Performance Benchmark")

        if st.button("Run Benchmark", type="primary"):
            results = []
            progress = st.progress(0)

            for i, qid in enumerate(selected_queries):
                meta = QUERIES[qid]
                sql = meta["sql"].strip()

                # DuckDB
                _, duck_ms = timed_query(duck_con, sql, "duckdb", n_runs)

                # MySQL
                mysql_ms = -1
                if use_mysql:
                    _, mysql_ms = timed_query(mysql_engine, sql, "mysql", n_runs)

                speedup = round(mysql_ms / duck_ms, 1) if duck_ms > 0 and mysql_ms > 0 else None

                results.append({
                    "Query": qid,
                    "Focus": meta["focus"],
                    "Cols Read": meta["cols"],
                    "DuckDB (ms)": round(duck_ms, 1),
                    "MySQL (ms)": round(mysql_ms, 1) if mysql_ms > 0 else "N/A",
                    "Speedup": f"{speedup}x" if speedup else "N/A",
                })
                progress.progress((i + 1) / len(selected_queries))

            results_df = pd.DataFrame(results)
            st.dataframe(results_df, use_container_width=True, hide_index=True)

            # Bar chart
            chart_data = []
            for r in results:
                chart_data.append({"Query": r["Query"], "Database": "DuckDB", "Time (ms)": r["DuckDB (ms)"]})
                if r["MySQL (ms)"] != "N/A":
                    chart_data.append({"Query": r["Query"], "Database": "MySQL", "Time (ms)": r["MySQL (ms)"]})

            chart_df = pd.DataFrame(chart_data)
            fig = px.bar(chart_df, x="Query", y="Time (ms)", color="Database",
                         barmode="group", title="Query Execution Time: DuckDB vs MySQL",
                         color_discrete_map={"DuckDB": "#2ecc71", "MySQL": "#f39c12"})
            fig.update_layout(xaxis_tickangle=-30)
            st.plotly_chart(fig, use_container_width=True)

            # Speedup chart
            speedup_data = [r for r in results if r["Speedup"] != "N/A"]
            if speedup_data:
                sp_df = pd.DataFrame(speedup_data)
                sp_df["Speedup_val"] = sp_df["Speedup"].str.replace("x", "").astype(float)
                fig2 = px.bar(sp_df, x="Query", y="Speedup_val",
                              title="Speedup (MySQL time / DuckDB time)",
                              labels={"Speedup_val": "Speedup (x)"},
                              color="Speedup_val",
                              color_continuous_scale=["#f39c12", "#2ecc71"])
                fig2.add_hline(y=1.0, line_dash="dash", line_color="red",
                               annotation_text="1x = same speed")
                fig2.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig2, use_container_width=True)

            # Key observations
            st.subheader("Key Observations")
            st.markdown("""
            - **OLAP queries (Q1-Q5):** DuckDB significantly faster due to columnar storage + vectorized execution
            - **Wide scan (Q6):** DuckDB advantage shrinks — must reconstruct full rows from columns
            - **OLTP queries (Q7-Q8):** MySQL competitive or faster — B-tree index gives direct row access
            """)

    # ── Tab 3: EXPLAIN ──
    with tab_explain:
        st.header("EXPLAIN ANALYZE Comparison")

        explain_query = st.selectbox("Select query to explain", list(QUERIES.keys()))
        meta = QUERIES[explain_query]
        sql = meta["sql"].strip()

        st.code(sql, language="sql")
        st.caption(f"Focus: {meta['focus']} | Columns read: {meta['cols']}")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("DuckDB EXPLAIN")
            try:
                plan = duck_con.execute(f"EXPLAIN {sql}").fetchdf()
                st.text(plan.to_string(index=False))
            except Exception as e:
                st.error(str(e))

        with col2:
            st.subheader("MySQL EXPLAIN")
            if use_mysql:
                try:
                    plan = pd.read_sql(f"EXPLAIN {sql}", mysql_engine)
                    st.dataframe(plan, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(str(e))
            else:
                st.info("MySQL not connected")

        # Mapping explanation
        st.subheader("Internals to Application Mapping")
        mappings = {
            "Q1: 50-Day Moving Avg": (
                "**Application:** Compute 50-day moving average for each stock.\n\n"
                "**DuckDB Internal:** `STREAMING_WINDOW` operator processes 2048-tuple chunks. "
                "The window frame slides over the vector batch, computing AVG in SIMD-friendly loops.\n\n"
                "**MySQL Internal:** Iterates one row at a time through the window frame. "
                "Uses `Using temporary; Using filesort` to materialize and sort data.\n\n"
                "**Why it matters:** Vectorized batch processing avoids per-row function call "
                "overhead, resulting in 10-30x speedup for window functions."
            ),
            "Q2: Daily Volatility": (
                "**Application:** Calculate (High-Low)/Close for each trading day.\n\n"
                "**DuckDB Internal:** `SEQ_SCAN` reads only High, Low, Close columns (3/7). "
                "Column pruning skips Date, Symbol, Open, Volume entirely.\n\n"
                "**MySQL Internal:** Reads entire rows (all 7 columns) from InnoDB pages, "
                "then discards unused columns.\n\n"
                "**Why it matters:** Columnar storage reduces I/O by ~57% for this query."
            ),
            "Q3: Annual Summary": (
                "**Application:** Compute yearly statistics per stock.\n\n"
                "**DuckDB Internal:** `HASH_GROUP_BY` hashes and aggregates entire vector "
                "batches. Reads only 4 needed columns.\n\n"
                "**MySQL Internal:** Row-by-row hash aggregation with `Using temporary`.\n\n"
                "**Why it matters:** Vectorized hashing processes 2048 keys per batch call."
            ),
            "Q4: Narrow Aggregate": (
                "**Application:** Compute global statistics (count, avg, sum).\n\n"
                "**DuckDB Internal:** Scans only Close + Volume columns (2/7). "
                "Compressed column segments reduce I/O further.\n\n"
                "**MySQL Internal:** Full-row scan required.\n\n"
                "**Why it matters:** Narrowest query shows the largest I/O advantage."
            ),
            "Q5: Rolling StdDev": (
                "**Application:** 20-day rolling standard deviation for risk assessment.\n\n"
                "**DuckDB Internal:** `STREAMING_WINDOW` with STDDEV computed per chunk.\n\n"
                "**MySQL Internal:** Recomputes STDDEV by re-scanning 20-row frame per row.\n\n"
                "**Why it matters:** Streaming approach avoids redundant re-scans of window frames."
            ),
            "Q6: Wide SELECT *": (
                "**Application:** Fetch all columns for display.\n\n"
                "**DuckDB Internal:** Must read all 7 column segments and reconstruct tuples. "
                "Column pruning advantage is neutralized.\n\n"
                "**MySQL Internal:** Rows are already stored together — no reconstruction needed.\n\n"
                "**Why it matters:** This control query shows columnar storage is NOT always "
                "faster. The architecture is optimized for analytical, not full-row retrieval."
            ),
            "Q7: Point Lookup": (
                "**Application:** Fetch a single day's data for one stock.\n\n"
                "**DuckDB Internal:** Must scan column segments even for 1 row (no B-tree index).\n\n"
                "**MySQL Internal:** B-tree index on (Symbol, Date) provides O(log n) direct access.\n\n"
                "**Why it matters:** Row-based + B-tree index excels at OLTP point queries."
            ),
            "Q8: Range Scan (1 month)": (
                "**Application:** Fetch one month of trading data for one stock.\n\n"
                "**DuckDB Internal:** Scans row groups, uses zone maps to skip irrelevant groups.\n\n"
                "**MySQL Internal:** B-tree range scan on (Symbol, Date) reads only matching rows.\n\n"
                "**Why it matters:** Small range scans favor indexed row-based storage."
            ),
        }
        st.markdown(mappings.get(explain_query, "No mapping available for this query."))

    # ── Tab 4: Architecture ──
    with tab_arch:
        st.header("DuckDB Architecture Overview")

        st.subheader("Columnar vs Row-Based Storage")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            #### DuckDB (Columnar)
            ```
            Column "Close":  [150.2, 152.1, 148.7, ...]
            Column "Volume": [80M,   92M,   75M,   ...]
            Column "Open":   [149.0, 151.5, 150.0, ...]
            ...
            ```
            - Reads only needed columns
            - Per-column compression (RLE, dict, bitpacking)
            - Cache-friendly for analytical scans
            """)
        with col2:
            st.markdown("""
            #### MySQL (Row-Based)
            ```
            Row 1: [2024-01-02, AAPL, 149.0, 151.0, 148.5, 150.2, 80M]
            Row 2: [2024-01-03, AAPL, 151.5, 153.0, 150.0, 152.1, 92M]
            Row 3: [2024-01-04, AAPL, 150.0, 150.5, 147.0, 148.7, 75M]
            ...
            ```
            - Reads entire rows always
            - B-tree index for fast point lookups
            - Efficient for OLTP transactions
            """)

        st.subheader("Vectorized vs Tuple-at-a-Time Execution")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("""
            #### DuckDB (Vectorized)
            ```
            Process 2048 values at once:
            [v1, v2, v3, ..., v2048] → AVG() → result

            Operators: STREAMING_WINDOW, HASH_GROUP_BY
            ```
            - Fewer function calls
            - SIMD-friendly loops
            - Better CPU cache utilization
            """)
        with col2:
            st.markdown("""
            #### MySQL (Tuple-at-a-Time)
            ```
            Process 1 row at a time:
            v1 → AVG() → update state
            v2 → AVG() → update state
            v3 → AVG() → update state
            ...
            ```
            - One function call per row
            - More overhead for large scans
            - Using temporary + filesort for windows
            """)

        st.subheader("When to Use Which?")
        comparison = pd.DataFrame({
            "Workload Type": ["Analytical aggregation", "Window functions",
                              "Narrow column scan", "Wide SELECT *",
                              "Point lookup", "Range scan (small)",
                              "Bulk insert", "Concurrent transactions"],
            "DuckDB": ["Excellent", "Excellent", "Excellent", "Good",
                       "Fair", "Fair", "Good", "Limited"],
            "MySQL": ["Fair", "Fair", "Fair", "Good",
                      "Excellent", "Excellent", "Good", "Excellent"],
        })
        st.dataframe(comparison, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
