# Demo Talking Script (5-10 min)

## Before Demo
- Terminal: `cd ~/DS551_Project/DS551_Project_`
- Zoom: share terminal screen
- Command: `python demo.py` (or `python demo.py --no-mysql` if MySQL not available)

---

## Step 0: Project Introduction (~1 min)

> Hi, we are Hanwen and Jialiang. Our project is Stock Market Analytics with DuckDB.
>
> DuckDB is an embedded OLAP database with two key design choices:
> **columnar storage** and **vectorized query execution**.
>
> We built a stock analytics application and compared DuckDB with MySQL
> to show how these internal design choices affect real query performance.
>
> Our dataset is daily OHLCV data from Yahoo Finance — 5 stocks over 10 years,
> about 12,000 rows, with 7 columns per row.

**[Press Enter]**

---

## Step 1: Data & Schema (~30 sec)

> Here you can see our dataset loaded into both DuckDB and MySQL.
> The schema has 7 columns: Date, Symbol, Open, High, Low, Close, Volume.
> DuckDB runs embedded in-process — no separate server needed.
> MySQL uses InnoDB with a composite index on (Symbol, Date).

**[Press Enter]**

---

## Step 2: Columnar Storage (~2 min)

> This is the first key internal mechanism: **columnar storage**.
>
> DuckDB stores each column separately. So when we only need Close and Volume
> for an aggregation, it reads just those 2 columns out of 7.
> MySQL stores data row-by-row, so it must read all 7 columns regardless.
>
> **Query A** is a narrow scan — only reads 2 columns.
> **Query B** is a wide scan (SELECT *) — reads all 7 columns.
>
> *(point to the results table)*
>
> You can see the narrow scan has a much larger speedup,
> because DuckDB skips 5 columns entirely.
> On the wide scan, the advantage shrinks because DuckDB must
> reconstruct rows from separate columns.
>
> This demonstrates that columnar storage is most effective when
> queries touch a small subset of columns — which is typical for
> analytical workloads like computing moving averages or volatility.

**[Press Enter]**

---

## Step 3: Vectorized Execution (~2 min)

> The second key mechanism is **vectorized execution**.
>
> DuckDB processes data in batches of 2048 tuples. Each operator like
> STREAMING_WINDOW or HASH_GROUP_BY works on an entire vector at once.
> MySQL processes one tuple at a time.
>
> We test this with three queries:
> 1. **50-day Moving Average** — uses STREAMING_WINDOW operator
> 2. **Rolling Standard Deviation** — also STREAMING_WINDOW
> 3. **Annual Summary with GROUP BY** — uses HASH_GROUP_BY
>
> *(point to results)*
>
> Window functions show the largest speedup — up to 20-30x faster.
> This is because DuckDB's streaming window operator processes entire chunks
> without materializing intermediate results, while MySQL uses temporary
> tables and filesort.
>
> The GROUP BY aggregation also benefits from vectorized hashing,
> though the speedup is smaller because hash-table access is less
> cache-friendly than sequential scan.

**[Press Enter]**

---

## Step 4: OLTP — MySQL Advantage (~1 min)

> To be fair, we also test scenarios where MySQL is competitive.
>
> **Point lookup**: fetch a single row by Symbol + Date.
> **Small range scan**: one month of data for one stock.
>
> *(point to results)*
>
> MySQL can use its B-tree index on (Symbol, Date) to directly
> locate the row without scanning any other data.
> DuckDB must still scan column segments even for a single row.
>
> This shows that each architecture has its strengths:
> **columnar for analytics, row-based for transactional lookups.**

**[Press Enter]**

---

## Step 5: Full Benchmark (~1 min)

> Here is the complete benchmark summary across all 8 queries.
>
> *(point to the table)*
>
> - OLAP queries (Q1-Q5): DuckDB is 5-30x faster
> - Wide scan (Q6): Advantage shrinks
> - OLTP queries (Q7-Q8): MySQL is competitive or faster
>
> This validates our thesis: DuckDB's columnar storage and vectorized
> execution are designed for analytical workloads, and the performance
> difference directly maps to the internal architecture differences.

**[Press Enter]**

---

## Step 6: EXPLAIN ANALYZE (~1 min)

> Finally, let's look at the execution plans to confirm what's happening internally.
>
> *(point to DuckDB EXPLAIN)*
>
> DuckDB shows SEQ_SCAN with only the accessed columns listed —
> this confirms column pruning is happening.
> For window queries, we see STREAMING_WINDOW operator.
> For GROUP BY, we see HASH_GROUP_BY.
>
> *(point to MySQL EXPLAIN)*
>
> MySQL shows "Using temporary" and "Using filesort" for window queries,
> confirming it needs extra materialization steps.
>
> This EXPLAIN output directly maps to the performance differences
> we observed in the benchmark.

**[Press Enter]**

---

## Closing (~30 sec)

> To summarize:
> 1. **Columnar storage** reduces I/O by reading only needed columns
> 2. **Vectorized execution** processes 2048 tuples per batch for better CPU efficiency
> 3. These advantages are specific to **OLAP workloads** —
>    row-based storage is still better for point lookups
>
> Thank you! Any questions?

---

## Q&A Preparation

Possible questions and answers:

**Q: Why only 12,000 rows? Would results change at larger scale?**
> At small scale, timing noise is significant. We use median-of-N runs to mitigate.
> At larger scale, we expect DuckDB's advantage to grow because columnar I/O savings
> and vectorized batch processing scale with data size.

**Q: Does DuckDB support indexes like MySQL?**
> DuckDB has experimental index support, but it's designed for sequential analytical scans.
> Its min/max zone maps per row group provide some filtering, but it doesn't rely on
> B-tree indexes like MySQL does.

**Q: Why not compare with MongoDB too?**
> Our focus areas are columnar storage and vectorized execution, which are best
> contrasted with MySQL's row-based storage. We will include a MongoDB comparison
> in the final report based on architectural analysis.

**Q: How does DuckDB handle concurrent writes?**
> DuckDB uses MVCC for concurrency control, but it's designed for single-writer,
> multiple-reader scenarios. This fits our use case since stock data is batch-appended daily.
