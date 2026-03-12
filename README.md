# Stock Analytics — DuckDB vs MySQL Benchmark

DSCI 551 Project: *Inside DuckDB: Columnar Storage and Vectorized Query Execution*

---

## 项目结构

```
stock-analytics/
├── pyproject.toml                  # uv 项目配置
├── src/
│   └── stock_analytics/
│       ├── __init__.py
│       └── main.py                 # 主程序
├── data/                           # 自动创建 — 存放 Parquet 缓存 & DuckDB 文件
│   ├── AAPL_MSFT_..._2015_2025.parquet
│   └── stock_analytics.duckdb
└── output/                         # 自动创建 — 存放每次 benchmark 结果
    ├── benchmark_results.csv       # 累计追加（多次运行叠加）
    ├── benchmark_20250309_120000.json
    ├── explain_Q1_50day_MA.txt
    ├── explain_Q3_annual_summary.txt
    └── explain_Q4_full_scan.txt
```

---

## 快速开始

### 1. 安装 uv（如未安装）

```bash
# Linux / WSL
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. 创建虚拟环境 & 安装依赖

```bash
cd stock-analytics
uv sync          # 自动读取 pyproject.toml，创建 .venv 并安装所有依赖
```

### 3. 运行

```bash
uv run python src/stock_analytics/main.py
# 或者用 entry point（已在 pyproject.toml 定义）:
uv run run-benchmark
```

---

## WSL MySQL 配置 & Windows Workbench 连接

### WSL 内启动 MySQL

```bash
# 启动服务
sudo service mysql start

# 验证运行状态
sudo service mysql status

# 设置 root 密码（首次配置）
sudo mysql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'password';
FLUSH PRIVILEGES;
EXIT;
```

### 允许远程连接（Workbench 需要）

默认 MySQL 只监听 127.0.0.1，Workbench 从 Windows 连接需要改为监听 0.0.0.0：

```bash
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
```

找到这一行并修改：
```
# 原来
bind-address = 127.0.0.1
# 改为
bind-address = 0.0.0.0
```

然后重启 MySQL：
```bash
sudo service mysql restart
```

再授权 root 允许从任意 IP 连接：
```sql
sudo mysql -u root -p
CREATE USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

### 获取 WSL 的 IP 地址

```powershell
# 在 PowerShell（Windows）中运行
wsl hostname -I
# 例如输出: 172.24.48.100
```

### MySQL Workbench 连接配置

| 字段 | 值 |
|---|---|
| Connection Method | Standard TCP/IP |
| Hostname | WSL 的 IP（如 `172.24.48.100`） |
| Port | `3306` |
| Username | `root` |
| Password | 你设置的密码 |

> **注意**：WSL2 的 IP 在每次重启 Windows 后可能会变化。可以在 WSL 内运行 `ip addr show eth0` 查看最新 IP。

---

## Output 说明

| 文件 | 内容 |
|---|---|
| `benchmark_results.csv` | 每次运行的所有查询耗时，按 session 追加，方便 Excel/Pandas 分析 |
| `benchmark_<timestamp>.json` | 单次完整快照，含描述和 focus 字段 |
| `explain_<QueryID>.txt` | DuckDB + MySQL 的 EXPLAIN ANALYZE 输出，Midterm Report 直接引用 |

---

## 查询集合说明

| Query ID | 场景 | 测试重点 |
|---|---|---|
| Q1_50day_MA | 50日均线 | Vectorized Window Function |
| Q2_daily_volatility | 每日波动率 | Columnar Scan（只读 3 列） |
| Q3_annual_summary | 年度汇总 | GROUP BY + 多聚合 |
| Q4_full_scan | 全表扫描 | I/O 压力测试 |
| Q5_rolling_stddev | 20日滚动标准差 | STDDEV Window |
