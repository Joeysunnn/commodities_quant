# Commodities Quant

Commodity inventory quant analysis system for copper, gold, and silver. The project combines exchange inventory, ETF holdings, price data, and derived inventory factors into a Streamlit dashboard, single-metal analysis pages, strategy backtests, and scheduled data update scripts.

> Note: This project depends on PostgreSQL and several external data sources. The repository also keeps many historical LME `.xls` report files as raw data assets for cleaning, reconciliation, and backfills. Do not delete them unless you are sure they are no longer needed.

## Features

- **Streamlit app**: `app.py` is the main entry point. It includes a macro inventory dashboard, copper/gold/silver analysis pages, and a strategy backtest page.
- **Factor calculation**: `factors.py` reads cleaned observations from `clean.observations` and calculates global inventory, exchange-level inventory, rolling percentiles, and derivative analysis metrics.
- **Strategies and backtests**: `strategy.py` defines Beta, Arbitrage, and Event strategy signals. `backtest_engine.py` handles vectorized backtests, performance metrics, and plots.
- **Data updates**: `database/` contains scripts for COMEX, LME, LBMA, GLD, SLV, SHFE, and price data ingestion, cleaning, and database loading.
- **Automation**: `.github/workflows/daily_run.yml` runs the daily update scripts on a schedule through GitHub Actions.

## Project Structure

```text
.
|-- app.py                         # Streamlit main entry
|-- views/                         # Streamlit page modules
|   |-- dashboard.py               # Macro inventory dashboard
|   |-- metal_analysis.py          # Copper, gold, and silver analysis
|   `-- backtest.py                # Strategy backtest page
|-- factors.py                     # Inventory factors and analysis metrics
|-- strategy.py                    # Strategy parameters, signals, and engine
|-- backtest_engine.py             # Backtest configuration, metrics, and charts
|-- utils.py                       # Plotly chart helpers
|-- database/
|   |-- db_utils.py                # PostgreSQL connection and loading helpers
|   |-- main.py                    # Batch import and database utility entry
|   |-- daily_auto_all.py          # General daily data update entry
|   |-- LME/                       # LME automatic/manual update scripts and reports
|   |-- comex/                     # COMEX daily data fetching
|   |-- GLD/ LBMA/ shex/           # ETF, LBMA, and SHFE update scripts
|   `-- rawdata/                   # Historical raw data, cleaning scripts, experiments
|-- .github/workflows/daily_run.yml
|-- requirements.txt
`-- .env.example
```

## Environment Setup

Python 3.10+ is recommended.

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Copy `.env.example` to `.env`, then fill in the local or remote PostgreSQL connection settings:

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=commodities_db
```

The app and data scripts expect these database objects:

- `clean.observations`: cleaned observation data. Core fields include `metal`, `source`, `freq`, `as_of_date`, `metric`, `value`, and `unit`.
- `clean.load_runs`: run logs for data update scripts.

## Usage

Start the Streamlit app:

```bash
streamlit run app.py
```

Test the database connection:

```bash
python database/main.py --test
```

Import all available historical cleaned data:

```bash
python database/main.py --all
```

Run the general daily data update:

```bash
python database/daily_auto_all.py
```

Run the LME automatic update separately:

```bash
python database/LME/lme_auto_update.py
```

## GitHub Actions

`.github/workflows/daily_run.yml` runs every day at 09:00 UTC and also supports manual dispatch.

Configure these repository secrets before running the workflow:

- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`

The workflow installs `requirements.txt`, then runs:

```bash
python database/daily_auto_all.py
python database/LME/lme_auto_update.py
```

The LME update depends on Selenium, Chrome/ChromeDriver, and external website availability. In CI, failures are often caused by browser setup, network access, or upstream page changes.

## Data Notes

The repository keeps historical raw data files, especially:

- `database/LME/lme_reports/`
- `database/rawdata/LME/lme_reports/`

These directories contain many LME `.xls` reports used for historical cleaning, backfills, and validation. They make the repository much larger, but they are treated as retained raw data assets.

`.gitignore` already excludes `.env`, virtual environments, caches, logs, and CSV files. Before adding new data files, decide whether they are reproducible outputs, temporary files, or raw data assets that should be kept.

## Maintenance Notes

- Use `database/db_utils.py` and `.env` for database access. Avoid hard-coding database URLs in new scripts.
- `database/rawdata/figures/common.py` centralizes data reads, `pivot_metric`, and date range selectors for figure experiments.
- `factors.py` and the Streamlit pages depend on cleaned data already existing in PostgreSQL. Without the database, syntax checks can still run, but the dashboard cannot be fully exercised.
- Data scrapers depend on external websites, network conditions, and upstream page structures. When a scraper fails, check network access, headers, browser automation, and source website changes first.
- Backtest output is for research only. It does not guarantee future returns and does not fully model live trading liquidity, margin, or execution costs.

## Troubleshooting

- **`ModuleNotFoundError`**: run `pip install -r requirements.txt`.
- **Database connection failure**: check `.env`, the PostgreSQL service, and whether the required schema/tables exist.
- **Streamlit page has no data**: confirm that `clean.observations` contains the expected `metal/source/metric` records.
- **GitHub Actions failure**: check repository secrets, external website access, and Selenium/ChromeDriver logs.
- **Chinese text displays incorrectly**: some older files may have encoding issues. Fix UI copy in a small dedicated change to avoid mixing text cleanup with business logic changes.

---

# Commodities Quant 中文版

商品库存量化分析系统，覆盖铜、黄金和白银。项目将交易所库存、ETF 持仓、价格数据和衍生库存因子整合到 Streamlit 看板、单金属分析页面、策略回测和定时数据更新脚本中。

> 注意：本项目依赖 PostgreSQL 数据库和多个外部数据源。仓库中也保留了大量历史 LME `.xls` 报告文件，主要作为清洗、回溯和校验用的原始数据资产。除非确认不再需要，否则不要删除。

## 功能概览

- **Streamlit 应用**：`app.py` 是主入口，页面包括宏观库存仪表盘、铜/黄金/白银分析页和策略回测页。
- **因子计算**：`factors.py` 从 `clean.observations` 读取清洗后的数据，计算全球库存、分交易所库存、滚动分位数和衍生分析指标。
- **策略与回测**：`strategy.py` 定义 Beta、Arbitrage、Event 三类策略信号，`backtest_engine.py` 负责向量化回测、绩效指标和图表。
- **数据更新**：`database/` 包含 COMEX、LME、LBMA、GLD、SLV、SHFE 和价格数据的抓取、清洗和入库脚本。
- **自动化任务**：`.github/workflows/daily_run.yml` 通过 GitHub Actions 定时执行每日数据更新脚本。

## 项目结构

```text
.
|-- app.py                         # Streamlit 主入口
|-- views/                         # Streamlit 页面模块
|   |-- dashboard.py               # 宏观库存仪表盘
|   |-- metal_analysis.py          # 铜、黄金、白银单品种分析
|   `-- backtest.py                # 策略回测页面
|-- factors.py                     # 库存因子和分析指标
|-- strategy.py                    # 策略参数、信号和策略引擎
|-- backtest_engine.py             # 回测配置、绩效指标和图表
|-- utils.py                       # Plotly 图表工具函数
|-- database/
|   |-- db_utils.py                # PostgreSQL 连接和入库工具
|   |-- main.py                    # 批量入库和数据库工具入口
|   |-- daily_auto_all.py          # 日常数据更新总入口
|   |-- LME/                       # LME 自动/手动更新脚本和报告文件
|   |-- comex/                     # COMEX 日度数据抓取
|   |-- GLD/ LBMA/ shex/           # ETF、LBMA、上期所更新脚本
|   `-- rawdata/                   # 历史原始数据、清洗脚本和图表实验
|-- .github/workflows/daily_run.yml
|-- requirements.txt
`-- .env.example
```

## 环境准备

建议使用 Python 3.10+。

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

复制 `.env.example` 为 `.env`，并填写本地或云端 PostgreSQL 连接信息：

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=commodities_db
```

应用和数据脚本默认依赖以下数据库对象：

- `clean.observations`：清洗后的观测数据，核心字段包括 `metal`、`source`、`freq`、`as_of_date`、`metric`、`value` 和 `unit`。
- `clean.load_runs`：数据更新脚本的运行日志。

## 运行方式

启动 Streamlit 主应用：

```bash
streamlit run app.py
```

测试数据库连接：

```bash
python database/main.py --test
```

批量导入历史清洗数据：

```bash
python database/main.py --all
```

执行日常数据更新：

```bash
python database/daily_auto_all.py
```

单独执行 LME 自动更新：

```bash
python database/LME/lme_auto_update.py
```

## GitHub Actions

`.github/workflows/daily_run.yml` 当前在 UTC 时间每天 09:00 触发，也支持手动触发。

运行前需要在 GitHub repository secrets 中配置：

- `DB_USER`
- `DB_PASSWORD`
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`

工作流会安装 `requirements.txt`，随后运行：

```bash
python database/daily_auto_all.py
python database/LME/lme_auto_update.py
```

其中 LME 更新依赖 Selenium、Chrome/ChromeDriver 和目标网站可访问性。CI 环境中常见失败原因包括浏览器配置、网络访问和上游页面结构变化。

## 数据说明

仓库保留了历史原始数据文件，尤其是：

- `database/LME/lme_reports/`
- `database/rawdata/LME/lme_reports/`

这些目录包含大量 LME `.xls` 报告，用于历史清洗、回溯和数据校验。它们会显著增加仓库体积，但目前按需要长期保留的原始数据资产处理。

`.gitignore` 已忽略 `.env`、虚拟环境、缓存、日志和 CSV 文件。新增数据文件前，建议先判断它属于可复现输出、临时文件，还是确实需要长期保留的原始数据。

## 维护注意事项

- `database/db_utils.py` 是数据库连接的统一入口，新增脚本应优先使用 `.env` 和工具函数，不要硬编码数据库 URL。
- `database/rawdata/figures/common.py` 统一了图表实验脚本的数据读取、`pivot_metric` 和时间范围选择器。
- `factors.py` 和 Streamlit 页面依赖数据库中已有的清洗数据；没有数据库时可以做语法检查，但无法完整运行看板。
- 数据抓取脚本依赖外部网站、网络状态和数据源页面结构。失败时先检查网络、headers、浏览器自动化和数据源是否改版。
- 回测结果仅用于研究，不代表未来收益，也没有完整覆盖实盘流动性、保证金和执行成本约束。

## 快速排障

- **`ModuleNotFoundError`**：确认已执行 `pip install -r requirements.txt`。
- **数据库连接失败**：检查 `.env`、PostgreSQL 服务以及 schema/table 是否存在。
- **Streamlit 页面无数据**：确认 `clean.observations` 中有对应的 `metal/source/metric` 数据。
- **GitHub Actions 失败**：优先查看 secrets、外部网站访问、Selenium/ChromeDriver 日志。
- **中文显示异常**：部分旧文件可能存在编码问题。如需修复 UI 文案，建议单独进行小范围编码/文案修复，避免和业务逻辑混在一起。
