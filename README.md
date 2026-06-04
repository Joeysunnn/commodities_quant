# Commodities Quant

商品库存量化分析系统。项目围绕铜、黄金、白银的交易所库存、ETF 持仓、价格数据和衍生指标，提供 Streamlit 看板、库存因子计算、策略回测，以及定时数据更新脚本。

> 注意：本项目依赖 PostgreSQL 数据库和多个外部数据源。仓库中保留了大量历史 LME `.xls` 报告文件，主要作为历史原始数据资产，不建议在不了解用途前删除。

## 功能概览

- **Streamlit 应用**：`app.py` 是入口，页面包括宏观库存仪表盘、单金属深度分析和策略回测。
- **因子计算**：`factors.py` 从 `clean.observations` 读取清洗后的数据，计算全局库存、分交易所库存、滚动分位数和衍生分析指标。
- **策略与回测**：`strategy.py` 定义 Beta、Arbitrage、Event 三类策略信号；`backtest_engine.py` 负责向量化回测、绩效指标和图表。
- **数据更新**：`database/` 下包含 COMEX、LME、LBMA、GLD、SLV、SHFE、价格数据等抓取、清洗和入库脚本。
- **自动化任务**：`.github/workflows/daily_run.yml` 通过 GitHub Actions 定时执行每日数据更新脚本。

## 项目结构

```text
.
├── app.py                         # Streamlit 主入口
├── views/                         # Streamlit 页面模块
│   ├── dashboard.py               # 宏观库存仪表盘
│   ├── metal_analysis.py          # 铜/金/银单品种深度分析
│   └── backtest.py                # 策略回测页面
├── factors.py                     # 库存因子和分析指标
├── strategy.py                    # 策略参数、信号和策略引擎
├── backtest_engine.py             # 回测配置、绩效指标和回测绘图
├── utils.py                       # Plotly 图表工具函数
├── database/
│   ├── db_utils.py                # PostgreSQL 连接和入库工具
│   ├── daily_auto_all.py          # 日常数据更新总入口
│   ├── LME/                       # LME 自动/手动抓取及报告文件
│   ├── comex/                     # COMEX 日度数据抓取
│   ├── GLD/ LBMA/ shex/           # ETF、LBMA、上期所更新脚本
│   └── rawdata/                   # 历史原始数据、清洗脚本和图表实验脚本
├── .github/workflows/daily_run.yml
├── requirements.txt
└── .env.example
```

## 环境准备

建议使用 Python 3.10+。

```bash
python -m venv .venv
source .venv/bin/activate  # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

复制 `.env.example` 为 `.env`，并填入本地或云端 PostgreSQL 连接信息：

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=commodities_db
```

应用和数据脚本默认读取以下数据库对象：

- `clean.observations`：清洗后的观测数据，核心字段包括 `metal`、`source`、`freq`、`as_of_date`、`metric`、`value`、`unit`。
- `clean.load_runs`：数据更新脚本的运行日志。

## 运行方式

启动主应用：

```bash
streamlit run app.py
```

运行数据库连接测试：

```bash
python database/main.py --test
```

批量导入历史清洗数据：

```bash
python database/main.py --all
```

执行每日数据更新：

```bash
python database/daily_auto_all.py
```

单独执行 LME 自动更新：

```bash
python database/LME/lme_auto_update.py
```

## GitHub Actions

`.github/workflows/daily_run.yml` 当前在 UTC 时间每天 09:00 触发，也支持手动触发。运行前需要在 GitHub repository secrets 中配置：

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

其中 LME 更新依赖 Selenium、Chrome/ChromeDriver 和目标网站可访问性，CI 环境中最容易因为浏览器或网络条件失败。

## 数据说明

仓库保留了历史原始数据文件，尤其是：

- `database/LME/lme_reports/`
- `database/rawdata/LME/lme_reports/`

这些目录包含大量 LME `.xls` 报告，用于历史清洗、回溯和数据校验。它们会让仓库体积明显增大，但本次整理按保留数据资产处理，没有将其移出 git。

`.gitignore` 已忽略 `.env`、虚拟环境、缓存、日志和 CSV 文件。新增数据文件前建议先判断它属于可复现输出、临时文件，还是确实需要长期保留的原始数据。

## 维护注意事项

- `database/db_utils.py` 是数据库连接的统一入口，新增脚本应优先使用 `.env` 和 `get_engine()`，不要硬编码数据库 URL。
- `database/rawdata/figures/common.py` 统一了图表实验脚本的数据读取、`pivot_metric` 和时间范围选择器。
- `factors.py` 和 Streamlit 页面依赖数据库中已有的清洗数据；没有数据库时可以通过语法检查，但无法完整运行看板。
- 数据抓取脚本依赖外部网站、网络状态和数据源页面结构，失败时先检查网络、headers、浏览器自动化和数据源是否改版。
- 回测结果仅用于研究，不代表未来收益，也没有覆盖实盘流动性、保证金和执行成本的全部约束。

## 快速排障

- **`ModuleNotFoundError`**：确认已执行 `pip install -r requirements.txt`。
- **数据库连接失败**：检查 `.env`、PostgreSQL 服务、schema/table 是否存在。
- **Streamlit 页面无数据**：确认 `clean.observations` 中有对应 `metal/source/metric` 数据。
- **GitHub Actions 失败**：优先查看 secrets、外部网站访问、Selenium/ChromeDriver 日志。
- **中文显示异常**：旧文件中存在历史编码问题；若要修复 UI 文案，建议单独开一轮小范围编码/文案修复，避免和业务逻辑混在一起。
