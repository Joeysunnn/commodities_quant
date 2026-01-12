"""
因子计算模块 (Factor Calculation Module)
=========================================
核心功能：
1. 从 PostgreSQL 数据库读取清洗后的数据
2. 计算滚动分位数 (Rolling Percentile)
3. 汇总全球总库存
4. 分交易所库存分位对比

计算规则：
- 数据范围: 2021-01-01 至今 (5年)
- 滚动窗口: 3年 (约756个交易日 或 156周)
- 展示范围: 最近2年的分位数
- 金银: 日频 | 铜: 周频
- 缺失值: ffill 填充
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import sys

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from database.db_utils import get_engine

# ================= 配置 =================
# 时间配置
DATA_START_DATE = "2021-01-01"  # 数据起始日期
ROLLING_WINDOW_YEARS = 3        # 滚动窗口 (年)
DISPLAY_YEARS = 2               # 展示最近N年的分位数

# 滚动窗口天数 (日频: 252交易日/年, 周频: 52周/年)
ROLLING_WINDOW_DAYS = ROLLING_WINDOW_YEARS * 252   # 756天
ROLLING_WINDOW_WEEKS = ROLLING_WINDOW_YEARS * 52   # 156周

# 金属配置 - 基于数据库实际数据
METAL_CONFIG = {
    "COPPER": {
        "freq": "W",  # 周频
        "rolling_window": ROLLING_WINDOW_WEEKS,
        "sources": {
            "LME": "lme_closing_mt",
            "COMEX": "comex_total_mt",
            "SHFE": "shfe_total_mt"
        },
        "unit": "mt"
    },
    "GOLD": {
        "freq": "D",  # 日频
        "rolling_window": ROLLING_WINDOW_DAYS,
        "sources": {
            "COMEX": "comex_total_oz",
            "LBMA": "lbma_holdings_oz",
            "GLD": "gld_holdings_oz"
        },
        "unit": "oz"
    },
    "SILVER": {
        "freq": "D",  # 日频
        "rolling_window": ROLLING_WINDOW_DAYS,
        "sources": {
            "COMEX": "comex_total_oz",
            "LBMA": "lbma_holdings_oz",
            "SLV": "slv_holdings_oz"
        },
        "unit": "oz"
    }
}


# ================= 数据库读取 =================
def load_all_data_from_db() -> pd.DataFrame:
    """
    从 PostgreSQL 数据库加载所有清洗后的数据
    
    Returns:
        pd.DataFrame: 所有观测数据
    """
    engine = get_engine()
    
    query = """
        SELECT metal, source, freq, as_of_date, metric, value, unit
        FROM clean.observations
        WHERE as_of_date >= '2021-01-01'
        ORDER BY as_of_date
    """
    
    df = pd.read_sql(query, engine)
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    
    return df


def get_inventory_series_from_db(metal: str, source: str, metric: str) -> pd.Series:
    """
    从数据库获取指定金属、来源、指标的时间序列
    
    Args:
        metal: 金属类型 (COPPER/GOLD/SILVER)
        source: 数据来源 (LME/COMEX/SHFE/LBMA/GLD/SLV)
        metric: 指标名称
    
    Returns:
        pd.Series: 以日期为索引的库存序列
    """
    engine = get_engine()
    
    query = """
        SELECT as_of_date, value
        FROM clean.observations
        WHERE metal = %(metal)s 
          AND source = %(source)s 
          AND metric = %(metric)s
          AND as_of_date >= '2021-01-01'
        ORDER BY as_of_date
    """
    
    df = pd.read_sql(query, engine, params={'metal': metal, 'source': source, 'metric': metric})
    
    if df.empty:
        return pd.Series(dtype=float)
    
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    df = df.drop_duplicates(subset='as_of_date', keep='last')
    df = df.set_index('as_of_date').sort_index()
    
    return df['value']


def get_price_series_from_db(metal: str) -> pd.Series:
    """
    从数据库获取指定金属的价格时间序列
    
    Args:
        metal: 金属类型
    
    Returns:
        pd.Series: 以日期为索引的价格序列
    """
    engine = get_engine()
    
    query = """
        SELECT as_of_date, value
        FROM clean.observations
        WHERE metal = %(metal)s 
          AND metric = 'price_futures_usd'
          AND as_of_date >= '2021-01-01'
        ORDER BY as_of_date
    """
    
    df = pd.read_sql(query, engine, params={'metal': metal})
    
    if df.empty:
        return pd.Series(dtype=float)
    
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    df = df.drop_duplicates(subset='as_of_date', keep='last')
    df = df.set_index('as_of_date').sort_index()
    
    return df['value']


# ================= 数据预处理 =================
def prepare_inventory_data(metal: str) -> dict:
    """
    准备指定金属的库存数据 (含填充逻辑)
    
    对于缺失的早期数据，使用最早可获得的值进行回填（不存入数据库）
    
    Args:
        metal: 金属类型
    
    Returns:
        dict: {source: pd.Series} 各来源的库存序列
    """
    config = METAL_CONFIG[metal]
    freq = config['freq']
    sources = config['sources']
    
    # 创建完整日期索引
    start_date = pd.to_datetime(DATA_START_DATE)
    end_date = pd.to_datetime('today')
    
    if freq == 'D':
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # 工作日
    else:  # 'W'
        date_range = pd.date_range(start=start_date, end=end_date, freq='W-FRI')  # 每周五
    
    result = {}
    
    for source, metric in sources.items():
        series = get_inventory_series_from_db(metal, source, metric)
        
        if series.empty:
            print(f"警告: {metal}/{source}/{metric} 数据库中无数据")
            # 创建空序列，稍后会用其他来源的数据估算或置零
            series = pd.Series(index=date_range, dtype=float)
        else:
            # 重新索引到完整日期范围
            series = series.reindex(date_range)
        
        # 向前填充 (ffill)
        series = series.ffill()
        
        # 对于早期缺失数据：使用最早可获得的值回填（不存入数据库）
        if series.isna().any():
            first_valid = series.first_valid_index()
            if first_valid is not None:
                first_value = series[first_valid]
                series = series.fillna(first_value)
            else:
                # 完全无数据，填充0
                series = series.fillna(0)
        
        result[source] = series
    
    return result


# ================= 分位数计算 =================
def rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    """
    计算滚动分位数 (当前值在过去N期数据中的排名百分比)
    
    Args:
        series: 时间序列
        window: 滚动窗口大小
    
    Returns:
        pd.Series: 分位数序列 (0-1)
    """
    def calc_pct(x):
        if len(x) < window * 0.5:  # 至少需要一半的数据
            return np.nan
        current = x.iloc[-1]
        rank = (x < current).sum()
        return rank / (len(x) - 1) if len(x) > 1 else 0.5
    
    return series.rolling(window=window, min_periods=int(window * 0.5)).apply(calc_pct, raw=False)


def calculate_global_percentile(metal: str) -> pd.DataFrame:
    """
    计算全球总库存分位数走势
    
    Args:
        metal: 金属类型
    
    Returns:
        pd.DataFrame: 包含 date, total_inventory, percentile 及各来源列的数据框
    """
    config = METAL_CONFIG[metal]
    rolling_window = config['rolling_window']
    
    # 获取各来源库存数据
    inventory_data = prepare_inventory_data(metal)
    
    # 汇总全球总库存
    combined = pd.DataFrame(inventory_data)
    combined['total'] = combined.sum(axis=1)
    
    # 计算滚动分位数
    combined['percentile'] = rolling_percentile(combined['total'], rolling_window)
    
    # 只保留最近2年的数据 (展示期)
    display_start = pd.to_datetime('today') - timedelta(days=DISPLAY_YEARS * 365)
    result = combined[combined.index >= display_start].copy()
    
    # 重置索引并整理列名
    result = result.reset_index()
    result = result.rename(columns={'index': 'date'})
    
    # 重新排列列顺序: date, total, percentile, 各来源
    source_cols = list(inventory_data.keys())
    result = result[['date', 'total', 'percentile'] + source_cols]
    
    return result


def calculate_regional_percentiles(metal: str) -> pd.DataFrame:
    """
    计算各交易所独立的分位数 (用于分组柱状图)
    
    Args:
        metal: 金属类型
    
    Returns:
        pd.DataFrame: 包含 source, current_value, percentile 的数据框
    """
    config = METAL_CONFIG[metal]
    rolling_window = config['rolling_window']
    
    # 获取各来源库存数据
    inventory_data = prepare_inventory_data(metal)
    
    results = []
    for source, series in inventory_data.items():
        # 计算滚动分位数
        pct_series = rolling_percentile(series, rolling_window)
        
        # 获取最新值
        latest_value = series.iloc[-1] if not series.empty else 0
        latest_pct = pct_series.iloc[-1] if not pct_series.empty else 0.5
        
        # 处理 NaN
        if pd.isna(latest_pct):
            latest_pct = 0.5
        
        results.append({
            'source': source,
            'current_value': latest_value,
            'percentile': latest_pct
        })
    
    return pd.DataFrame(results)


def calculate_source_percentile_trend(metal: str, source: str) -> pd.DataFrame:
    """
    计算单个来源的分位数走势
    
    Args:
        metal: 金属类型
        source: 数据来源
    
    Returns:
        pd.DataFrame: 包含 date, value, percentile 的数据框
    """
    config = METAL_CONFIG[metal]
    rolling_window = config['rolling_window']
    
    if source not in config['sources']:
        raise ValueError(f"未知的数据来源: {source}")
    
    # 获取库存数据
    inventory_data = prepare_inventory_data(metal)
    series = inventory_data.get(source, pd.Series(dtype=float))
    
    if series.empty:
        return pd.DataFrame(columns=['date', 'value', 'percentile'])
    
    # 计算滚动分位数
    pct_series = rolling_percentile(series, rolling_window)
    
    # 合并结果
    result = pd.DataFrame({
        'date': series.index,
        'value': series.values,
        'percentile': pct_series.values
    })
    
    # 只保留最近2年
    display_start = pd.to_datetime('today') - timedelta(days=DISPLAY_YEARS * 365)
    result = result[result['date'] >= display_start].copy()
    
    return result


# ================= 价格数据 =================
def get_price_data(metal: str) -> pd.DataFrame:
    """
    获取价格数据
    
    Args:
        metal: 金属类型
    
    Returns:
        pd.DataFrame: 包含 date, price 的数据框
    """
    price_series = get_price_series_from_db(metal)
    
    if price_series.empty:
        return pd.DataFrame(columns=['date', 'price'])
    
    # 创建完整日期索引并ffill
    start_date = pd.to_datetime(DATA_START_DATE)
    end_date = pd.to_datetime('today')
    date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    
    price_series = price_series.reindex(date_range).ffill().bfill()
    
    # 只保留最近2年
    display_start = pd.to_datetime('today') - timedelta(days=DISPLAY_YEARS * 365)
    price_series = price_series[price_series.index >= display_start]
    
    result = pd.DataFrame({
        'date': price_series.index,
        'price': price_series.values
    })
    
    return result


# ================= 衍生因子计算层 (Derived Metrics) =================
# 用于计算差值(Diff)、比率(Ratio)、净流向(Net Flow)等衍生指标

def get_metric_series(metal: str, source: str, metric: str) -> pd.Series:
    """
    从数据库获取任意指标的时间序列 (通用函数)
    
    Args:
        metal: 金属类型
        source: 数据来源
        metric: 指标名称
    
    Returns:
        pd.Series: 以日期为索引的序列
    """
    config = METAL_CONFIG[metal]
    freq = config['freq']
    
    # 创建完整日期索引
    start_date = pd.to_datetime(DATA_START_DATE)
    end_date = pd.to_datetime('today')
    
    if freq == 'D':
        date_range = pd.date_range(start=start_date, end=end_date, freq='B')
    else:
        date_range = pd.date_range(start=start_date, end=end_date, freq='W-FRI')
    
    series = get_inventory_series_from_db(metal, source, metric)
    
    if series.empty:
        return pd.Series(index=date_range, dtype=float).fillna(0)
    
    # 重新索引并填充
    series = series.reindex(date_range).ffill()
    
    # 早期缺失数据用第一个有效值回填
    if series.isna().any():
        first_valid = series.first_valid_index()
        if first_valid is not None:
            series = series.fillna(series[first_valid])
        else:
            series = series.fillna(0)
    
    return series


def _filter_display_period(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
    """过滤只保留最近2年的数据"""
    display_start = pd.to_datetime('today') - timedelta(days=DISPLAY_YEARS * 365)
    return df[df[date_col] >= display_start].copy()


# ===================== 铜 (COPPER) 衍生因子 =====================

def get_lme_cancelled_ratio(metal: str = 'COPPER') -> pd.DataFrame:
    """
    计算 LME 注销仓单占比 (Cancelled Warrant Ratio)
    
    公式: cancelled_ratio = lme_cancelled_mt / lme_closing_mt
    含义: 高占比(>40-50%)是库存即将流出的先行指标
    
    Returns:
        pd.DataFrame: date, cancelled, closing, ratio, price
    """
    cancelled = get_metric_series(metal, 'LME', 'lme_cancelled_mt')
    closing = get_metric_series(metal, 'LME', 'lme_closing_mt')
    price = get_price_series_from_db(metal)
    
    # 对齐索引
    idx = cancelled.index
    closing = closing.reindex(idx).ffill().bfill()
    price = price.reindex(idx).ffill().bfill()
    
    # 计算比率 (避免除零)
    ratio = np.where(closing > 0, cancelled / closing, 0)
    
    result = pd.DataFrame({
        'date': idx,
        'cancelled': cancelled.values,
        'closing': closing.values,
        'ratio': ratio,
        'price': price.values
    })
    
    return _filter_display_period(result)


def get_lme_flow_analysis(metal: str = 'COPPER') -> pd.DataFrame:
    """
    LME 库存流动分析 (Delivered In vs Out)
    
    含义: 
    - Delivered In 暴增 -> 供给过剩(看空)
    - Delivered Out 暴增 -> 需求强劲(看多)
    
    Returns:
        pd.DataFrame: date, delivered_in, delivered_out, net_flow
    """
    delivered_in = get_metric_series(metal, 'LME', 'lme_delivered_in_mt')
    delivered_out = get_metric_series(metal, 'LME', 'lme_delivered_out_mt')
    
    idx = delivered_in.index
    delivered_out = delivered_out.reindex(idx).ffill().fillna(0)
    
    result = pd.DataFrame({
        'date': idx,
        'delivered_in': delivered_in.values,
        'delivered_out': delivered_out.values,
        'net_flow': delivered_in.values - delivered_out.values  # 正=净入库, 负=净出库
    })
    
    return _filter_display_period(result)


def get_comex_structure_copper() -> pd.DataFrame:
    """
    COMEX 铜库存结构 (Registered vs Eligible)
    
    含义:
    - Registered: 可交割的"真库存"
    - Eligible: 放在仓库但没注册的"潜水库存"
    - Registered极低时空头易被逼仓
    
    Returns:
        pd.DataFrame: date, registered, eligible, total, reg_ratio
    """
    registered = get_metric_series('COPPER', 'COMEX', 'comex_registered_mt')
    eligible = get_metric_series('COPPER', 'COMEX', 'comex_eligible_mt')
    
    idx = registered.index
    eligible = eligible.reindex(idx).ffill().fillna(0)
    
    total = registered + eligible
    reg_ratio = np.where(total > 0, registered / total, 0)
    
    result = pd.DataFrame({
        'date': idx,
        'registered': registered.values,
        'eligible': eligible.values,
        'total': total.values,
        'reg_ratio': reg_ratio
    })
    
    return _filter_display_period(result)


def get_price_vs_open_interest(metal: str = 'COPPER') -> pd.DataFrame:
    """
    价格与持仓量对比 (Price vs Open Interest)
    
    含义:
    - 价涨 + OI增: 多头进攻，趋势强劲(看多)
    - 价涨 + OI减: 空头止损，动力不足(中性/看跌)
    
    Returns:
        pd.DataFrame: date, price, open_interest
    """
    price = get_price_series_from_db(metal)
    oi = get_metric_series(metal, 'LME', 'lme_open_interest_mt')
    
    idx = oi.index
    price = price.reindex(idx).ffill().bfill()
    
    result = pd.DataFrame({
        'date': idx,
        'price': price.values,
        'open_interest': oi.values
    })
    
    return _filter_display_period(result)


# ===================== 黄金 (GOLD) 衍生因子 =====================

def get_gld_fund_flows() -> pd.DataFrame:
    """
    GLD ETF 资金流向 (Holdings Change vs Price)
    
    含义:
    - 价涨 + 持仓增: 真实资金流入，健康趋势(看多)
    - 价涨 + 持仓减: 无量上涨/诱多(背离/看空)
    
    Returns:
        pd.DataFrame: date, holdings, holdings_change, price
    """
    holdings = get_metric_series('GOLD', 'GLD', 'gld_holdings_oz')
    price = get_price_series_from_db('GOLD')
    
    idx = holdings.index
    price = price.reindex(idx).ffill().bfill()
    
    # 计算每日变化量
    holdings_change = holdings.diff().fillna(0)
    
    result = pd.DataFrame({
        'date': idx,
        'holdings': holdings.values,
        'holdings_change': holdings_change.values,
        'price': price.values
    })
    
    return _filter_display_period(result)


def get_comex_free_vs_pledged() -> pd.DataFrame:
    """
    COMEX 黄金真实流动性 (Free vs Pledged)
    
    含义:
    - Pledged: 已质押作为保证金，无法立刻交割
    - Free = Registered - Pledged: 真正可用的库存
    - Free归零 = 极其严重的流动性枯竭
    
    Returns:
        pd.DataFrame: date, registered, pledged, free, free_ratio
    """
    registered = get_metric_series('GOLD', 'COMEX', 'comex_registered_oz')
    pledged = get_metric_series('GOLD', 'COMEX', 'comex_pledged_oz')
    
    idx = registered.index
    pledged = pledged.reindex(idx).ffill().fillna(0)
    
    # 计算自由可用库存
    free = registered - pledged
    free = free.clip(lower=0)  # 不能为负
    
    free_ratio = np.where(registered > 0, free / registered, 1)
    
    result = pd.DataFrame({
        'date': idx,
        'registered': registered.values,
        'pledged': pledged.values,
        'free': free.values,
        'free_ratio': free_ratio
    })
    
    return _filter_display_period(result)


def get_lbma_vs_comex_gold() -> pd.DataFrame:
    """
    场外 vs 场内库存转移 (LBMA vs COMEX)
    
    含义:
    - LBMA: 全球最大金库，代表实物底仓
    - COMEX: 投机交易场所，代表衍生品库存
    - LBMA骤降+COMEX上升 = 大规模期现套利(EFP)
    
    Returns:
        pd.DataFrame: date, lbma, comex, lbma_pct, comex_pct
    """
    lbma = get_metric_series('GOLD', 'LBMA', 'lbma_holdings_oz')
    comex = get_metric_series('GOLD', 'COMEX', 'comex_total_oz')
    
    idx = lbma.index
    comex = comex.reindex(idx).ffill().fillna(0)
    
    # 计算占比 (归一化)
    total = lbma + comex
    lbma_pct = np.where(total > 0, lbma / total, 0.5)
    comex_pct = np.where(total > 0, comex / total, 0.5)
    
    result = pd.DataFrame({
        'date': idx,
        'lbma': lbma.values,
        'comex': comex.values,
        'lbma_pct': lbma_pct,
        'comex_pct': comex_pct
    })
    
    return _filter_display_period(result)


# ===================== 白银 (SILVER) 衍生因子 =====================

def get_slv_vs_comex_squeeze() -> pd.DataFrame:
    """
    "逼空监控" SLV vs COMEX Registered (Squeeze Monitor)
    
    含义:
    - SLV: 散户和投资者的囤货意愿
    - COMEX Registered: 交易所可交割现货
    - "鳄鱼大开口": SLV飙升+COMEX骤降 = 逼空信号
    
    Returns:
        pd.DataFrame: date, slv_holdings, comex_registered, divergence
    """
    slv = get_metric_series('SILVER', 'SLV', 'slv_holdings_oz')
    comex_reg = get_metric_series('SILVER', 'COMEX', 'comex_registered_oz')
    
    idx = slv.index
    comex_reg = comex_reg.reindex(idx).ffill().fillna(0)
    
    # 计算背离度 (归一化后的差值变化)
    # 使用标准化后的差值来衡量背离程度
    slv_norm = (slv - slv.mean()) / slv.std() if slv.std() > 0 else 0
    comex_norm = (comex_reg - comex_reg.mean()) / comex_reg.std() if comex_reg.std() > 0 else 0
    divergence = slv_norm - comex_norm  # 正值=SLV相对强势
    
    result = pd.DataFrame({
        'date': idx,
        'slv_holdings': slv.values,
        'comex_registered': comex_reg.values,
        'divergence': divergence.values
    })
    
    return _filter_display_period(result)


def get_comex_structure_silver() -> pd.DataFrame:
    """
    COMEX 白银库存结构 (Registered vs Eligible)
    
    含义:
    - 白银的Eligible占比通常更高(长期投资者存银条)
    - Registered/Total < 20% = 库存结构脆弱，易爆发溢价
    
    Returns:
        pd.DataFrame: date, registered, eligible, total, reg_ratio
    """
    registered = get_metric_series('SILVER', 'COMEX', 'comex_registered_oz')
    eligible = get_metric_series('SILVER', 'COMEX', 'comex_eligible_oz')
    
    idx = registered.index
    eligible = eligible.reindex(idx).ffill().fillna(0)
    
    total = registered + eligible
    reg_ratio = np.where(total > 0, registered / total, 0)
    
    result = pd.DataFrame({
        'date': idx,
        'registered': registered.values,
        'eligible': eligible.values,
        'total': total.values,
        'reg_ratio': reg_ratio
    })
    
    return _filter_display_period(result)


def get_lbma_flows_silver() -> pd.DataFrame:
    """
    LBMA 白银巨鲸流向 (Net Flows vs Price)
    
    含义:
    - LBMA是"深水区"，对应光伏等工业巨头长单储备
    - 连续大幅流出 = 工业需求旺盛，价格底部
    - 价格下跌但LBMA巨额流出 = 背离看涨
    
    Returns:
        pd.DataFrame: date, holdings, holdings_change, price
    """
    holdings = get_metric_series('SILVER', 'LBMA', 'lbma_holdings_oz')
    price = get_price_series_from_db('SILVER')
    
    idx = holdings.index
    price = price.reindex(idx).ffill().bfill()
    
    # 计算变化量 (由于LBMA是月频，这里计算日变化后ffill的效果)
    holdings_change = holdings.diff().fillna(0)
    
    result = pd.DataFrame({
        'date': idx,
        'holdings': holdings.values,
        'holdings_change': holdings_change.values,
        'price': price.values
    })
    
    return _filter_display_period(result)


# ================= 仪表盘信号 =================
def get_dashboard_signals() -> dict:
    """
    获取仪表盘多空信号
    
    Returns:
        dict: {metal: {'percentile': float, 'signal': str, 'color': str}}
    """
    signals = {}
    
    for metal in ['COPPER', 'GOLD', 'SILVER']:
        try:
            global_pct = calculate_global_percentile(metal)
            
            if global_pct.empty or global_pct['percentile'].isna().all():
                latest_pct = 0.5
            else:
                latest_pct = global_pct['percentile'].dropna().iloc[-1] if not global_pct['percentile'].dropna().empty else 0.5
            
            # 判断信号
            if latest_pct <= 0.05:
                signal = "🟢 强看多 (Shortage)"
                color = "green"
            elif latest_pct >= 0.95:
                signal = "🔴 强看空 (Glut)"
                color = "red"
            elif latest_pct <= 0.10:
                signal = "🟢 看多"
                color = "lightgreen"
            elif latest_pct >= 0.90:
                signal = "🔴 看空"
                color = "lightcoral"
            else:
                signal = "⚪ 中性 (Neutral)"
                color = "gray"
            
            signals[metal] = {
                'percentile': latest_pct,
                'signal': signal,
                'color': color
            }
        except Exception as e:
            print(f"计算 {metal} 信号时出错: {e}")
            signals[metal] = {
                'percentile': 0.5,
                'signal': "⚪ 数据缺失",
                'color': "gray"
            }
    
    return signals


def get_heatmap_data() -> pd.DataFrame:
    """
    获取热力图数据 (各交易所 x 各金属 的分位数矩阵)
    
    Returns:
        pd.DataFrame: 行=金属, 列=交易所, 值=分位数
    """
    data = []
    for metal in ['COPPER', 'GOLD', 'SILVER']:
        regional = calculate_regional_percentiles(metal)
        row = {'metal': metal}
        for _, r in regional.iterrows():
            row[r['source']] = r['percentile']
        data.append(row)
    
    result = pd.DataFrame(data)
    result = result.set_index('metal')
    
    return result


# ================= 测试入口 =================
if __name__ == "__main__":
    print("=" * 60)
    print("因子计算模块测试 (从数据库读取)")
    print("=" * 60)
    
    # 测试数据库连接和数据加载
    print("\n1. 加载数据...")
    try:
        df = load_all_data_from_db()
        print(f"   共加载 {len(df)} 条记录")
        print(f"   日期范围: {df['as_of_date'].min()} ~ {df['as_of_date'].max()}")
        print(f"   金属类型: {df['metal'].unique()}")
        print(f"   数据来源: {df['source'].unique()}")
    except Exception as e:
        print(f"   加载失败: {e}")
        exit(1)
    
    # 测试仪表盘信号
    print("\n2. 仪表盘信号...")
    signals = get_dashboard_signals()
    for metal, info in signals.items():
        print(f"   {metal}: {info['percentile']:.1%} - {info['signal']}")
    
    # 测试全球分位数
    print("\n3. 全球库存分位数 (COPPER)...")
    copper_global = calculate_global_percentile('COPPER')
    print(f"   数据行数: {len(copper_global)}")
    if not copper_global.empty:
        print(f"   最新日期: {copper_global['date'].iloc[-1]}")
        print(f"   最新总库存: {copper_global['total'].iloc[-1]:,.0f}")
        print(f"   最新分位: {copper_global['percentile'].iloc[-1]:.1%}")
    
    # 测试区域分位数
    print("\n4. 区域分位数 (COPPER)...")
    copper_regional = calculate_regional_percentiles('COPPER')
    print(copper_regional.to_string(index=False))
    
    # 测试价格数据
    print("\n5. 价格数据 (GOLD)...")
    gold_price = get_price_data('GOLD')
    print(f"   数据行数: {len(gold_price)}")
    if not gold_price.empty:
        print(f"   最新日期: {gold_price['date'].iloc[-1]}")
        print(f"   最新价格: ${gold_price['price'].iloc[-1]:,.2f}")
    
    # 测试热力图数据
    print("\n6. 热力图数据...")
    heatmap = get_heatmap_data()
    print(heatmap.to_string())
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)
