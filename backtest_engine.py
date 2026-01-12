"""
向量化回测引擎 (Vectorized Backtesting Engine)
==============================================
第二阶段：向量化回测

核心功能：
1. 利用 pandas 矩阵运算快速验证策略历史表现
2. 与 strategy.py 的信号系统无缝对接
3. 计算标准化绩效指标 (夏普、最大回撤、CAGR等)
4. 生成可视化回测报告

关键设计：
- 信号滞后 (shift(1)) 避免 Look-ahead Bias
- 支持交易成本模拟
- 分策略、分金属的灵活回测
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
import sys

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from factors import get_price_series_from_db, METAL_CONFIG
from strategy import (
    BetaStrategy, ArbitrageStrategy, EventStrategy,
    BetaStrategyParams, ArbitrageStrategyParams, EventStrategyParams,
    Signal, create_default_engine
)


# ================= 回测参数配置 =================
@dataclass
class BacktestConfig:
    """回测配置参数"""
    # 交易成本
    commission_rate: float = 0.001      # 手续费率 (0.1%)
    slippage_rate: float = 0.0005       # 滑点 (0.05%)
    
    # 风险参数
    risk_free_rate: float = 0.02        # 无风险利率 (年化2%)
    trading_days_per_year: int = 252    # 年交易日
    
    # 回测范围
    start_date: Optional[str] = None    # 起始日期 (None=全部数据)
    end_date: Optional[str] = None      # 结束日期 (None=至今)


# ================= 绩效指标计算 =================
@dataclass
class PerformanceMetrics:
    """绩效指标"""
    # 收益指标
    total_return: float = 0.0           # 总收益率
    cagr: float = 0.0                   # 年化复合增长率
    
    # 风险指标
    volatility: float = 0.0             # 年化波动率
    max_drawdown: float = 0.0           # 最大回撤
    max_drawdown_duration: int = 0      # 最大回撤持续天数
    
    # 风险调整收益
    sharpe_ratio: float = 0.0           # 夏普比率
    sortino_ratio: float = 0.0          # 索提诺比率 (只考虑下行波动)
    calmar_ratio: float = 0.0           # 卡玛比率 (收益/最大回撤)
    
    # 交易统计
    total_trades: int = 0               # 交易次数
    win_rate: float = 0.0               # 胜率
    profit_factor: float = 0.0          # 盈亏比
    avg_trade_return: float = 0.0       # 平均每笔收益
    
    # 基准对比
    benchmark_return: float = 0.0       # 基准收益
    alpha: float = 0.0                  # 超额收益
    beta: float = 0.0                   # 市场敏感度
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            '总收益率': f'{self.total_return:.2%}',
            '年化收益(CAGR)': f'{self.cagr:.2%}',
            '年化波动率': f'{self.volatility:.2%}',
            '最大回撤': f'{self.max_drawdown:.2%}',
            '最大回撤天数': f'{self.max_drawdown_duration}天',
            '夏普比率': f'{self.sharpe_ratio:.2f}',
            '索提诺比率': f'{self.sortino_ratio:.2f}',
            '卡玛比率': f'{self.calmar_ratio:.2f}',
            '交易次数': self.total_trades,
            '胜率': f'{self.win_rate:.1%}',
            '盈亏比': f'{self.profit_factor:.2f}',
            '基准收益': f'{self.benchmark_return:.2%}',
            'Alpha': f'{self.alpha:.2%}',
            'Beta': f'{self.beta:.2f}',
        }


# ================= 核心回测引擎 =================
class VectorBacktester:
    """
    向量化回测引擎
    
    特点：
    1. 使用 pandas 向量运算，避免 for 循环
    2. 信号自动滞后一期，避免前视偏差
    3. 支持交易成本模拟
    """
    
    def __init__(self, config: BacktestConfig = None):
        """
        初始化回测引擎
        
        Args:
            config: 回测配置参数
        """
        self.config = config or BacktestConfig()
        self.results: pd.DataFrame = pd.DataFrame()
        self.metrics: PerformanceMetrics = PerformanceMetrics()
        self._is_run = False
    
    def prepare_data(self, metal: str, signals_df: pd.DataFrame) -> pd.DataFrame:
        """
        准备回测数据：合并价格与信号
        
        Args:
            metal: 金属类型 (COPPER/GOLD/SILVER)
            signals_df: 策略信号 DataFrame，需包含 'date', 'signal' 列
        
        Returns:
            pd.DataFrame: 合并后的数据
        """
        # 获取价格数据
        price_series = get_price_series_from_db(metal)
        
        if price_series.empty:
            raise ValueError(f"无法获取 {metal} 价格数据")
        
        # 创建价格 DataFrame
        price_df = pd.DataFrame({
            'date': price_series.index,
            'price': price_series.values
        })
        
        # 确保日期格式一致
        price_df['date'] = pd.to_datetime(price_df['date'])
        signals_df = signals_df.copy()
        signals_df['date'] = pd.to_datetime(signals_df['date'])
        
        # 合并数据
        df = pd.merge(price_df, signals_df[['date', 'signal']], on='date', how='left')
        
        # 填充缺失信号 (用前值填充，无信号默认为0)
        df['signal'] = df['signal'].ffill().fillna(0).astype(int)
        
        # 设置日期索引
        df = df.set_index('date').sort_index()
        
        # 应用日期范围过滤
        if self.config.start_date:
            df = df[df.index >= pd.to_datetime(self.config.start_date)]
        if self.config.end_date:
            df = df[df.index <= pd.to_datetime(self.config.end_date)]
        
        return df
    
    def run(self, metal: str, signals_df: pd.DataFrame) -> pd.DataFrame:
        """
        执行回测
        
        Args:
            metal: 金属类型
            signals_df: 策略信号 DataFrame
        
        Returns:
            pd.DataFrame: 回测结果
        """
        # 准备数据
        df = self.prepare_data(metal, signals_df)
        
        if df.empty or len(df) < 2:
            raise ValueError("数据不足，无法执行回测")
        
        # ========== 核心计算逻辑 ==========
        
        # 1. 计算市场收益率 (对数收益)
        df['market_return'] = np.log(df['price'] / df['price'].shift(1))
        
        # 2. 信号滞后一期 (关键！避免前视偏差)
        # 今天收盘后才能看到库存数据，所以信号只能在明天执行
        df['position'] = df['signal'].shift(1).fillna(0).astype(int)
        
        # 3. 计算持仓变化 (用于计算交易成本)
        df['position_change'] = df['position'].diff().abs().fillna(0)
        
        # 4. 计算交易成本
        total_cost_rate = self.config.commission_rate + self.config.slippage_rate
        df['trade_cost'] = df['position_change'] * total_cost_rate
        
        # 5. 计算策略收益 (含交易成本)
        df['strategy_return'] = df['position'] * df['market_return'] - df['trade_cost']
        
        # 6. 计算累计净值曲线
        df['cumulative_market'] = (1 + df['market_return'].fillna(0)).cumprod()
        df['cumulative_strategy'] = (1 + df['strategy_return'].fillna(0)).cumprod()
        
        # 7. 计算回撤
        df['strategy_peak'] = df['cumulative_strategy'].cummax()
        df['drawdown'] = df['cumulative_strategy'] / df['strategy_peak'] - 1
        
        self.results = df
        self._is_run = True
        
        # 计算绩效指标
        self._calculate_metrics()
        
        return df
    
    def run_strategy(self, strategy) -> pd.DataFrame:
        """
        直接运行策略对象进行回测
        
        Args:
            strategy: 策略实例 (BetaStrategy/ArbitrageStrategy/EventStrategy)
        
        Returns:
            pd.DataFrame: 回测结果
        """
        # 生成信号
        signals_df = strategy.generate_signals()
        
        if signals_df.empty:
            raise ValueError(f"策略 {strategy.name} 未生成任何信号")
        
        # 执行回测
        return self.run(strategy.metal, signals_df)
    
    def _calculate_metrics(self):
        """计算绩效指标"""
        if not self._is_run or self.results.empty:
            return
        
        df = self.results
        config = self.config
        
        # 基础数据
        returns = df['strategy_return'].dropna()
        market_returns = df['market_return'].dropna()
        n_days = len(returns)
        
        if n_days == 0:
            return
        
        # ===== 收益指标 =====
        # 总收益率
        total_return = df['cumulative_strategy'].iloc[-1] - 1
        
        # 年化收益 (CAGR)
        years = n_days / config.trading_days_per_year
        cagr = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
        
        # ===== 风险指标 =====
        # 年化波动率
        volatility = returns.std() * np.sqrt(config.trading_days_per_year)
        
        # 最大回撤
        max_drawdown = df['drawdown'].min()
        
        # 最大回撤持续天数
        drawdown_duration = self._calc_max_drawdown_duration(df)
        
        # ===== 风险调整收益 =====
        # 夏普比率
        excess_return = cagr - config.risk_free_rate
        sharpe = excess_return / volatility if volatility > 0 else 0
        
        # 索提诺比率 (只考虑下行波动)
        downside_returns = returns[returns < 0]
        downside_std = downside_returns.std() * np.sqrt(config.trading_days_per_year) if len(downside_returns) > 0 else 0
        sortino = excess_return / downside_std if downside_std > 0 else 0
        
        # 卡玛比率
        calmar = cagr / abs(max_drawdown) if max_drawdown != 0 else 0
        
        # ===== 交易统计 =====
        # 交易次数 (持仓变化次数)
        position_changes = df['position_change']
        total_trades = int((position_changes > 0).sum())
        
        # 胜率和盈亏比
        win_rate, profit_factor, avg_trade_return = self._calc_trade_stats(df)
        
        # ===== 基准对比 =====
        benchmark_return = df['cumulative_market'].iloc[-1] - 1
        alpha = total_return - benchmark_return
        
        # Beta (策略与市场的协方差 / 市场方差)
        if market_returns.var() > 0:
            beta = returns.cov(market_returns) / market_returns.var()
        else:
            beta = 0
        
        # 保存指标
        self.metrics = PerformanceMetrics(
            total_return=total_return,
            cagr=cagr,
            volatility=volatility,
            max_drawdown=max_drawdown,
            max_drawdown_duration=drawdown_duration,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            total_trades=total_trades,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_trade_return=avg_trade_return,
            benchmark_return=benchmark_return,
            alpha=alpha,
            beta=beta
        )
    
    def _calc_max_drawdown_duration(self, df: pd.DataFrame) -> int:
        """计算最大回撤持续天数"""
        in_drawdown = df['drawdown'] < 0
        
        if not in_drawdown.any():
            return 0
        
        # 计算连续回撤期
        groups = (~in_drawdown).cumsum()
        drawdown_lengths = in_drawdown.groupby(groups).sum()
        
        return int(drawdown_lengths.max()) if len(drawdown_lengths) > 0 else 0
    
    def _calc_trade_stats(self, df: pd.DataFrame) -> Tuple[float, float, float]:
        """计算交易统计指标"""
        # 找出每笔交易的收益
        position_changes = df['position_change'] > 0
        trade_indices = position_changes[position_changes].index.tolist()
        
        if len(trade_indices) < 2:
            return 0.0, 0.0, 0.0
        
        # 计算每笔交易的收益
        trade_returns = []
        for i in range(len(trade_indices) - 1):
            start_idx = trade_indices[i]
            end_idx = trade_indices[i + 1]
            
            period_return = df.loc[start_idx:end_idx, 'strategy_return'].sum()
            trade_returns.append(period_return)
        
        if not trade_returns:
            return 0.0, 0.0, 0.0
        
        trade_returns = np.array(trade_returns)
        
        # 胜率
        wins = (trade_returns > 0).sum()
        win_rate = wins / len(trade_returns) if len(trade_returns) > 0 else 0
        
        # 盈亏比
        avg_win = trade_returns[trade_returns > 0].mean() if wins > 0 else 0
        losses = (trade_returns < 0).sum()
        avg_loss = abs(trade_returns[trade_returns < 0].mean()) if losses > 0 else 1
        profit_factor = avg_win / avg_loss if avg_loss > 0 else 0
        
        # 平均收益
        avg_trade_return = trade_returns.mean()
        
        return win_rate, profit_factor, avg_trade_return
    
    def get_metrics(self) -> PerformanceMetrics:
        """获取绩效指标"""
        return self.metrics
    
    def get_metrics_dict(self) -> dict:
        """获取绩效指标字典"""
        return self.metrics.to_dict()
    
    def plot_equity_curve(self, title: str = "回测结果") -> go.Figure:
        """
        绘制资金曲线
        
        Args:
            title: 图表标题
        
        Returns:
            plotly Figure 对象
        """
        if not self._is_run:
            raise ValueError("请先运行回测")
        
        df = self.results
        
        # 创建子图
        fig = make_subplots(
            rows=3, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.5, 0.25, 0.25],
            subplot_titles=('净值曲线', '持仓信号', '回撤')
        )
        
        # 1. 净值曲线
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df['cumulative_strategy'],
                name='策略净值', line=dict(color='#2E86AB', width=2)
            ),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df['cumulative_market'],
                name='基准(买入持有)', line=dict(color='#A23B72', width=1.5, dash='dot')
            ),
            row=1, col=1
        )
        
        # 2. 持仓信号
        colors = df['position'].map({1: '#00C853', 0: '#9E9E9E', -1: '#FF5252'})
        fig.add_trace(
            go.Bar(
                x=df.index, y=df['position'],
                name='持仓', marker_color=colors.tolist(),
                showlegend=False
            ),
            row=2, col=1
        )
        
        # 3. 回撤
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df['drawdown'],
                name='回撤', fill='tozeroy',
                line=dict(color='#FF5252', width=1),
                fillcolor='rgba(255, 82, 82, 0.3)'
            ),
            row=3, col=1
        )
        
        # 更新布局
        fig.update_layout(
            title=dict(text=title, x=0.5),
            height=700,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode='x unified'
        )
        
        fig.update_yaxes(title_text="净值", row=1, col=1)
        fig.update_yaxes(title_text="仓位", row=2, col=1, tickvals=[-1, 0, 1], ticktext=['空', '平', '多'])
        fig.update_yaxes(title_text="回撤%", tickformat='.1%', row=3, col=1)
        fig.update_xaxes(title_text="日期", row=3, col=1)
        
        return fig
    
    def plot_monthly_returns(self) -> go.Figure:
        """绘制月度收益热力图"""
        if not self._is_run:
            raise ValueError("请先运行回测")
        
        df = self.results.copy()
        df['year'] = df.index.year
        df['month'] = df.index.month
        
        # 计算月度收益
        monthly = df.groupby(['year', 'month'])['strategy_return'].sum().unstack()
        
        # 创建热力图
        fig = go.Figure(data=go.Heatmap(
            z=monthly.values,
            x=['1月', '2月', '3月', '4月', '5月', '6月', 
               '7月', '8月', '9月', '10月', '11月', '12月'],
            y=monthly.index.astype(str),
            colorscale='RdYlGn',
            zmid=0,
            text=np.round(monthly.values * 100, 1),
            texttemplate='%{text:.1f}%',
            textfont={"size": 10},
            hovertemplate='%{y}年%{x}: %{z:.2%}<extra></extra>'
        ))
        
        fig.update_layout(
            title=dict(text='月度收益热力图', x=0.5),
            xaxis_title='月份',
            yaxis_title='年份',
            height=400
        )
        
        return fig
    
    def summary(self) -> str:
        """生成回测摘要报告"""
        if not self._is_run:
            return "未执行回测"
        
        m = self.metrics
        
        report = f"""
╔══════════════════════════════════════════════════════╗
║              📊 回测绩效报告                          ║
╠══════════════════════════════════════════════════════╣
║ 【收益指标】                                          ║
║   总收益率:     {m.total_return:>10.2%}                ║
║   年化收益:     {m.cagr:>10.2%}                        ║
║   基准收益:     {m.benchmark_return:>10.2%}            ║
║   Alpha:        {m.alpha:>10.2%}                       ║
╠══════════════════════════════════════════════════════╣
║ 【风险指标】                                          ║
║   年化波动率:   {m.volatility:>10.2%}                  ║
║   最大回撤:     {m.max_drawdown:>10.2%}                ║
║   回撤天数:     {m.max_drawdown_duration:>10}天        ║
║   Beta:         {m.beta:>10.2f}                        ║
╠══════════════════════════════════════════════════════╣
║ 【风险调整收益】                                      ║
║   夏普比率:     {m.sharpe_ratio:>10.2f}                ║
║   索提诺比率:   {m.sortino_ratio:>10.2f}               ║
║   卡玛比率:     {m.calmar_ratio:>10.2f}                ║
╠══════════════════════════════════════════════════════╣
║ 【交易统计】                                          ║
║   交易次数:     {m.total_trades:>10}                   ║
║   胜率:         {m.win_rate:>10.1%}                    ║
║   盈亏比:       {m.profit_factor:>10.2f}               ║
╚══════════════════════════════════════════════════════╝
"""
        return report


# ================= 便捷回测函数 =================
def backtest_beta_strategy(metal: str, params: BetaStrategyParams = None, 
                           config: BacktestConfig = None) -> Tuple[VectorBacktester, pd.DataFrame]:
    """
    快速回测趋势策略
    
    Args:
        metal: 金属类型
        params: 策略参数
        config: 回测配置
    
    Returns:
        (回测器实例, 回测结果DataFrame)
    """
    strategy = BetaStrategy(metal, params)
    backtester = VectorBacktester(config)
    results = backtester.run_strategy(strategy)
    
    return backtester, results


def backtest_arbitrage_strategy(metal: str = 'COPPER', params: ArbitrageStrategyParams = None,
                                 config: BacktestConfig = None) -> Tuple[VectorBacktester, pd.DataFrame]:
    """
    快速回测套利策略
    
    Args:
        metal: 金属类型 (默认铜)
        params: 策略参数
        config: 回测配置
    
    Returns:
        (回测器实例, 回测结果DataFrame)
    """
    strategy = ArbitrageStrategy(metal, params)
    backtester = VectorBacktester(config)
    results = backtester.run_strategy(strategy)
    
    return backtester, results


def backtest_event_strategy(params: EventStrategyParams = None,
                            config: BacktestConfig = None) -> Tuple[VectorBacktester, pd.DataFrame]:
    """
    快速回测事件驱动策略 (白银逼空)
    
    Args:
        params: 策略参数
        config: 回测配置
    
    Returns:
        (回测器实例, 回测结果DataFrame)
    """
    strategy = EventStrategy(params)
    backtester = VectorBacktester(config)
    results = backtester.run_strategy(strategy)
    
    return backtester, results


def backtest_all_strategies(config: BacktestConfig = None) -> Dict[str, VectorBacktester]:
    """
    回测所有策略
    
    Returns:
        dict: {策略名称: 回测器实例}
    """
    config = config or BacktestConfig()
    results = {}
    
    # Beta策略 - 三个金属
    for metal in ['COPPER', 'GOLD', 'SILVER']:
        key = f"Beta_{metal}"
        try:
            backtester, _ = backtest_beta_strategy(metal, config=config)
            results[key] = backtester
            print(f"✓ {key} 回测完成")
        except Exception as e:
            print(f"✗ {key} 回测失败: {e}")
    
    # 套利策略 - 铜
    try:
        backtester, _ = backtest_arbitrage_strategy(config=config)
        results['Arbitrage_COPPER'] = backtester
        print("✓ Arbitrage_COPPER 回测完成")
    except Exception as e:
        print(f"✗ Arbitrage_COPPER 回测失败: {e}")
    
    # 事件策略 - 白银
    try:
        backtester, _ = backtest_event_strategy(config=config)
        results['Event_SILVER'] = backtester
        print("✓ Event_SILVER 回测完成")
    except Exception as e:
        print(f"✗ Event_SILVER 回测失败: {e}")
    
    return results


def compare_strategies(backtesters: Dict[str, VectorBacktester]) -> pd.DataFrame:
    """
    对比多个策略的绩效
    
    Args:
        backtesters: {策略名称: 回测器实例}
    
    Returns:
        pd.DataFrame: 绩效对比表
    """
    comparison = []
    
    for name, bt in backtesters.items():
        m = bt.get_metrics()
        comparison.append({
            '策略': name,
            '总收益': m.total_return,
            '年化收益': m.cagr,
            '夏普比率': m.sharpe_ratio,
            '最大回撤': m.max_drawdown,
            '胜率': m.win_rate,
            '交易次数': m.total_trades,
            'Alpha': m.alpha
        })
    
    df = pd.DataFrame(comparison)
    df = df.set_index('策略')
    
    return df


# ================= 测试入口 =================
if __name__ == "__main__":
    print("=" * 70)
    print("向量化回测引擎测试 - 第二阶段")
    print("=" * 70)
    
    # 配置回测参数
    config = BacktestConfig(
        commission_rate=0.001,    # 0.1% 手续费
        slippage_rate=0.0005,     # 0.05% 滑点
        risk_free_rate=0.02       # 2% 无风险利率
    )
    
    # 测试1: 单策略回测
    print("\n" + "=" * 50)
    print("测试1: 趋势策略 (GOLD) 回测")
    print("=" * 50)
    
    try:
        backtester, results = backtest_beta_strategy('GOLD', config=config)
        print(backtester.summary())
        
        # 显示最近10天结果
        print("\n最近10天回测数据:")
        print(results[['price', 'signal', 'position', 'strategy_return', 
                      'cumulative_strategy', 'drawdown']].tail(10).to_string())
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试2: 全策略回测对比
    print("\n" + "=" * 50)
    print("测试2: 全策略回测对比")
    print("=" * 50)
    
    try:
        all_backtesters = backtest_all_strategies(config)
        
        if all_backtesters:
            comparison = compare_strategies(all_backtesters)
            print("\n策略对比:")
            print(comparison.to_string())
    except Exception as e:
        print(f"错误: {e}")
    
    print("\n" + "=" * 70)
    print("回测引擎测试完成!")
    print("=" * 70)
