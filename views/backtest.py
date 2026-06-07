"""
策略回测页面 (Backtest View)
============================
功能：
1. 基于库存分位的多空策略回测
2. 收益曲线展示
3. 风险指标计算
4. 多策略对比
"""

import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from backtest_engine import (
    VectorBacktester, BacktestConfig,
    backtest_beta_strategy, backtest_arbitrage_strategy, backtest_event_strategy,
    backtest_all_strategies, compare_strategies
)
from strategy import (
    BetaStrategy, ArbitrageStrategy, EventStrategy,
    BetaStrategyParams, ArbitrageStrategyParams, EventStrategyParams,
    Signal
)
from views.ui_text import bilingual_page_title, bilingual_section


# 金属配置
METAL_OPTIONS = {
    'COPPER': {'name': '🟤 Copper / 铜', 'unit': 'mt'},
    'GOLD': {'name': '🟡 Gold / 金', 'unit': 'oz'},
    'SILVER': {'name': '⚪ Silver / 银', 'unit': 'oz'},
}

STRATEGY_OPTIONS = {
    'Beta': '📈 Beta Trend Strategy / 趋势策略 - Inventory percentile',
    'Arbitrage': '🔄 Arbitrage Strategy / 套利策略 - Exchange spread',
    'Event': '⚡ Event Strategy / 事件驱动 - Silver squeeze monitor',
}


def show():
    """显示回测页面"""
    
    bilingual_page_title("Strategy Lab", "策略实验室", "🔬")
    st.markdown("---")
    
    # ===================== 侧边栏：策略配置 =====================
    with st.sidebar:
        bilingual_section("Strategy Configuration", "策略配置", "⚙️")
        
        # 1. 选择策略类型
        strategy_type = st.selectbox(
            "Strategy type / 策略类型",
            options=list(STRATEGY_OPTIONS.keys()),
            format_func=lambda x: STRATEGY_OPTIONS[x],
            index=0
        )
        
        st.markdown("---")
        
        # 2. 根据策略类型显示不同的参数
        if strategy_type == 'Beta':
            bilingual_section("Beta Parameters", "趋势策略参数", "📊")
            
            metal = st.selectbox(
                "Backtest metal / 回测金属",
                options=list(METAL_OPTIONS.keys()),
                format_func=lambda x: METAL_OPTIONS[x]['name'],
                index=1  # 默认选金
            )
            
            st.markdown("##### Long Conditions / 做多条件")
            long_entry_pct = st.slider(
                "Long entry / 做多入场 (percentile <)",
                min_value=1, max_value=30, value=5, step=1,
                format="%d%%",
                help="Enter long positions when inventory percentile is below this level. / 当库存分位低于此值时做多"
            )
            long_exit_pct = st.slider(
                "Long exit / 做多平仓 (percentile >)",
                min_value=20, max_value=60, value=30, step=1,
                format="%d%%",
                help="Exit long positions when inventory percentile rises above this level. / 当库存分位高于此值时平多仓"
            )
            
            st.markdown("##### Short Conditions / 做空条件")
            short_entry_pct = st.slider(
                "Short entry / 做空入场 (percentile >)",
                min_value=70, max_value=99, value=95, step=1,
                format="%d%%",
                help="Enter short positions when inventory percentile is above this level. / 当库存分位高于此值时做空"
            )
            short_exit_pct = st.slider(
                "Short exit / 做空平仓 (percentile <)",
                min_value=50, max_value=85, value=70, step=1,
                format="%d%%",
                help="Exit short positions when inventory percentile falls below this level. / 当库存分位低于此值时平空仓"
            )
            
            # 转换为小数
            params = BetaStrategyParams(
                long_entry=long_entry_pct / 100,
                long_exit=long_exit_pct / 100,
                short_entry=short_entry_pct / 100,
                short_exit=short_exit_pct / 100
            )
            
        elif strategy_type == 'Arbitrage':
            bilingual_section("Arbitrage Parameters", "套利策略参数", "🔄")
            
            metal = 'COPPER'  # 套利策略仅支持铜
            st.info("COMEX-LME spread strategy; copper only. / 套利策略基于 COMEX-LME 价差，仅适用于铜")
            
            st.markdown("##### Spread Thresholds / 价差阈值")
            spread_long_pct = st.slider(
                "Long spread / 做多价差 (COMEX-LME <)",
                min_value=-40, max_value=-5, value=-20, step=1,
                format="%d%%",
                help="Long the spread when COMEX is relatively tight. / COMEX紧缺时做多价差"
            )
            spread_short_pct = st.slider(
                "Short spread / 做空价差 (COMEX-LME >)",
                min_value=5, max_value=40, value=20, step=1,
                format="%d%%",
                help="Short the spread when COMEX is relatively abundant. / COMEX充裕时做空价差"
            )
            
            # 转换为小数
            params = ArbitrageStrategyParams(
                spread_long_entry=spread_long_pct / 100,
                spread_short_entry=spread_short_pct / 100
            )
            
        else:  # Event
            bilingual_section("Event Parameters", "事件策略参数", "⚡")
            
            metal = 'SILVER'  # 事件策略仅支持白银
            st.info("Squeeze monitor strategy; silver only. / 逼空监控策略仅适用于白银")
            
            st.markdown("##### Divergence Thresholds / 背离度阈值")
            div_entry = st.slider(
                "Long entry / 做多入场 (divergence >)",
                min_value=0.5, max_value=3.0, value=1.5, step=0.1,
                format="%.1fσ",
                help="Enter long when SLV-COMEX divergence is above this level. / SLV与COMEX背离度超过此值时做多"
            )
            div_exit = st.slider(
                "Long exit / 做多平仓 (divergence <)",
                min_value=0.0, max_value=1.5, value=0.5, step=0.1,
                format="%.1fσ",
                help="Exit when divergence normalizes. / 背离度回落时平仓"
            )
            
            params = EventStrategyParams(
                divergence_long_entry=div_entry,
                divergence_long_exit=div_exit
            )
        
        st.markdown("---")
        
        # 3. 交易成本配置
        bilingual_section("Trading Costs", "交易成本", "💰")
        commission_bps = st.slider(
            "Commission / 手续费率",
            min_value=0, max_value=50, value=10, step=1,
            format="%d bps",
            help="One-way commission. 1 bps = 0.01%. / 单边手续费"
        )
        slippage_bps = st.slider(
            "Slippage / 滑点",
            min_value=0, max_value=30, value=5, step=1,
            format="%d bps",
            help="Estimated slippage cost. 1 bps = 0.01%. / 预估滑点成本"
        )
        
        # 转换为小数 (bps -> decimal)
        commission = commission_bps / 10000
        slippage = slippage_bps / 10000
        
        config = BacktestConfig(
            commission_rate=commission,
            slippage_rate=slippage,
            risk_free_rate=0.02
        )
        
        st.markdown("---")
        
        # 4. 运行回测按钮
        run_backtest = st.button("🚀 Run Backtest / 开始回测", type="primary", use_container_width=True)
        run_compare = st.button("📊 Compare All Strategies / 全策略对比", use_container_width=True)
    
    # ===================== 主区域：回测结果 =====================
    
    if run_backtest:
        with st.spinner("Running backtest... / 正在执行回测..."):
            try:
                # 根据策略类型执行回测
                if strategy_type == 'Beta':
                    strategy = BetaStrategy(metal, params)
                    backtester = VectorBacktester(config)
                    results = backtester.run_strategy(strategy)
                    strategy_name = f"Beta Trend Strategy - {METAL_OPTIONS[metal]['name']}"
                    
                elif strategy_type == 'Arbitrage':
                    strategy = ArbitrageStrategy(metal, params)
                    backtester = VectorBacktester(config)
                    results = backtester.run_strategy(strategy)
                    strategy_name = "Arbitrage Strategy - Copper COMEX/LME"
                    
                else:  # Event
                    strategy = EventStrategy(params)
                    backtester = VectorBacktester(config)
                    results = backtester.run_strategy(strategy)
                    strategy_name = "Event Strategy - Silver Squeeze"
                
                # 获取绩效指标
                metrics = backtester.get_metrics()
                
                # ===== 显示结果 =====
                st.success(f"Backtest completed / 回测完成: {strategy_name}")
                
                # 核心指标卡片
                bilingual_section("Core Performance Metrics", "核心绩效指标", "📊")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    delta_color = "normal" if metrics.total_return >= 0 else "inverse"
                    st.metric(
                        "Total Return / 总收益率",
                        f"{metrics.total_return:.2%}",
                        delta=f"vs Benchmark / 基准 {metrics.alpha:+.2%}",
                        delta_color=delta_color
                    )
                
                with col2:
                    st.metric(
                        "CAGR / 年化收益",
                        f"{metrics.cagr:.2%}"
                    )
                
                with col3:
                    sharpe_color = "normal" if metrics.sharpe_ratio >= 0 else "inverse"
                    st.metric(
                        "Sharpe Ratio / 夏普比率",
                        f"{metrics.sharpe_ratio:.2f}",
                        delta_color=sharpe_color
                    )
                
                with col4:
                    st.metric(
                        "Max Drawdown / 最大回撤",
                        f"{metrics.max_drawdown:.2%}"
                    )
                
                # 第二行指标
                col5, col6, col7, col8 = st.columns(4)
                
                with col5:
                    st.metric("Volatility / 年化波动率", f"{metrics.volatility:.2%}")
                
                with col6:
                    st.metric("Sortino Ratio / 索提诺比率", f"{metrics.sortino_ratio:.2f}")
                
                with col7:
                    st.metric("Win Rate / 胜率", f"{metrics.win_rate:.1%}")
                
                with col8:
                    st.metric("Trades / 交易次数", f"{metrics.total_trades}")
                
                st.markdown("---")
                
                # 净值曲线
                bilingual_section("Equity Curve And Position Signals", "净值曲线与持仓信号", "📈")
                fig_equity = backtester.plot_equity_curve(title=strategy_name)
                st.plotly_chart(fig_equity, use_container_width=True)
                
                # 月度收益热力图
                bilingual_section("Monthly Return Heatmap", "月度收益热力图", "📅")
                try:
                    fig_monthly = backtester.plot_monthly_returns()
                    st.plotly_chart(fig_monthly, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not generate monthly heatmap / 无法生成月度热力图: {e}")
                
                st.markdown("---")
                
                # 交易信号历史
                bilingual_section("Trading Signal History", "交易信号历史", "📋")
                
                # 显示信号变化点
                signal_changes = results[results['position'].diff() != 0].copy()
                if not signal_changes.empty:
                    signal_changes['Signal / 信号'] = signal_changes['position'].map({
                        1: '🟢 Long / 做多', 0: '⚪ Flat / 平仓', -1: '🔴 Short / 做空'
                    })
                    signal_changes['Price / 价格'] = signal_changes['price']
                    
                    display_cols = ['Signal / 信号', 'Price / 价格']
                    if 'cumulative_strategy' in signal_changes.columns:
                        signal_changes['Equity / 净值'] = signal_changes['cumulative_strategy']
                        display_cols.append('Equity / 净值')
                    
                    st.dataframe(
                        signal_changes[display_cols].tail(20).style.format({
                            'Price / 价格': '${:,.2f}',
                            'Equity / 净值': '{:.4f}'
                        }),
                        use_container_width=True
                    )
                else:
                    st.info("No signal changes. / 无交易信号变化")
                
                # 详细数据（可折叠）
                with st.expander("📊 Full Backtest Data / 查看完整回测数据"):
                    st.dataframe(
                        results[['price', 'signal', 'position', 'market_return', 
                                'strategy_return', 'cumulative_strategy', 'drawdown']].tail(50).style.format({
                            'price': '${:,.2f}',
                            'market_return': '{:.4%}',
                            'strategy_return': '{:.4%}',
                            'cumulative_strategy': '{:.4f}',
                            'drawdown': '{:.2%}'
                        }),
                        use_container_width=True
                    )
                
            except Exception as e:
                st.error(f"❌ Backtest failed / 回测失败: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    elif run_compare:
        # 全策略对比
        bilingual_section("All Strategy Backtest Comparison", "全策略回测对比", "📊")
        
        with st.spinner("Running all strategies... / 正在回测所有策略..."):
            try:
                all_backtesters = backtest_all_strategies(config)
                
                if all_backtesters:
                    comparison = compare_strategies(all_backtesters)
                    comparison = comparison.rename(columns={
                        '总收益': 'Total Return / 总收益',
                        '年化收益': 'CAGR / 年化收益',
                        '夏普比率': 'Sharpe Ratio / 夏普比率',
                        '最大回撤': 'Max Drawdown / 最大回撤',
                        '胜率': 'Win Rate / 胜率',
                        '交易次数': 'Trades / 交易次数',
                    })
                    comparison.index.name = 'Strategy / 策略'
                    
                    # 格式化显示
                    styled_comparison = comparison.style.format({
                        'Total Return / 总收益': '{:.2%}',
                        'CAGR / 年化收益': '{:.2%}',
                        'Sharpe Ratio / 夏普比率': '{:.2f}',
                        'Max Drawdown / 最大回撤': '{:.2%}',
                        'Win Rate / 胜率': '{:.1%}',
                        'Alpha': '{:.2%}'
                    }).background_gradient(
                        subset=['Sharpe Ratio / 夏普比率'], cmap='RdYlGn'
                    ).background_gradient(
                        subset=['Max Drawdown / 最大回撤'], cmap='RdYlGn_r'
                    )
                    
                    st.dataframe(styled_comparison, use_container_width=True)
                    
                    # 绘制对比图
                    st.markdown("---")
                    bilingual_section("Equity Curve Comparison", "净值曲线对比", "📈")
                    
                    import plotly.graph_objects as go
                    fig = go.Figure()
                    
                    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3A506B']
                    
                    for i, (name, bt) in enumerate(all_backtesters.items()):
                        results = bt.results
                        fig.add_trace(go.Scatter(
                            x=results.index,
                            y=results['cumulative_strategy'],
                            name=name,
                            line=dict(color=colors[i % len(colors)], width=2)
                        ))
                    
                    fig.add_hline(y=1, line_dash="dash", line_color="gray",
                                  annotation_text="Initial Equity / 初始净值")
                    
                    fig.update_layout(
                        title="All Strategy Equity Curve Comparison / 全策略净值曲线对比",
                        xaxis_title="Date / 日期",
                        yaxis_title="Equity / 净值",
                        height=500,
                        hovermode='x unified',
                        legend=dict(orientation="h", yanchor="bottom", y=1.02)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                else:
                    st.warning("No backtest results available. / 没有可用的回测结果")
                    
            except Exception as e:
                st.error(f"❌ Strategy comparison failed / 全策略对比失败: {e}")
    
    else:
        # 默认显示说明
        st.info("Configure strategy parameters on the left, then click Run Backtest. / 请在左侧配置策略参数，然后点击「开始回测」")
        
        st.markdown("""
        ### 📖 Strategy Guide / 策略说明
        
        #### 1. Beta Trend Strategy / 趋势策略
        - **Logic / 逻辑**: Inventory is a contrarian price indicator; very low inventory can support prices.
        - **Long / 做多**: Global inventory percentile < 5%.
        - **Short / 做空**: Global inventory percentile > 95%.
        - **Applies to / 适用**: Copper, Gold, Silver / 铜、金、银.
        
        #### 2. Arbitrage Strategy / 套利策略
        - **Logic / 逻辑**: Trade supply-demand mismatches between COMEX and LME.
        - **Long spread / 做多价差**: COMEX percentile - LME percentile < -20%.
        - **Short spread / 做空价差**: COMEX percentile - LME percentile > 20%.
        - **Applies to / 适用**: Copper / 铜.
        
        #### 3. Event Strategy / 事件驱动
        - **Logic / 逻辑**: Monitor SLV vs COMEX divergence as a squeeze signal.
        - **Long / 做多**: Divergence > 1.5σ, with SLV rising and COMEX falling.
        - **Applies to / 适用**: Silver / 银.
        
        ---
        
        ### ⚠️ Risk Notice / 风险提示
        
        - Backtest results do not guarantee future performance. / 回测结果不代表未来表现。
        - Signals are shifted by one period to reduce look-ahead bias. / 信号已滞后一期，避免前视偏差。
        - Live trading must consider liquidity, margin, and execution constraints. / 实盘需考虑流动性、保证金等因素。
        """)
