"""
宏观仪表盘页面 (Macro Dashboard View)
=====================================
首页概览：
1. 三金属多空信号灯
2. 全球库存热力图
3. 快速导航
"""

import streamlit as st
import sys
from pathlib import Path

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from factors import (
    get_dashboard_signals,
    get_heatmap_data,
    calculate_global_percentile
)
from utils import (
    plot_heatmap,
    plot_percentile_trend,
    create_signal_card_html,
    THEME
)


def get_signal_display(percentile: float) -> tuple:
    """
    根据分位值返回信号文本、颜色和emoji
    
    Args:
        percentile: 分位数 (0-1)
    
    Returns:
        tuple: (信号文本, 颜色, emoji)
    """
    if percentile <= 0.05:
        return "强看多 (Strong Buy)", "#00C853", "🟢"
    elif percentile <= 0.10:
        return "看多 (Buy)", "#69F0AE", "🟢"
    elif percentile >= 0.95:
        return "强看空 (Strong Sell)", "#D50000", "🔴"
    elif percentile >= 0.90:
        return "看空 (Sell)", "#FF5252", "🔴"
    else:
        return "中性 (Neutral)", "#9E9E9E", "⚪"


def render_signal_card(metal: str, percentile: float, signal: str, color: str):
    """
    渲染信号卡片
    """
    metal_display = {
        'COPPER': ('🟤 铜', 'Copper'),
        'GOLD': ('🟡 金', 'Gold'),
        'SILVER': ('⚪ 银', 'Silver')
    }.get(metal, (metal, metal))
    
    signal_text, signal_color, emoji = get_signal_display(percentile)
    
    # 根据分位数确定背景渐变
    if percentile <= 0.10:
        bg_gradient = "linear-gradient(135deg, #E8F5E9 0%, #C8E6C9 100%)"
    elif percentile >= 0.90:
        bg_gradient = "linear-gradient(135deg, #FFEBEE 0%, #FFCDD2 100%)"
    else:
        bg_gradient = "linear-gradient(135deg, #F5F5F5 0%, #E0E0E0 100%)"
    
    st.markdown(f"""
    <div style="
        background: {bg_gradient};
        border-radius: 15px;
        padding: 25px;
        text-align: center;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border-left: 5px solid {signal_color};
        margin: 5px;
    ">
        <h3 style="margin: 0 0 10px 0; color: #333; font-size: 1.3rem;">
            {metal_display[0]}
        </h3>
        <p style="margin: 0; color: #666; font-size: 0.9rem;">{metal_display[1]}</p>
        <h2 style="margin: 15px 0; color: {signal_color}; font-size: 1.5rem;">
            {emoji} {signal_text}
        </h2>
        <div style="
            background: white;
            border-radius: 10px;
            padding: 10px;
            margin-top: 10px;
        ">
            <p style="margin: 0; color: #666; font-size: 0.85rem;">全球库存分位</p>
            <p style="margin: 5px 0 0 0; color: {signal_color}; font-size: 1.8rem; font-weight: bold;">
                {percentile:.1%}
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)


def show():
    """显示仪表盘页面"""
    
    # 页面标题
    st.markdown("""
    <h1 style="text-align: center; color: #1f77b4; margin-bottom: 0;">
        🌍 宏观库存仪表盘
    </h1>
    <p style="text-align: center; color: #666; font-size: 1.1rem; margin-top: 5px;">
        Macro Inventory Dashboard
    </p>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ===================== 加载数据 =====================
    with st.spinner("正在加载数据..."):
        try:
            signals = get_dashboard_signals()
            heatmap_data = get_heatmap_data()
        except Exception as e:
            st.error(f"数据加载失败: {e}")
            return
    
    # ===================== 第一部分：多空信号灯 =====================
    st.subheader("🚦 多空信号灯 (Bull/Bear Signals)")
    st.caption("基于全球库存3年滚动分位数 | Based on 3-Year Rolling Percentile")
    
    col1, col2, col3 = st.columns(3)
    
    metals = ['COPPER', 'GOLD', 'SILVER']
    cols = [col1, col2, col3]
    
    for metal, col in zip(metals, cols):
        with col:
            info = signals.get(metal, {'percentile': 0.5, 'signal': '数据缺失', 'color': 'gray'})
            render_signal_card(
                metal=metal,
                percentile=info['percentile'],
                signal=info['signal'],
                color=info['color']
            )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # ===================== 第二部分：信号解读 =====================
    with st.expander("📖 信号解读说明", expanded=False):
        st.markdown("""
        | 信号 | 分位数范围 | 含义 | 操作建议 |
        |------|-----------|------|----------|
        | 🟢 **强看多** | < 5% | 库存处于历史极低位，供应紧张 | 考虑做多 |
        | 🟢 看多 | 5% - 10% | 库存偏低 | 偏多思路 |
        | ⚪ 中性 | 10% - 90% | 库存正常区间 | 观望或根据趋势操作 |
        | 🔴 看空 | 90% - 95% | 库存偏高 | 偏空思路 |
        | 🔴 **强看空** | > 95% | 库存处于历史极高位，供应过剩 | 考虑做空 |
        
        > ⚠️ **注意**：此信号仅基于库存分位数，实际交易需结合价格趋势、基本面等多因素分析。
        """)
    
    st.markdown("---")
    
    # ===================== 第三部分：热力图 =====================
    st.subheader("🔥 全球库存压力热力图 (Inventory Pressure Heatmap)")
    st.caption("行 = 金属 | 列 = 交易所/数据源 | 颜色 = 分位数 (绿低红高)")
    
    if not heatmap_data.empty:
        fig_heatmap = plot_heatmap(
            heatmap_data,
            title="",
            height=300
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("暂无热力图数据")
    
    st.markdown("---")
    
    # ===================== 第四部分：迷你趋势图 =====================
    st.subheader("📈 近期分位走势 (Recent Percentile Trends)")
    
    col_c, col_g, col_s = st.columns(3)
    
    with col_c:
        st.markdown("##### 🟤 铜 (Copper)")
        try:
            copper_data = calculate_global_percentile('COPPER')
            if not copper_data.empty:
                # 只取最近30个数据点作为迷你图
                mini_data = copper_data.tail(30)
                fig = plot_percentile_trend(mini_data, title="", metal='COPPER', height=200)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据")
        except Exception as e:
            st.warning(f"加载失败: {e}")
    
    with col_g:
        st.markdown("##### 🟡 金 (Gold)")
        try:
            gold_data = calculate_global_percentile('GOLD')
            if not gold_data.empty:
                mini_data = gold_data.tail(60)
                fig = plot_percentile_trend(mini_data, title="", metal='GOLD', height=200)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据")
        except Exception as e:
            st.warning(f"加载失败: {e}")
    
    with col_s:
        st.markdown("##### ⚪ 银 (Silver)")
        try:
            silver_data = calculate_global_percentile('SILVER')
            if not silver_data.empty:
                mini_data = silver_data.tail(60)
                fig = plot_percentile_trend(mini_data, title="", metal='SILVER', height=200)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("暂无数据")
        except Exception as e:
            st.warning(f"加载失败: {e}")
    
    st.markdown("---")
    
    # ===================== 第五部分：快速导航 =====================
    st.subheader("🔗 快速导航 (Quick Links)")
    
    nav_col1, nav_col2, nav_col3, nav_col4 = st.columns(4)
    
    with nav_col1:
        st.markdown("""
        <div style="background: #EFEBE9; border-radius: 10px; padding: 15px; text-align: center;">
            <h4 style="margin: 0;">🟤 铜分析</h4>
            <p style="color: #666; font-size: 0.9rem;">Copper Analysis</p>
        </div>
        """, unsafe_allow_html=True)
    
    with nav_col2:
        st.markdown("""
        <div style="background: #FFF8E1; border-radius: 10px; padding: 15px; text-align: center;">
            <h4 style="margin: 0;">🟡 金分析</h4>
            <p style="color: #666; font-size: 0.9rem;">Gold Analysis</p>
        </div>
        """, unsafe_allow_html=True)
    
    with nav_col3:
        st.markdown("""
        <div style="background: #ECEFF1; border-radius: 10px; padding: 15px; text-align: center;">
            <h4 style="margin: 0;">⚪ 银分析</h4>
            <p style="color: #666; font-size: 0.9rem;">Silver Analysis</p>
        </div>
        """, unsafe_allow_html=True)
    
    with nav_col4:
        st.markdown("""
        <div style="background: #E3F2FD; border-radius: 10px; padding: 15px; text-align: center;">
            <h4 style="margin: 0;">📈 策略回测</h4>
            <p style="color: #666; font-size: 0.9rem;">Backtest (开发中)</p>
        </div>
        """, unsafe_allow_html=True)
    
    # 底部说明
    st.markdown("---")
    st.caption("""
    📊 **数据说明**：分位数基于2021年至今的数据，使用3年滚动窗口计算。
    数据来源包括 LME、COMEX、SHFE、LBMA、GLD ETF、SLV ETF。
    """)