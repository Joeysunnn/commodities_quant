"""
金属详情页 (Metal Analysis View)
================================
展示单个金属的完整分析：
1. 价格走势
2. 全球总库存分位走势
3. 分交易所库存分位对比（柱状图 + 走势图）
4. 全球库存结构堆叠图
5. 差异化深度分析（按金属类型不同展示不同图表）
"""

import streamlit as st
import sys
from pathlib import Path

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT_DIR))

from factors import (
    calculate_global_percentile,
    calculate_regional_percentiles,
    calculate_source_percentile_trend,
    get_price_data,
    METAL_CONFIG,
    # 铜衍生因子
    get_lme_cancelled_ratio,
    get_lme_flow_analysis,
    get_comex_structure_copper,
    get_price_vs_open_interest,
    # 黄金衍生因子
    get_gld_fund_flows,
    get_comex_free_vs_pledged,
    get_lbma_vs_comex_gold,
    # 白银衍生因子
    get_slv_vs_comex_squeeze,
    get_comex_structure_silver,
    get_lbma_flows_silver,
)
from utils import (
    plot_percentile_trend,
    plot_regional_bar,
    plot_price_trend,
    plot_inventory_stacked,
    plot_multi_source_percentile,
    # 复合图表模板
    plot_combo_ratio_price,
    plot_flow_bar,
    plot_stacked_area_structure,
    plot_dual_axis_lines,
    plot_fund_flows_bar,
    plot_normalized_area,
    plot_squeeze_divergence,
    THEME
)
from views.ui_text import bilingual_chart_title, bilingual_page_title, bilingual_section

# 金属显示名称
METAL_DISPLAY = {
    'COPPER': {'english': 'Copper', 'chinese': '铜', 'emoji': '🟤', 'unit': 'mt'},
    'GOLD': {'english': 'Gold', 'chinese': '金', 'emoji': '🟡', 'unit': 'oz'},
    'SILVER': {'english': 'Silver', 'chinese': '银', 'emoji': '⚪', 'unit': 'oz'},
}


def show(metal_name: str):
    """
    显示金属详情页
    
    Args:
        metal_name: 金属名称 (COPPER/GOLD/SILVER)
    """
    # 获取金属配置
    metal_info = METAL_DISPLAY.get(metal_name, {'english': metal_name.title(), 'chinese': metal_name, 'emoji': '🔘', 'unit': 'mt'})
    config = METAL_CONFIG.get(metal_name, {})
    sources = list(config.get('sources', {}).keys())
    unit = metal_info['unit']
    
    # 页面标题
    bilingual_page_title(f"{metal_info['english']} Deep Dive", f"{metal_info['chinese']}深度分析", metal_info['emoji'])
    st.markdown("---")
    
    # ===================== 数据加载 =====================
    with st.spinner("Loading data... / 正在加载数据..."):
        try:
            # 加载所有需要的数据
            global_pct_df = calculate_global_percentile(metal_name)
            regional_df = calculate_regional_percentiles(metal_name)
            price_df = get_price_data(metal_name)
            
            # 加载各来源的分位数走势
            source_trends = {}
            for source in sources:
                try:
                    source_trends[source] = calculate_source_percentile_trend(metal_name, source)
                except Exception as e:
                    st.warning(f"Failed to load {source} data / 加载 {source} 数据时出错: {e}")
                    
        except Exception as e:
            st.error(f"Data loading failed / 数据加载失败: {e}")
            return
    
    # ===================== 第一行：价格 + 全球分位 =====================
    bilingual_section("Price And Inventory Percentile Overview", "价格与库存分位概览", "📈")
    col1, col2 = st.columns(2)
    
    with col1:
        # 图表1: 价格走势
        bilingual_chart_title("1", "Price Trend", "价格走势")
        if not price_df.empty:
            fig_price = plot_price_trend(
                price_df, 
                title="",  # 标题已在上方
                metal=metal_name,
                height=350
            )
            st.plotly_chart(fig_price, use_container_width=True)
        else:
            st.info("No price data yet. / 暂无价格数据")
    
    with col2:
        # 图表2: 全球总库存分位走势
        bilingual_chart_title("2", "Global Inventory Percentile", "全球总库存分位")
        if not global_pct_df.empty:
            fig_global = plot_percentile_trend(
                global_pct_df,
                title="",
                metal=metal_name,
                height=350
            )
            st.plotly_chart(fig_global, use_container_width=True)
        else:
            st.info("No inventory percentile data yet. / 暂无库存分位数据")
    
    st.markdown("---")
    
    # ===================== 第二行：分交易所分位 =====================
    bilingual_section("Exchange Inventory Percentile Analysis", "分交易所库存分位分析", "🏛️")
    col3, col4 = st.columns(2)
    
    with col3:
        # 图表3: 分交易所当前分位柱状图
        bilingual_chart_title("3", "Current Percentile By Exchange", "当前分位对比")
        if not regional_df.empty:
            fig_bar = plot_regional_bar(
                regional_df,
                title="",
                height=350
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No regional percentile data yet. / 暂无区域分位数据")
    
    with col4:
        # 图表3b: 分交易所分位走势对比
        bilingual_chart_title("3b", "Percentile Trend By Exchange", "分位走势对比")
        if source_trends:
            fig_multi = plot_multi_source_percentile(
                source_trends,
                title="",
                height=350
            )
            st.plotly_chart(fig_multi, use_container_width=True)
        else:
            st.info("No exchange trend data yet. / 暂无分交易所走势数据")
    
    st.markdown("---")
    
    # ===================== 第三行：绝对库存量 =====================
    bilingual_section("Absolute Inventory Analysis", "绝对库存量分析", "📦")
    
    # 图表4: 全球库存结构堆叠图
    bilingual_chart_title("4", "Global Inventory Structure", "全球库存结构")
    if not global_pct_df.empty:
        fig_stacked = plot_inventory_stacked(
            global_pct_df,
            source_cols=sources,
            title="",
            unit=unit,
            height=400
        )
        st.plotly_chart(fig_stacked, use_container_width=True)
    else:
        st.info("No inventory structure data yet. / 暂无库存结构数据")
    
    st.markdown("---")
    
    # ===================== 差异化深度分析 =====================
    if metal_name == 'COPPER':
        _render_copper_deep_analysis()
    elif metal_name == 'GOLD':
        _render_gold_deep_analysis()
    elif metal_name == 'SILVER':
        _render_silver_deep_analysis()
    
    st.markdown("---")
    
    # ===================== 详细数据表格（可折叠） =====================
    with st.expander("📋 Detailed Data Tables / 查看详细数据表格"):
        tab1, tab2, tab3 = st.tabs(["Global Inventory / 全球库存", "Regional Percentile / 区域分位", "Price Data / 价格数据"])
        
        with tab1:
            if not global_pct_df.empty:
                st.dataframe(
                    global_pct_df.tail(20).style.format({
                        'percentile': '{:.1%}',
                        'total': '{:,.0f}',
                        **{s: '{:,.0f}' for s in sources}
                    }),
                    use_container_width=True
                )
            else:
                st.info("No data yet. / 暂无数据")
        
        with tab2:
            if not regional_df.empty:
                st.dataframe(
                    regional_df.style.format({
                        'percentile': '{:.1%}',
                        'current_value': '{:,.0f}'
                    }),
                    use_container_width=True
                )
            else:
                st.info("No data yet. / 暂无数据")
        
        with tab3:
            if not price_df.empty:
                st.dataframe(
                    price_df.tail(20).style.format({
                        'price': '${:,.2f}'
                    }),
                    use_container_width=True
                )
            else:
                st.info("No data yet. / 暂无数据")
    
    # ===================== 底部说明 =====================
    st.markdown("---")
    st.caption(f"""
    📊 **Data Notes / 数据说明**:
    - Percentiles use a rolling 3-year window.
    - Data sources / 数据来源: {', '.join(sources)}
    - Frequency / 频率: {'Daily / 日度' if config.get('freq') == 'D' else 'Weekly / 周度'}
    - Unit / 单位: {unit.upper()}
    """)


# ================= 独立测试入口 =================
if __name__ == "__main__":
    print("请使用 streamlit run app.py 访问完整应用")


# ===================== 铜 - 差异化深度分析 =====================
def _render_copper_deep_analysis():
    """铜的专属深度分析图表"""
    bilingual_section("Copper Deep Dive", "铜 - 深度分析", "🔬")
    
    # Row 3: LME 深度
    bilingual_section("LME Market Microstructure", "LME 市场微观结构")
    col1, col2 = st.columns(2)
    
    with col1:
        # 图表5: LME 库存流动分析
        bilingual_chart_title("5", "LME Inventory Flow", "LME 库存流动")
        st.caption("Inventory inflow spike = oversupply/bearish; outflow spike = strong demand/bullish. / 入库暴增=供给过剩(看空)；出库暴增=需求强劲(看多)")
        try:
            df_flow = get_lme_flow_analysis()
            if not df_flow.empty:
                fig = plot_flow_bar(
                    df_flow,
                    title="",
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No LME flow data yet. / 暂无LME流动数据")
        except Exception as e:
            st.warning(f"Failed to load LME flow data / 加载LME流动数据失败: {e}")
    
    with col2:
        # 图表6: LME 注销仓单占比
        bilingual_chart_title("6", "Cancelled Warrant Ratio", "LME 注销仓单占比")
        st.caption("A 40-50%+ ratio can be a leading sign of future inventory outflow. / 占比超过40-50%可能是库存即将流出的先行指标")
        try:
            df_cancelled = get_lme_cancelled_ratio()
            if not df_cancelled.empty:
                fig = plot_combo_ratio_price(
                    df_cancelled,
                    ratio_col='ratio',
                    title="",
                    ratio_name='Cancelled Ratio / 注销占比',
                    height=350,
                    ratio_threshold=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No cancelled warrant data yet. / 暂无注销仓单数据")
        except Exception as e:
            st.warning(f"Failed to load cancelled warrant data / 加载注销仓单数据失败: {e}")
    
    # Row 4: 结构与资金
    bilingual_section("Inventory Structure And Fund Flows", "库存结构与资金流向")
    col3, col4 = st.columns(2)
    
    with col3:
        # 图表7: COMEX 库存结构
        bilingual_chart_title("7", "COMEX Inventory Structure", "COMEX 库存结构")
        st.caption("Very low registered inventory can create short squeeze pressure. / Registered 极低时空头易被逼仓")
        try:
            df_structure = get_comex_structure_copper()
            if not df_structure.empty:
                fig = plot_stacked_area_structure(
                    df_structure,
                    bottom_col='eligible',
                    top_col='registered',
                    title="",
                    height=350,
                    unit='mt',
                    bottom_name='Eligible / 非活性',
                    top_name='Registered / 可交割',
                    top_color='#B87333'  # 铜色
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # 显示关键指标
                latest_ratio = df_structure['reg_ratio'].iloc[-1]
                st.metric("Registered Ratio / 当前 Registered 占比", f"{latest_ratio:.1%}")
            else:
                st.info("No COMEX structure data yet. / 暂无COMEX结构数据")
        except Exception as e:
            st.warning(f"Failed to load COMEX structure data / 加载COMEX结构数据失败: {e}")
    
    with col4:
        # 图表8: 价格与持仓量
        bilingual_chart_title("8", "Price Vs Open Interest", "价格与持仓量")
        st.caption("Same direction = healthier trend; divergence = weaker momentum. / 同向=健康趋势；背离=动力不足")
        try:
            df_oi = get_price_vs_open_interest()
            if not df_oi.empty:
                fig = plot_dual_axis_lines(
                    df_oi,
                    y1_col='price',
                    y2_col='open_interest',
                    title="",
                    height=350,
                    y1_name='Price / 价格',
                    y2_name='Open Interest / 持仓量',
                    y1_unit='USD',
                    y2_unit='mt'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No open interest data yet. / 暂无持仓量数据")
        except Exception as e:
            st.warning(f"Failed to load open interest data / 加载持仓量数据失败: {e}")


# ===================== 黄金 - 差异化深度分析 =====================
def _render_gold_deep_analysis():
    """黄金的专属深度分析图表"""
    bilingual_section("Gold Deep Dive", "黄金 - 深度分析", "🔬")
    
    # Row 3: 投资情绪
    bilingual_section("Investment Sentiment And Fund Flows", "投资情绪与资金流向")
    col1, col2 = st.columns(2)
    
    with col1:
        # 图表5: GLD ETF 资金流向
        bilingual_chart_title("5", "GLD ETF Fund Flows Vs Price", "GLD ETF 资金流向")
        st.caption("Price up + holdings up = healthy move; price up + holdings down = bullish trap divergence. / 价涨+持仓增=健康；价涨+持仓减=诱多背离")
        try:
            df_gld = get_gld_fund_flows()
            if not df_gld.empty:
                fig = plot_fund_flows_bar(
                    df_gld,
                    change_col='holdings_change',
                    title="",
                    height=350,
                    unit='oz'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No GLD fund flow data yet. / 暂无GLD资金流向数据")
        except Exception as e:
            st.warning(f"Failed to load GLD data / 加载GLD数据失败: {e}")
    
    with col2:
        # 图表6: LBMA vs COMEX 占比
        bilingual_chart_title("6", "Off-Exchange Vs Exchange Inventory", "场外 vs 场内库存")
        st.caption("LBMA drop + COMEX rise can indicate large EFP activity. / LBMA骤降+COMEX上升=大规模期现套利(EFP)")
        try:
            df_ratio = get_lbma_vs_comex_gold()
            if not df_ratio.empty:
                fig = plot_normalized_area(
                    df_ratio,
                    pct1_col='lbma_pct',
                    pct2_col='comex_pct',
                    title="",
                    height=350,
                    name1='LBMA',
                    name2='COMEX'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No LBMA/COMEX comparison data yet. / 暂无LBMA/COMEX对比数据")
        except Exception as e:
            st.warning(f"Failed to load LBMA/COMEX data / 加载LBMA/COMEX数据失败: {e}")
    
    # Row 4: 交易所压力 (单张大图)
    bilingual_section("Exchange Liquidity Pressure", "交易所流动性压力")
    
    # 图表7: COMEX Registered Breakdown
    bilingual_chart_title("7", "COMEX Real Liquidity", "COMEX 真实流动性")
    st.caption("Pledged = locked collateral; Free = deliverable inventory; Free near zero = severe liquidity stress. / Pledged=已质押锁定；Free=真正可交割；Free归零=严重流动性枯竭")
    try:
        df_pledged = get_comex_free_vs_pledged()
        if not df_pledged.empty:
            fig = plot_stacked_area_structure(
                df_pledged,
                bottom_col='pledged',
                top_col='free',
                title="",
                height=450,
                unit='oz',
                bottom_name='Pledged / 已质押',
                top_name='Free / 可交割',
                bottom_color='#999999',
                top_color='#FFD700'  # 金色
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # 显示关键指标
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                st.metric("Free Deliverable / Free 可交割", f"{df_pledged['free'].iloc[-1]:,.0f} oz")
            with col_m2:
                st.metric("Pledged / 已质押", f"{df_pledged['pledged'].iloc[-1]:,.0f} oz")
            with col_m3:
                st.metric("Free Ratio / Free 占比", f"{df_pledged['free_ratio'].iloc[-1]:.1%}")
        else:
            st.info("No COMEX pledged inventory data yet. / 暂无COMEX质押数据")
    except Exception as e:
        st.warning(f"Failed to load COMEX pledged inventory data / 加载COMEX质押数据失败: {e}")


# ===================== 白银 - 差异化深度分析 =====================
def _render_silver_deep_analysis():
    """白银的专属深度分析图表"""
    bilingual_section("Silver Deep Dive", "白银 - 深度分析", "🔬")
    
    # Row 3: 逼空监控 (灵魂图表，全宽)
    bilingual_section("Squeeze Monitor", "逼空监控")
    
    # 图表5: SLV vs COMEX Registered
    bilingual_chart_title("5", "SLV Vs COMEX Registered", "SLV vs COMEX Registered - 鳄鱼大开口")
    st.caption("SLV rising + COMEX registered falling = squeeze signal; a wider gap implies stronger pressure. / SLV飙升+COMEX骤降=逼空信号；剪刀差越大，爆发力越强")
    try:
        df_squeeze = get_slv_vs_comex_squeeze()
        if not df_squeeze.empty:
            fig = plot_squeeze_divergence(
                df_squeeze,
                y1_col='slv_holdings',
                y2_col='comex_registered',
                title="",
                height=450,
                y1_name='SLV Holdings',
                y2_name='COMEX Registered'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # 显示关键指标
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                st.metric("SLV Holdings", f"{df_squeeze['slv_holdings'].iloc[-1]/1e6:,.1f} M oz")
            with col_m2:
                st.metric("COMEX Registered", f"{df_squeeze['comex_registered'].iloc[-1]/1e6:,.1f} M oz")
        else:
            st.info("No SLV/COMEX data yet. / 暂无SLV/COMEX数据")
    except Exception as e:
        st.warning(f"Failed to load SLV/COMEX data / 加载SLV/COMEX数据失败: {e}")
    
    # Row 4: 结构与工业
    bilingual_section("Inventory Structure And Industrial Demand", "库存结构与工业需求")
    col1, col2 = st.columns(2)
    
    with col1:
        # 图表6: COMEX 库存结构
        bilingual_chart_title("6", "COMEX Inventory Structure", "COMEX 库存结构")
        st.caption("Silver eligible inventory is usually higher; Reg/Total below 20% means fragile structure. / 白银 Eligible 占比通常更高；Reg/Total<20%=结构脆弱")
        try:
            df_structure = get_comex_structure_silver()
            if not df_structure.empty:
                fig = plot_stacked_area_structure(
                    df_structure,
                    bottom_col='eligible',
                    top_col='registered',
                    title="",
                    height=350,
                    unit='oz',
                    bottom_name='Eligible / 沉睡',
                    top_name='Registered / 活跃',
                    top_color='#C0C0C0'  # 银色
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # 显示关键指标
                latest_ratio = df_structure['reg_ratio'].iloc[-1]
                color = "inverse" if latest_ratio < 0.2 else "normal"
                st.metric("Registered Ratio / 当前 Registered 占比", f"{latest_ratio:.1%}",
                         delta="⚠️ Below 20% alert / 低于20%警戒" if latest_ratio < 0.2 else None,
                         delta_color=color)
            else:
                st.info("No COMEX structure data yet. / 暂无COMEX结构数据")
        except Exception as e:
            st.warning(f"Failed to load COMEX structure data / 加载COMEX结构数据失败: {e}")
    
    with col2:
        # 图表7: LBMA 巨鲸流向
        bilingual_chart_title("7", "LBMA Net Flows Vs Price", "LBMA 巨鲸流向")
        st.caption("LBMA represents deep industrial inventory; price down + large outflow can be bullish divergence. / LBMA=工业深水区；价跌但巨额流出=工业抄底(背离看涨)")
        try:
            df_lbma = get_lbma_flows_silver()
            if not df_lbma.empty:
                fig = plot_fund_flows_bar(
                    df_lbma,
                    change_col='holdings_change',
                    title="",
                    height=350,
                    unit='oz'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No LBMA flow data yet. / 暂无LBMA流向数据")
        except Exception as e:
            st.warning(f"Failed to load LBMA flow data / 加载LBMA流向数据失败: {e}")
