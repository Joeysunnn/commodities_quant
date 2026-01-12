"""
商品量化分析系统 - 主入口
==========================
Commodity Quant System - Main Entry

功能：
1. 宏观仪表盘 - 多空信号灯 + 热力图
2. 金属详情页 - 铜/金/银深度分析
3. 策略回测 - (开发中)
4. PDF报告生成 - (开发中)
"""

import streamlit as st
from views import dashboard, metal_analysis, backtest

# ===================== 页面配置 =====================
st.set_page_config(
    page_title="商品库存量化系统 | Commodity Quant",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===================== 自定义CSS样式 =====================
st.markdown("""
<style>
    /* 侧边栏样式 */
    .css-1d391kg {
        padding-top: 1rem;
    }
    
    /* 主标题样式 */
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    
    /* 信号卡片样式 */
    .signal-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    
    /* 隐藏 Streamlit 默认的 hamburger menu 和 footer */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ===================== 左侧导航栏 =====================
st.sidebar.image("https://img.icons8.com/color/96/commodity.png", width=80)
st.sidebar.title("📊 商品库存分析")
st.sidebar.markdown("**Commodity Inventory Analysis**")
st.sidebar.markdown("---")

# 导航选项
PAGES = {
    "🌍 宏观仪表盘": "dashboard",
    "🟤 铜 (Copper)": "copper",
    "🟡 金 (Gold)": "gold",
    "⚪ 银 (Silver)": "silver",
    "📈 策略回测": "backtest"
}

page = st.sidebar.radio(
    "选择模块",
    options=list(PAGES.keys()),
    index=0
)

# ===================== 页面分发逻辑 =====================
selected_page = PAGES[page]

if selected_page == "dashboard":
    dashboard.show()
elif selected_page == "copper":
    metal_analysis.show("COPPER")
elif selected_page == "gold":
    metal_analysis.show("GOLD")
elif selected_page == "silver":
    metal_analysis.show("SILVER")
elif selected_page == "backtest":
    try:
        backtest.show()
    except Exception as e:
        st.warning("⚠️ 回测模块正在开发中...")
        st.info(f"错误信息: {e}")

# ===================== 侧边栏底部 =====================
st.sidebar.markdown("---")

# PDF 报告生成
st.sidebar.subheader("📄 报告生成")
report_type = st.sidebar.selectbox(
    "选择报告类型",
    ["每日投研日报", "每周市场周报", "月度分析报告"]
)

if st.sidebar.button("🖨️ 生成报告", use_container_width=True):
    st.sidebar.info("正在生成报告，请稍候...")
    # TODO: 调用报告生成函数
    st.sidebar.warning("报告生成功能开发中...")

# 数据更新状态
st.sidebar.markdown("---")
st.sidebar.caption("📅 数据更新时间")
st.sidebar.caption("COMEX: 2026-01-06")
st.sidebar.caption("LME: 2026-01-06")
st.sidebar.caption("SHFE: 2026-01-03")

# 版本信息
st.sidebar.markdown("---")
st.sidebar.caption("v1.0.0 | © 2026 Commodity Quant")