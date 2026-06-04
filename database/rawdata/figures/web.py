import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from common import add_range_selector as _add_range_selector, get_metal_data as load_metal_data, pivot_metric as _pivot_metric

# 页面配置
st.set_page_config(
    page_title="Metal Market Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 数据库连接
@st.cache_resource
def get_engine():
    raise RuntimeError("Use common.get_metal_data for database access.")


# 数据读取函数
@st.cache_data(ttl=3600)
def get_metal_data(metal_name):
    """读取指定金属的所有数据"""
    return load_metal_data(metal_name)

def pivot_metric(df, metric_name):
    """将指定 metric 转为时间序列"""
    return _pivot_metric(df, metric_name)

def add_range_selector(fig):
    """添加时间范围选择器"""
    return _add_range_selector(fig)


# ============================================================================
# GOLD 图表生成函数
# ============================================================================

def create_gold_g1(df):
    """G1: 金价"""
    spot_close = pivot_metric(df, 'price_futures_usd')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=spot_close.index, y=spot_close.values, name='Gold Futures Price',
                             line=dict(color='gold', width=2), mode='lines'))
    fig.update_layout(title='G1: Gold Futures Price (GC=F)', xaxis_title='Date', yaxis_title='Price (USD/oz)',
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_gold_g2(df):
    """G2: 三地库存"""
    lbma = pivot_metric(df, 'lbma_holdings_oz')
    comex = pivot_metric(df, 'comex_total_oz')
    gld = pivot_metric(df, 'gld_holdings_oz')
    
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                       subplot_titles=('LBMA Holdings (Monthly)', 'COMEX Total (Daily)', 'GLD Holdings (Daily)'))
    fig.add_trace(go.Scatter(x=lbma.index, y=lbma.values, name='LBMA',
                            line=dict(color='darkblue', width=2), mode='lines+markers', marker=dict(size=4)), row=1, col=1)
    fig.add_trace(go.Scatter(x=comex.index, y=comex.values, name='COMEX',
                            line=dict(color='orange', width=2), mode='lines'), row=2, col=1)
    fig.add_trace(go.Scatter(x=gld.index, y=gld.values, name='GLD',
                            line=dict(color='green', width=2), mode='lines'), row=3, col=1)
    fig.update_layout(title='G2: Three-Region Visible Inventory', template='plotly_white',
                     height=900, hovermode='x unified', showlegend=True)
    fig.update_yaxes(title_text='oz', row=1, col=1)
    fig.update_yaxes(title_text='oz', row=2, col=1)
    fig.update_yaxes(title_text='oz', row=3, col=1)
    fig.update_xaxes(title_text='Date', row=3, col=1)
    fig.update_xaxes(rangeslider_visible=True, rangeselector=dict(
        buttons=list([dict(count=1, label="1m", step="month", stepmode="backward"),
                     dict(count=6, label="6m", step="month", stepmode="backward"),
                     dict(count=1, label="1y", step="year", stepmode="backward"),
                     dict(step="all", label="All")]), x=0, y=-0.05), row=3, col=1)
    return fig

def create_gold_g3(df):
    """G3: 结构紧缺度"""
    comex_total = pivot_metric(df, 'comex_total_oz')
    comex_registered = pivot_metric(df, 'comex_registered_oz')
    comex_eligible = pivot_metric(df, 'comex_eligible_oz')
    registered_share = (comex_registered / comex_total * 100).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=registered_share.index, y=registered_share.values, name='Registered Share (%)',
                            line=dict(color='red', width=2.5), mode='lines', fill='tozeroy', fillcolor='rgba(255, 0, 0, 0.1)'))
    fig.add_trace(go.Scatter(x=comex_eligible.index, y=comex_eligible.values, name='Eligible',
                            line=dict(color='lightblue', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.add_trace(go.Scatter(x=comex_registered.index, y=comex_registered.values, name='Registered',
                            line=dict(color='darkred', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.update_layout(title='G3: COMEX Structural Tightness', xaxis_title='Date', yaxis_title='Registered Share (%)',
                     yaxis2=dict(title='Absolute Inventory (oz)', overlaying='y', side='right'),
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_gold_g4(df):
    """G4: 地理再分配"""
    comex = pivot_metric(df, 'comex_total_oz')
    lbma = pivot_metric(df, 'lbma_holdings_oz')
    comex_monthly = comex.resample('M').last()
    lbma_monthly = lbma.resample('M').last()
    geo_ratio = (comex_monthly / lbma_monthly).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=geo_ratio.index, y=geo_ratio.values, name='COMEX/LBMA Ratio',
                            line=dict(color='purple', width=2.5), mode='lines+markers', marker=dict(size=5)))
    fig.add_hline(y=geo_ratio.mean(), line_dash="dash", line_color="gray",
                 annotation_text=f"Mean: {geo_ratio.mean():.3f}", annotation_position="right")
    fig.update_layout(title='G4: Geographic Redistribution (COMEX/LBMA)', xaxis_title='Date',
                     yaxis_title='COMEX / LBMA', template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_gold_g5(df):
    """G5: 流量/拐点"""
    comex = pivot_metric(df, 'comex_total_oz')
    gld = pivot_metric(df, 'gld_holdings_oz')
    comex_delta = comex.diff(periods=28).dropna()
    gld_delta = gld.diff(periods=28).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=comex_delta.index, y=comex_delta.values, name='Δ4W COMEX Total',
                        marker=dict(color='#FF4500', line=dict(width=0)), opacity=1.0))
    fig.add_trace(go.Bar(x=gld_delta.index, y=gld_delta.values, name='Δ4W GLD Holdings',
                        marker=dict(color='#00FF7F', line=dict(width=0)), opacity=1.0))
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=1.5)
    fig.update_layout(title='G5: Inventory Flow/Inflection (4-Week Change)', xaxis_title='Date',
                     yaxis_title='Δ Inventory (oz)', template='plotly_white', height=500,
                     hovermode='x unified', barmode='group',
                     plot_bgcolor='white', paper_bgcolor='white')
    add_range_selector(fig)
    return fig

# ============================================================================
# SILVER 图表生成函数
# ============================================================================

def create_silver_s1(df):
    """S1: 银价"""
    spot_close = pivot_metric(df, 'price_futures_usd')
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=spot_close.index, y=spot_close.values, name='Silver Futures Price',
                             line=dict(color='silver', width=2), mode='lines'))
    fig.update_layout(title='S1: Silver Futures Price (SI=F)', xaxis_title='Date', yaxis_title='Price (USD/oz)',
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_silver_s2(df):
    """S2: 三地库存"""
    lbma = pivot_metric(df, 'lbma_holdings_oz')
    comex = pivot_metric(df, 'comex_total_oz')
    slv = pivot_metric(df, 'slv_holdings_oz')
    slv_weekly = slv.resample('W-FRI').last().dropna()
    
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                       subplot_titles=('LBMA Holdings (Monthly)', 'COMEX Total (Daily)', 'SLV Holdings (Weekly)'))
    fig.add_trace(go.Scatter(x=lbma.index, y=lbma.values, name='LBMA',
                            line=dict(color='darkblue', width=2), mode='lines+markers', marker=dict(size=4)), row=1, col=1)
    fig.add_trace(go.Scatter(x=comex.index, y=comex.values, name='COMEX',
                            line=dict(color='orange', width=2), mode='lines'), row=2, col=1)
    fig.add_trace(go.Scatter(x=slv_weekly.index, y=slv_weekly.values, name='SLV',
                            line=dict(color='purple', width=2), mode='lines+markers', marker=dict(size=3)), row=3, col=1)
    fig.update_layout(title='S2: Three-Region Visible Inventory', template='plotly_white',
                     height=900, hovermode='x unified', showlegend=True)
    fig.update_yaxes(title_text='oz', row=1, col=1)
    fig.update_yaxes(title_text='oz', row=2, col=1)
    fig.update_yaxes(title_text='oz', row=3, col=1)
    fig.update_xaxes(title_text='Date', row=3, col=1)
    fig.update_xaxes(rangeslider_visible=True, rangeselector=dict(
        buttons=list([dict(count=1, label="1m", step="month", stepmode="backward"),
                     dict(count=6, label="6m", step="month", stepmode="backward"),
                     dict(count=1, label="1y", step="year", stepmode="backward"),
                     dict(step="all", label="All")]), x=0, y=-0.05), row=3, col=1)
    return fig

def create_silver_s3(df):
    """S3: COMEX 结构紧缺度"""
    comex_total = pivot_metric(df, 'comex_total_oz')
    comex_registered = pivot_metric(df, 'comex_registered_oz')
    comex_eligible = pivot_metric(df, 'comex_eligible_oz')
    registered_share = (comex_registered / comex_total * 100).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=registered_share.index, y=registered_share.values, name='Registered Share (%)',
                            line=dict(color='red', width=2.5), mode='lines', fill='tozeroy', fillcolor='rgba(255, 0, 0, 0.1)'))
    fig.add_trace(go.Scatter(x=comex_eligible.index, y=comex_eligible.values, name='Eligible',
                            line=dict(color='lightblue', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.add_trace(go.Scatter(x=comex_registered.index, y=comex_registered.values, name='Registered',
                            line=dict(color='darkred', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.update_layout(title='S3: COMEX Delivery Structure Tightness', xaxis_title='Date',
                     yaxis_title='Registered Share (%)', yaxis2=dict(title='Absolute Inventory (oz)', overlaying='y', side='right'),
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_silver_s4(df):
    """S4: 地理再分配"""
    comex = pivot_metric(df, 'comex_total_oz')
    lbma = pivot_metric(df, 'lbma_holdings_oz')
    comex_monthly = comex.resample('M').last()
    lbma_monthly = lbma.resample('M').last()
    geo_ratio = (comex_monthly / lbma_monthly).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=geo_ratio.index, y=geo_ratio.values, name='COMEX/LBMA Ratio',
                            line=dict(color='purple', width=2.5), mode='lines+markers', marker=dict(size=5)))
    fig.add_hline(y=geo_ratio.mean(), line_dash="dash", line_color="gray",
                 annotation_text=f"Mean: {geo_ratio.mean():.3f}", annotation_position="right")
    fig.update_layout(title='S4: Geographic Redistribution (COMEX/LBMA)', xaxis_title='Date',
                     yaxis_title='COMEX / LBMA', template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_silver_s5(df):
    """S5: 资金流动量"""
    slv = pivot_metric(df, 'slv_holdings_oz')
    comex = pivot_metric(df, 'comex_total_oz')
    slv_weekly = slv.resample('W-FRI').last().dropna()
    slv_delta = slv_weekly.diff(periods=4).dropna()
    comex_delta = comex.diff(periods=28).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=slv_delta.index, y=slv_delta.values, name='Δ4W SLV Holdings',
                        marker_color='purple', opacity=0.7))
    fig.add_trace(go.Bar(x=comex_delta.index, y=comex_delta.values, name='Δ4W COMEX Total',
                        marker_color='orange', opacity=0.7))
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)
    fig.update_layout(title='S5: Capital Flow Momentum (4-Week Change)', xaxis_title='Date',
                     yaxis_title='Δ Inventory (oz)', template='plotly_white', height=500,
                     hovermode='x unified', barmode='group')
    add_range_selector(fig)
    return fig

# ============================================================================
# COPPER 图表生成函数
# ============================================================================

def create_copper_c0(df):
    """C0: 库存结构堆叠图 (The Inventory Structure)"""
    # 获取COMEX铜库存数据（已经是MT单位）
    comex_total_mt = pivot_metric(df, 'comex_total_mt')
    comex_registered_mt = pivot_metric(df, 'comex_registered_mt')
    comex_eligible_mt = pivot_metric(df, 'comex_eligible_mt')
    
    # 计算注册仓单比率
    registered_ratio = (comex_registered_mt / comex_total_mt * 100).dropna()
    
    # 创建双Y轴图表
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 左Y轴：堆叠面积图显示库存结构
    fig.add_trace(
        go.Scatter(
            x=comex_registered_mt.index, 
            y=comex_registered_mt.values,
            name='Registered (可交割)',
            line=dict(width=0),
            fillcolor='rgba(220, 20, 60, 0.6)',
            fill='tozeroy',
            mode='none',
            stackgroup='one',
            hovertemplate='Registered: %{y:.2f} MT<extra></extra>'
        ),
        secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=comex_eligible_mt.index, 
            y=comex_eligible_mt.values,
            name='Eligible (后备军)',
            line=dict(width=0),
            fillcolor='rgba(135, 206, 250, 0.6)',
            fill='tonexty',
            mode='none',
            stackgroup='one',
            hovertemplate='Eligible: %{y:.2f} MT<extra></extra>'
        ),
        secondary_y=False
    )
    
    # 右Y轴：注册仓单比率
    fig.add_trace(
        go.Scatter(
            x=registered_ratio.index,
            y=registered_ratio.values,
            name='Registered Ratio',
            line=dict(color='darkred', width=2.5, dash='solid'),
            mode='lines',
            hovertemplate='Ratio: %{y:.2f}%<extra></extra>'
        ),
        secondary_y=True
    )
    
    # 添加10%警戒线（右Y轴）
    fig.add_hline(
        y=10, 
        line_dash="dash", 
        line_color="orange", 
        line_width=2,
        annotation_text="10% 警戒线",
        annotation_position="right",
        secondary_y=True
    )
    
    # 添加20%参考线（右Y轴）
    fig.add_hline(
        y=20, 
        line_dash="dot", 
        line_color="gray", 
        line_width=1.5,
        annotation_text="20% 参考线",
        annotation_position="right",
        secondary_y=True
    )
    
    # 更新布局
    fig.update_layout(
        title='C0: COMEX Copper Inventory Structure (库存结构)',
        xaxis_title='Date',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # 设置Y轴标题
    fig.update_yaxes(title_text="库存量 (MT)", secondary_y=False)
    fig.update_yaxes(title_text="注册仓单比率 (%)", secondary_y=True)
    
    # 添加时间范围选择器
    add_range_selector(fig)
    
    return fig

def create_copper_c1(df):
    """C1: 铜价"""
    price = pivot_metric(df, 'price_futures_usd')
    price_w = price.resample('W-FRI').last().dropna()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=price_w.index, y=price_w.values, name='Copper Futures Price',
                             line=dict(color='peru', width=2), mode='lines'))
    fig.update_layout(title='C1: Copper Futures Price (HG=F)', xaxis_title='Date', yaxis_title='Price (USD/mt)',
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_copper_c2(df):
    """C2: 三交易所库存"""
    lme = pivot_metric(df, 'lme_closing_mt').resample('W-FRI').last().dropna()
    shfe = pivot_metric(df, 'shfe_total_mt').resample('W-FRI').last().dropna()
    comex = pivot_metric(df, 'comex_total_mt').resample('W-FRI').last().dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=lme.index, y=lme.values, name=f'LME Closing ({len(lme)} points)',
                            line=dict(color='darkblue', width=2.5), mode='lines'))
    fig.add_trace(go.Scatter(x=shfe.index, y=shfe.values, name=f'SHFE Total ({len(shfe)} points)',
                            line=dict(color='red', width=2.5), mode='lines'))
    if len(comex) > 0:
        fig.add_trace(go.Scatter(x=comex.index, y=comex.values, name=f'COMEX Total ({len(comex)} points)',
                                line=dict(color='orange', width=2), mode='lines+markers',
                                marker=dict(size=4), opacity=0.8))
    fig.update_layout(title='C2: Three-Exchange Inventory Levels', xaxis_title='Date', yaxis_title='Inventory (MT)',
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_copper_c3(df):
    """C3: 全球可视库存"""
    lme = pivot_metric(df, 'lme_closing_mt').resample('W-FRI').last().dropna()
    shfe = pivot_metric(df, 'shfe_total_mt').resample('W-FRI').last().dropna()
    comex = pivot_metric(df, 'comex_total_mt').resample('W-FRI').last().dropna()
    
    all_dates = lme.index.union(shfe.index)
    if len(comex) > 0:
        all_dates = all_dates.union(comex.index)
    
    lme_aligned = lme.reindex(all_dates).ffill()
    shfe_aligned = shfe.reindex(all_dates).ffill()
    comex_aligned = comex.reindex(all_dates).ffill()
    
    if len(comex) > 0:
        gvi = lme_aligned + shfe_aligned + comex_aligned
        label = 'GVI (LME + SHFE + COMEX)'
    else:
        gvi = lme_aligned + shfe_aligned
        label = 'GVI ex-COMEX (LME + SHFE)'
    gvi = gvi.dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=gvi.index, y=gvi.values, name=label,
                            line=dict(color='darkgreen', width=3), mode='lines',
                            fill='tozeroy', fillcolor='rgba(0, 100, 0, 0.1)'))
    fig.update_layout(title='C3: Global Visible Inventory (GVI)', xaxis_title='Date', yaxis_title='Total Inventory (MT)',
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_copper_c4(df):
    """C4: LME Tightness"""
    lme_closing = pivot_metric(df, 'lme_closing_mt').resample('W-FRI').last().dropna()
    lme_cancelled = pivot_metric(df, 'lme_cancelled_mt').resample('W-FRI').last().dropna()
    open_tonnage = pivot_metric(df, 'lme_open_interest_mt').resample('W-FRI').last().dropna()
    cancelled_share = (lme_cancelled / lme_closing * 100).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=cancelled_share.index, y=cancelled_share.values, name='Cancelled Share (%)',
                            line=dict(color='red', width=2.5), mode='lines', fill='tozeroy', fillcolor='rgba(255, 0, 0, 0.15)'))
    if len(open_tonnage) > 0:
        fig.add_trace(go.Scatter(x=open_tonnage.index, y=open_tonnage.values, name='Open Tonnage',
                                line=dict(color='darkblue', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.update_layout(title='C4: LME Tightness Structure', xaxis_title='Date', yaxis_title='Cancelled Share (%)',
                     yaxis2=dict(title='Open Tonnage (MT)', overlaying='y', side='right'),
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_copper_c5(df):
    """C5: SHFE 交割化结构"""
    shfe_total = pivot_metric(df, 'shfe_total_mt').resample('W-FRI').last().dropna()
    shfe_futures = pivot_metric(df, 'shfe_futures_mt').resample('W-FRI').last().dropna()
    deliverable_share = (shfe_futures / shfe_total * 100).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=deliverable_share.index, y=deliverable_share.values, name='Deliverable Share (%)',
                            line=dict(color='darkred', width=2.5), mode='lines', fill='tozeroy', fillcolor='rgba(139, 0, 0, 0.15)'))
    fig.add_trace(go.Scatter(x=shfe_futures.index, y=shfe_futures.values, name='SHFE Futures',
                            line=dict(color='orange', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.add_trace(go.Scatter(x=shfe_total.index, y=shfe_total.values, name='SHFE Total',
                            line=dict(color='blue', width=1.5, dash='dash'), mode='lines', yaxis='y2'))
    fig.update_layout(title='C5: SHFE Deliverable Structure', xaxis_title='Date', yaxis_title='Deliverable Share (%)',
                     yaxis2=dict(title='Absolute Inventory (MT)', overlaying='y', side='right'),
                     template='plotly_white', height=500, hovermode='x unified')
    add_range_selector(fig)
    return fig

def create_copper_c6(df):
    """C6: 库存动量"""
    lme = pivot_metric(df, 'lme_closing_mt').resample('W-FRI').last().dropna()
    shfe = pivot_metric(df, 'shfe_total_mt').resample('W-FRI').last().dropna()
    lme_delta = lme.diff(periods=4).dropna()
    shfe_delta = shfe.diff(periods=4).dropna()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=lme_delta.index, y=lme_delta.values, name='Δ4W LME Closing',
                        marker_color='darkblue', opacity=1.0))
    fig.add_trace(go.Bar(x=shfe_delta.index, y=shfe_delta.values, name='Δ4W SHFE Total',
                        marker_color='red', opacity=1.0))
    fig.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)
    fig.update_layout(title='C6: Inventory Momentum/Inflection', xaxis_title='Date', yaxis_title='Δ Inventory (MT)',
                     template='plotly_white', height=500, hovermode='x unified', barmode='group')
    add_range_selector(fig)
    return fig

# ============================================================================
# 主应用
# ============================================================================

def main():
    st.title("📊 Metal Market Analytics Dashboard")
    st.markdown("---")
    
    # 侧边栏
    st.sidebar.title("Navigation")
    st.sidebar.markdown("### Select Metal & Chart")
    
    # 选择金属
    metal = st.sidebar.selectbox(
        "Metal",
        ["GOLD", "SILVER", "COPPER"],
        index=0
    )
    
    # 定义每种金属的图表
    chart_options = {
        "GOLD": {
            "G1: Price": create_gold_g1,
            "G2: Three-Region Inventory": create_gold_g2,
            "G3: Structural Tightness": create_gold_g3,
            "G4: Geographic Redistribution": create_gold_g4,
            "G5: Inventory Flow": create_gold_g5
        },
        "SILVER": {
            "S1: Price": create_silver_s1,
            "S2: Three-Region Inventory": create_silver_s2,
            "S3: COMEX Structure": create_silver_s3,
            "S4: Geographic Redistribution": create_silver_s4,
            "S5: Capital Flow": create_silver_s5
        },
        "COPPER": {
            "C0: Inventory Structure": create_copper_c0,
            "C1: Price": create_copper_c1,
            "C2: Exchange Inventory": create_copper_c2,
            "C3: Global Visible Inventory": create_copper_c3,
            "C4: LME Tightness": create_copper_c4,
            "C5: SHFE Structure": create_copper_c5,
            "C6: Inventory Momentum": create_copper_c6
        }
    }
    
    # 选择图表
    chart_name = st.sidebar.selectbox(
        "Chart",
        list(chart_options[metal].keys())
    )
    
    # 显示全部图表选项
    show_all = st.sidebar.checkbox("Show All Charts", value=False)
    
    st.sidebar.markdown("---")
    st.sidebar.info(f"""
    **{metal} Analytics**
    
    Currently viewing: {chart_name}
    
    Use the dropdown menus above to switch between metals and charts.
    """)
    
    # 加载数据
    with st.spinner(f"Loading {metal} data..."):
        df = get_metal_data(metal)
    
    # 显示数据统计
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Metal", metal)
    with col2:
        st.metric("Total Records", f"{len(df):,}")
    with col3:
        metrics_count = df['metric'].nunique()
        st.metric("Unique Metrics", metrics_count)
    
    st.markdown("---")
    
    # 显示图表
    if show_all:
        st.subheader(f"All {metal} Charts")
        for name, func in chart_options[metal].items():
            with st.spinner(f"Generating {name}..."):
                fig = func(df)
                st.plotly_chart(fig, width='stretch')
            st.markdown("---")
    else:
        st.subheader(chart_name)
        with st.spinner("Generating chart..."):
            chart_func = chart_options[metal][chart_name]
            fig = chart_func(df)
            st.plotly_chart(fig, width='stretch')
    
    # 页脚
    st.markdown("---")
    st.caption("Metal Market Analytics Dashboard | Data sourced from PostgreSQL database")

if __name__ == "__main__":
    main()
