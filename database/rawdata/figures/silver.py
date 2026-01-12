import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# 连接数据库
engine = create_engine('postgresql://postgres:125123@localhost:5432/commodities_db')

def get_silver_data():
    """读取 SILVER 所有数据"""
    query = """
    SELECT as_of_date, metric, value, unit, source
    FROM clean.observations 
    WHERE metal = 'SILVER'
    ORDER BY as_of_date, metric
    """
    df = pd.read_sql(query, engine)
    df['as_of_date'] = pd.to_datetime(df['as_of_date'])
    return df

def pivot_metric(df, metric_name):
    """将指定 metric 转为时间序列"""
    data = df[df['metric'] == metric_name].copy()
    data = data.sort_values('as_of_date')
    data = data.drop_duplicates(subset=['as_of_date'], keep='last')
    data = data.set_index('as_of_date')['value']
    return data

def add_range_selector(fig):
    """添加时间范围选择器"""
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward"),
                dict(count=1, label="1y", step="year", stepmode="backward"),
                dict(step="all", label="All")
            ]),
            bgcolor="rgba(255, 255, 255, 0.8)",
            activecolor="rgba(100, 149, 237, 0.5)",
            x=0,
            y=1.02
        )
    )

# 读取数据
print("正在从数据库读取 SILVER 数据...")
df = get_silver_data()
print(f"读取完成：{len(df)} 条记录")

# 提取各个指标
spot_close = pivot_metric(df, 'spot_close')
lbma_holdings = pivot_metric(df, 'lbma_holdings_oz')
comex_total = pivot_metric(df, 'comex_total_oz')
comex_registered = pivot_metric(df, 'comex_registered_oz')
comex_eligible = pivot_metric(df, 'comex_eligible_oz')
slv_holdings = pivot_metric(df, 'slv_holdings_oz')

# 将 SLV 转为周频（W-FRI 最后值）以减少断点影响
print("将 SLV 数据转为周频...")
slv_weekly = slv_holdings.resample('W-FRI').last().dropna()

print("\n正在生成图表...")

# ============================================================================
# 图 S1: 价格
# ============================================================================
print("S1: 价格图...")
fig_s1 = go.Figure()

fig_s1.add_trace(go.Scatter(
    x=spot_close.index,
    y=spot_close.values,
    name='Silver Spot Price',
    line=dict(color='silver', width=2),
    mode='lines'
))

fig_s1.update_layout(
    title=dict(text='S1: Silver Spot Price (YFINANCE)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Price (USD/oz)',
    template='plotly_white',
    height=500,
    hovermode='x unified'
)
add_range_selector(fig_s1)

# ============================================================================
# 图 S2: 三地可视库存（分频处理 - 使用子图）
# ============================================================================
print("S2: 三地库存子图...")
fig_s2 = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.08,
    subplot_titles=('LBMA Holdings (Monthly)', 'COMEX Total (Daily)', 'SLV Holdings (Weekly)')
)

# 子图A: LBMA (月频)
fig_s2.add_trace(go.Scatter(
    x=lbma_holdings.index,
    y=lbma_holdings.values,
    name='LBMA',
    line=dict(color='darkblue', width=2),
    mode='lines+markers',
    marker=dict(size=4)
), row=1, col=1)

# 子图B: COMEX (日频)
fig_s2.add_trace(go.Scatter(
    x=comex_total.index,
    y=comex_total.values,
    name='COMEX',
    line=dict(color='orange', width=2),
    mode='lines'
), row=2, col=1)

# 子图C: SLV (周频)
fig_s2.add_trace(go.Scatter(
    x=slv_weekly.index,
    y=slv_weekly.values,
    name='SLV',
    line=dict(color='purple', width=2),
    mode='lines+markers',
    marker=dict(size=3)
), row=3, col=1)

fig_s2.update_layout(
    title=dict(text='S2: Three-Region Visible Inventory (Multi-Frequency)', x=0.5, xanchor='center', font=dict(size=18)),
    template='plotly_white',
    height=900,
    hovermode='x unified',
    showlegend=True
)

fig_s2.update_yaxes(title_text='oz', row=1, col=1)
fig_s2.update_yaxes(title_text='oz', row=2, col=1)
fig_s2.update_yaxes(title_text='oz', row=3, col=1)
fig_s2.update_xaxes(title_text='Date', row=3, col=1)

# 只在最底部添加范围选择器
fig_s2.update_xaxes(
    rangeslider_visible=True,
    rangeselector=dict(
        buttons=list([
            dict(count=1, label="1m", step="month", stepmode="backward"),
            dict(count=6, label="6m", step="month", stepmode="backward"),
            dict(count=1, label="1y", step="year", stepmode="backward"),
            dict(step="all", label="All")
        ]),
        bgcolor="rgba(255, 255, 255, 0.8)",
        activecolor="rgba(100, 149, 237, 0.5)",
        x=0,
        y=-0.05
    ),
    row=3, col=1
)

# ============================================================================
# 图 S3: COMEX 结构紧缺度（Registered/Total 比例）
# ============================================================================
print("S3: COMEX 结构紧缺度...")
fig_s3 = go.Figure()

# 计算 Registered Share
registered_share = (comex_registered / comex_total * 100).dropna()

fig_s3.add_trace(go.Scatter(
    x=registered_share.index,
    y=registered_share.values,
    name='Registered Share (%)',
    line=dict(color='red', width=2.5),
    mode='lines',
    fill='tozeroy',
    fillcolor='rgba(255, 0, 0, 0.1)'
))

# 可选：添加 Eligible 和 Registered 的绝对值（右轴）
fig_s3.add_trace(go.Scatter(
    x=comex_eligible.index,
    y=comex_eligible.values,
    name='Eligible',
    line=dict(color='lightblue', width=1.5, dash='dash'),
    mode='lines',
    yaxis='y2'
))

fig_s3.add_trace(go.Scatter(
    x=comex_registered.index,
    y=comex_registered.values,
    name='Registered',
    line=dict(color='darkred', width=1.5, dash='dash'),
    mode='lines',
    yaxis='y2'
))

fig_s3.update_layout(
    title=dict(text='S3: COMEX Delivery Structure Tightness (Registered/Total)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Registered Share (%)',
    yaxis2=dict(
        title='Absolute Inventory (oz)',
        overlaying='y',
        side='right'
    ),
    template='plotly_white',
    height=500,
    hovermode='x unified',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_s3)

# ============================================================================
# 图 S4: 地理再分配指标（COMEX/LBMA 比率）
# ============================================================================
print("S4: 地理再分配指标...")
fig_s4 = go.Figure()

# 将 COMEX 转为月度（取月末值）
comex_monthly = comex_total.resample('M').last()
lbma_monthly = lbma_holdings.resample('M').last()

# 计算比率
geo_ratio = (comex_monthly / lbma_monthly).dropna()

fig_s4.add_trace(go.Scatter(
    x=geo_ratio.index,
    y=geo_ratio.values,
    name='COMEX/LBMA Ratio',
    line=dict(color='purple', width=2.5),
    mode='lines+markers',
    marker=dict(size=5)
))

# 添加参考线（均值）
mean_ratio = geo_ratio.mean()
fig_s4.add_hline(
    y=mean_ratio,
    line_dash="dash",
    line_color="gray",
    annotation_text=f"Mean: {mean_ratio:.3f}",
    annotation_position="right"
)

fig_s4.update_layout(
    title=dict(text='S4: Geographic Redistribution (COMEX/LBMA Ratio, Monthly)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='COMEX / LBMA',
    template='plotly_white',
    height=500,
    hovermode='x unified'
)
add_range_selector(fig_s4)

# ============================================================================
# 图 S5: 资金流动量（4周变化量）- 银的关键指标
# ============================================================================
print("S5: 资金流动量...")
fig_s5 = go.Figure()

# 计算 4 周变化量
# 对于 SLV 周频数据，4周 = 4个数据点
slv_delta_4w = slv_weekly.diff(periods=4).dropna()
# 对于 COMEX 日频数据，4周 ≈ 28天
comex_delta_4w = comex_total.diff(periods=28).dropna()

fig_s5.add_trace(go.Bar(
    x=slv_delta_4w.index,
    y=slv_delta_4w.values,
    name='Δ4W SLV Holdings',
    marker_color='purple',
    opacity=0.7
))

fig_s5.add_trace(go.Bar(
    x=comex_delta_4w.index,
    y=comex_delta_4w.values,
    name='Δ4W COMEX Total',
    marker_color='orange',
    opacity=0.7
))

# 添加零线
fig_s5.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)

fig_s5.update_layout(
    title=dict(text='S5: Capital Flow Momentum (4-Week Change)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Δ Inventory (oz)',
    template='plotly_white',
    height=500,
    hovermode='x unified',
    barmode='group',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_s5)

# ============================================================================
# 显示所有图表
# ============================================================================
print("\n显示图表...")
fig_s1.show()
fig_s2.show()
fig_s3.show()
fig_s4.show()
fig_s5.show()

print("\n✓ 完成！5 张 SILVER 图表已生成")
print("\n图表说明：")
print("S1: 银价走势")
print("S2: 三地库存水平（LBMA月频 + COMEX日频 + SLV周频，分层展示）")
print("S3: COMEX 交割结构紧缺（Registered/Total比例 + 绝对库存）")
print("S4: 地理再分配（COMEX/LBMA比率，月度对齐）")
print("S5: 资金流动量（4周变化量，SLV+COMEX对比，识别资金流拐点）")
print("\n注意：SLV数据已转为周频（W-FRI）以减少断点影响，视觉更稳定")