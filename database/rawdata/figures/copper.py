import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# 连接数据库
engine = create_engine('postgresql://postgres:125123@localhost:5432/commodities_db')

def get_copper_data():
    """读取 COPPER 所有数据"""
    query = """
    SELECT as_of_date, metric, value, unit, source
    FROM clean.observations 
    WHERE metal = 'COPPER'
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
print("正在从数据库读取 COPPER 数据...")
df = get_copper_data()
print(f"读取完成：{len(df)} 条记录")

# 提取各个指标（日频）
fut_front_usd = pivot_metric(df, 'fut_front_usd')
lme_closing_mt = pivot_metric(df, 'lme_closing_mt')
lme_cancelled_mt = pivot_metric(df, 'lme_cancelled_mt')
lme_opening_mt = pivot_metric(df, 'lme_opening_mt')
open_tonnage = pivot_metric(df, 'open_tonnage')
shfe_total_mt = pivot_metric(df, 'shfe_total_mt')
shfe_futures_mt = pivot_metric(df, 'shfe_futures_mt')
comex_total_mt = pivot_metric(df, 'comex_total_mt')

# 转换为周频（W-FRI 最后值）
print("\n将所有序列对齐到周频（W-FRI）...")
fut_front_usd_w = fut_front_usd.resample('W-FRI').last().dropna()
lme_closing_mt_w = lme_closing_mt.resample('W-FRI').last().dropna()
lme_cancelled_mt_w = lme_cancelled_mt.resample('W-FRI').last().dropna()
open_tonnage_w = open_tonnage.resample('W-FRI').last().dropna()
shfe_total_mt_w = shfe_total_mt.resample('W-FRI').last().dropna()
shfe_futures_mt_w = shfe_futures_mt.resample('W-FRI').last().dropna()
comex_total_mt_w = comex_total_mt.resample('W-FRI').last().dropna()

print(f"LME Closing: {len(lme_closing_mt_w)} 周")
print(f"SHFE Total: {len(shfe_total_mt_w)} 周")
print(f"COMEX Total: {len(comex_total_mt_w)} 周")

print("\n正在生成图表...")

# ============================================================================
# 图 C1: 价格
# ============================================================================
print("C1: 价格图...")
fig_c1 = go.Figure()

fig_c1.add_trace(go.Scatter(
    x=fut_front_usd_w.index,
    y=fut_front_usd_w.values,
    name='Copper Futures Price',
    line=dict(color='peru', width=2),
    mode='lines'
))

fig_c1.update_layout(
    title=dict(text='C1: Copper Futures Price (YFINANCE - HG=F)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Price (USD/mt)',
    template='plotly_white',
    height=500,
    hovermode='x unified'
)
add_range_selector(fig_c1)

# ============================================================================
# 图 C2: 三交易所库存水平（周频）
# ============================================================================
print("C2: 三交易所库存水平...")
fig_c2 = go.Figure()

# LME
fig_c2.add_trace(go.Scatter(
    x=lme_closing_mt_w.index,
    y=lme_closing_mt_w.values,
    name='LME Closing',
    line=dict(color='darkblue', width=2.5),
    mode='lines'
))

# SHFE
fig_c2.add_trace(go.Scatter(
    x=shfe_total_mt_w.index,
    y=shfe_total_mt_w.values,
    name='SHFE Total',
    line=dict(color='red', width=2.5),
    mode='lines'
))

# COMEX (数据稀疏，灰色虚线)
if len(comex_total_mt_w) > 0:
    fig_c2.add_trace(go.Scatter(
        x=comex_total_mt_w.index,
        y=comex_total_mt_w.values,
        name='COMEX Total (sparse)',
        line=dict(color='gray', width=1.5, dash='dash'),
        mode='lines+markers',
        marker=dict(size=5),
        opacity=0.6
    ))

fig_c2.update_layout(
    title=dict(text='C2: Three-Exchange Inventory Levels (Weekly)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Inventory (MT)',
    template='plotly_white',
    height=500,
    hovermode='x unified',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_c2)

# ============================================================================
# 图 C3: 全球可视库存 GVI
# ============================================================================
print("C3: 全球可视库存 GVI...")
fig_c3 = go.Figure()

# 合并日期索引
all_dates = lme_closing_mt_w.index.union(shfe_total_mt_w.index)
if len(comex_total_mt_w) > 0:
    all_dates = all_dates.union(comex_total_mt_w.index)

# 对齐数据
lme_aligned = lme_closing_mt_w.reindex(all_dates).ffill()
shfe_aligned = shfe_total_mt_w.reindex(all_dates).ffill()
comex_aligned = comex_total_mt_w.reindex(all_dates).ffill()

# 计算 GVI
if len(comex_total_mt_w) > 0:
    gvi = lme_aligned + shfe_aligned + comex_aligned
    gvi_label = 'GVI (LME + SHFE + COMEX)'
else:
    gvi = lme_aligned + shfe_aligned
    gvi_label = 'GVI ex-COMEX (LME + SHFE)'

gvi = gvi.dropna()

fig_c3.add_trace(go.Scatter(
    x=gvi.index,
    y=gvi.values,
    name=gvi_label,
    line=dict(color='darkgreen', width=3),
    mode='lines',
    fill='tozeroy',
    fillcolor='rgba(0, 100, 0, 0.1)'
))

fig_c3.update_layout(
    title=dict(text='C3: Global Visible Inventory (GVI)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Total Inventory (MT)',
    template='plotly_white',
    height=500,
    hovermode='x unified'
)
add_range_selector(fig_c3)

# ============================================================================
# 图 C4: LME Tightness 结构（Cancelled Share）
# ============================================================================
print("C4: LME Tightness 结构...")
fig_c4 = go.Figure()

# 计算 Cancelled Share
cancelled_share = (lme_cancelled_mt_w / lme_closing_mt_w * 100).dropna()

fig_c4.add_trace(go.Scatter(
    x=cancelled_share.index,
    y=cancelled_share.values,
    name='Cancelled Share (%)',
    line=dict(color='red', width=2.5),
    mode='lines',
    fill='tozeroy',
    fillcolor='rgba(255, 0, 0, 0.15)'
))

# 添加 Open Tonnage（右轴）
if len(open_tonnage_w) > 0:
    fig_c4.add_trace(go.Scatter(
        x=open_tonnage_w.index,
        y=open_tonnage_w.values,
        name='Open Tonnage',
        line=dict(color='darkblue', width=1.5, dash='dash'),
        mode='lines',
        yaxis='y2'
    ))

fig_c4.update_layout(
    title=dict(text='C4: LME Tightness Structure (Cancelled/Closing)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Cancelled Share (%)',
    yaxis2=dict(
        title='Open Tonnage (MT)',
        overlaying='y',
        side='right'
    ),
    template='plotly_white',
    height=500,
    hovermode='x unified',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_c4)

# ============================================================================
# 图 C5: SHFE 交割化结构（Deliverable Share）
# ============================================================================
print("C5: SHFE 交割化结构...")
fig_c5 = go.Figure()

# 计算 Deliverable Share
deliverable_share = (shfe_futures_mt_w / shfe_total_mt_w * 100).dropna()

fig_c5.add_trace(go.Scatter(
    x=deliverable_share.index,
    y=deliverable_share.values,
    name='Deliverable Share (%)',
    line=dict(color='darkred', width=2.5),
    mode='lines',
    fill='tozeroy',
    fillcolor='rgba(139, 0, 0, 0.15)'
))

# 添加绝对值（右轴）
fig_c5.add_trace(go.Scatter(
    x=shfe_futures_mt_w.index,
    y=shfe_futures_mt_w.values,
    name='SHFE Futures',
    line=dict(color='orange', width=1.5, dash='dash'),
    mode='lines',
    yaxis='y2'
))

fig_c5.add_trace(go.Scatter(
    x=shfe_total_mt_w.index,
    y=shfe_total_mt_w.values,
    name='SHFE Total',
    line=dict(color='blue', width=1.5, dash='dash'),
    mode='lines',
    yaxis='y2'
))

fig_c5.update_layout(
    title=dict(text='C5: SHFE Deliverable Structure (Futures/Total)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Deliverable Share (%)',
    yaxis2=dict(
        title='Absolute Inventory (MT)',
        overlaying='y',
        side='right'
    ),
    template='plotly_white',
    height=500,
    hovermode='x unified',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_c5)

# ============================================================================
# 图 C6: 库存动量/拐点（4周变化）
# ============================================================================
print("C6: 库存动量/拐点...")
fig_c6 = go.Figure()

# 计算 4 周变化量（周频数据，4周 = 4个数据点）
lme_delta_4w = lme_closing_mt_w.diff(periods=4).dropna()
shfe_delta_4w = shfe_total_mt_w.diff(periods=4).dropna()

fig_c6.add_trace(go.Bar(
    x=lme_delta_4w.index,
    y=lme_delta_4w.values,
    name='Δ4W LME Closing',
    marker_color='darkblue',
    opacity=0.7
))

fig_c6.add_trace(go.Bar(
    x=shfe_delta_4w.index,
    y=shfe_delta_4w.values,
    name='Δ4W SHFE Total',
    marker_color='red',
    opacity=0.7
))

# 添加零线
fig_c6.add_hline(y=0, line_dash="solid", line_color="black", line_width=1)

fig_c6.update_layout(
    title=dict(text='C6: Inventory Momentum/Inflection (4-Week Change)', x=0.5, xanchor='center', font=dict(size=18)),
    xaxis_title='Date',
    yaxis_title='Δ Inventory (MT)',
    template='plotly_white',
    height=500,
    hovermode='x unified',
    barmode='group',
    legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
)
add_range_selector(fig_c6)

# ============================================================================
# 显示所有图表
# ============================================================================
print("\n显示图表...")
fig_c1.show()
fig_c2.show()
fig_c3.show()
fig_c4.show()
fig_c5.show()
fig_c6.show()

print("\n✓ 完成！6 张 COPPER 图表已生成")
print("\n图表说明：")
print("C1: 铜期货价格（YFINANCE HG=F）")
print("C2: 三交易所库存水平（LME + SHFE + COMEX，周频对齐）")
print("C3: 全球可视库存 GVI（合成指标）")
print("C4: LME Tightness 结构（Cancelled Share + Open Tonnage）")
print("C5: SHFE 交割化结构（Deliverable Share，中国紧缺度）")
print("C6: 库存动量/拐点（4周变化量，LME + SHFE）")
print("\n注意：所有数据已对齐到周频（W-FRI），COMEX数据稀疏以灰色虚线显示")