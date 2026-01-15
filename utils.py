"""
通用绘图工具模块 (Visualization Utility Module)
================================================
统一 Plotly 图表风格，避免页面代码重复

核心图表：
1. plot_percentile_trend() - 分位数走势面积图（带警戒线）
2. plot_regional_bar() - 区域分位数柱状图
3. plot_price_trend() - 价格走势线图
4. plot_inventory_stacked() - 库存堆叠图
5. plot_heatmap() - 热力图
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np

# ================= 全局样式配置 =================
THEME = {
    # 颜色方案
    'colors': {
        'primary': '#1f77b4',      # 主色调（蓝色）
        'secondary': '#ff7f0e',    # 次色调（橙色）
        'success': '#2ca02c',      # 看多（绿色）
        'danger': '#d62728',       # 看空（红色）
        'warning': '#ffbb33',      # 警告（黄色）
        'neutral': '#7f7f7f',      # 中性（灰色）
        'background': '#fafafa',   # 背景色
        'grid': '#e5e5e5',         # 网格线
    },
    # 金属专属颜色
    'metal_colors': {
        'COPPER': '#B87333',       # 铜色
        'GOLD': '#FFD700',         # 金色
        'SILVER': '#C0C0C0',       # 银色
    },
    # 交易所颜色
    'source_colors': {
        'LME': '#1f77b4',          # 蓝色
        'COMEX': '#ff7f0e',        # 橙色
        'SHFE': '#2ca02c',         # 绿色
        'LBMA': '#9467bd',         # 紫色
        'GLD': '#FFD700',          # 金色
        'SLV': '#C0C0C0',          # 银色
    },
    # 字体
    'font': {
        'family': 'Arial, sans-serif',
        'size': 12,
        'color': '#333333'
    },
    # 布局
    'layout': {
        'paper_bgcolor': 'white',
        'plot_bgcolor': '#fafafa',
        'margin': dict(l=60, r=40, t=60, b=40),
    }
}

# 警戒线阈值
THRESHOLDS = {
    'strong_bullish': 0.05,    # 强看多 (< 5%)
    'bullish': 0.10,           # 看多 (< 10%)
    'bearish': 0.90,           # 看空 (> 90%)
    'strong_bearish': 0.95,    # 强看空 (> 95%)
}


# ================= 基础布局函数 =================
def get_base_layout(title: str = "", height: int = 400, show_legend: bool = True) -> dict:
    """
    获取基础布局配置
    
    Args:
        title: 图表标题
        height: 图表高度
        show_legend: 是否显示图例
    
    Returns:
        dict: Plotly 布局配置
    """
    return {
        'title': {
            'text': title,
            'font': {'size': 16, 'color': THEME['font']['color']},
            'x': 0.5,
            'xanchor': 'center'
        },
        'font': THEME['font'],
        'paper_bgcolor': THEME['layout']['paper_bgcolor'],
        'plot_bgcolor': THEME['layout']['plot_bgcolor'],
        'margin': THEME['layout']['margin'],
        'height': height,
        'showlegend': show_legend,
        'legend': {
            'orientation': 'h',
            'yanchor': 'bottom',
            'y': 1.02,
            'xanchor': 'right',
            'x': 1
        },
        'hovermode': 'x unified',
    }


def add_threshold_lines(fig, y_min: float = 0, y_max: float = 1) -> go.Figure:
    """
    添加警戒线（5% 和 95%）
    
    Args:
        fig: Plotly Figure 对象
        y_min: Y轴最小值
        y_max: Y轴最大值
    
    Returns:
        go.Figure: 更新后的图表
    """
    # 5% 看多警戒线（绿色）
    fig.add_hline(
        y=THRESHOLDS['strong_bullish'],
        line_dash="dash",
        line_color=THEME['colors']['success'],
        line_width=1.5,
        annotation_text="5%",
        annotation_position="right",
        annotation_font_color=THEME['colors']['success'],
        annotation_font_size=10
    )
    
    # 95% 看空警戒线（红色）
    fig.add_hline(
        y=THRESHOLDS['strong_bearish'],
        line_dash="dash",
        line_color=THEME['colors']['danger'],
        line_width=1.5,
        annotation_text="95%",
        annotation_position="right",
        annotation_font_color=THEME['colors']['danger'],
        annotation_font_size=10
    )
    
    return fig


# ================= 图表一：分位数走势面积图 =================
def plot_percentile_trend(
    df: pd.DataFrame,
    date_col: str = 'date',
    pct_col: str = 'percentile',
    title: str = "全球库存分位走势 (Global Inventory Percentile)",
    height: int = 400,
    show_thresholds: bool = True,
    fill_color: str = None,
    metal: str = None
) -> go.Figure:
    """
    绘制分位数走势面积图（带警戒线）
    
    Args:
        df: 包含日期和分位数的 DataFrame
        date_col: 日期列名
        pct_col: 分位数列名
        title: 图表标题
        height: 图表高度
        show_thresholds: 是否显示警戒线
        fill_color: 填充颜色（默认根据金属自动选择）
        metal: 金属类型（用于自动选择颜色）
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 选择颜色
    if fill_color is None:
        if metal and metal in THEME['metal_colors']:
            fill_color = THEME['metal_colors'][metal]
        else:
            fill_color = THEME['colors']['primary']
    
    # 创建图表
    fig = go.Figure()
    
    # 添加面积图
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[pct_col],
        mode='lines',
        name='分位数',
        line=dict(color=fill_color, width=2),
        fill='tozeroy',
        fillcolor=f'rgba{tuple(list(px.colors.hex_to_rgb(fill_color)) + [0.3])}',
        hovertemplate='%{x|%Y-%m-%d}<br>分位数: %{y:.1%}<extra></extra>'
    ))
    
    # 添加警戒线
    if show_thresholds:
        fig = add_threshold_lines(fig)
    
    # 更新布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {
            'title': '日期',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '%Y-%m',
        },
        'yaxis': {
            'title': '历史分位 (%)',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '.0%',
            'range': [-0.05, 1.05],  # 扩展范围让0%和100%更明显
        }
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 图表二：区域分位数柱状图 =================
def plot_regional_bar(
    df: pd.DataFrame,
    source_col: str = 'source',
    pct_col: str = 'percentile',
    value_col: str = 'current_value',
    title: str = "分交易所库存分位 (Regional Inventory Percentile)",
    height: int = 350,
    show_values: bool = True
) -> go.Figure:
    """
    绘制区域分位数柱状图（三根柱子）
    
    Args:
        df: 包含交易所和分位数的 DataFrame
        source_col: 交易所列名
        pct_col: 分位数列名
        value_col: 当前值列名（可选，用于显示）
        title: 图表标题
        height: 图表高度
        show_values: 是否在柱子上显示数值
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 根据分位数确定颜色
    def get_bar_color(pct):
        if pd.isna(pct):
            return THEME['colors']['neutral']
        if pct <= THRESHOLDS['strong_bullish']:
            return THEME['colors']['success']
        elif pct >= THRESHOLDS['strong_bearish']:
            return THEME['colors']['danger']
        elif pct <= THRESHOLDS['bullish']:
            return '#90EE90'  # 浅绿
        elif pct >= THRESHOLDS['bearish']:
            return '#FFB6C1'  # 浅红
        else:
            return THEME['colors']['primary']
    
    colors = [get_bar_color(pct) for pct in df[pct_col]]
    
    # 创建图表
    fig = go.Figure()
    
    # 添加柱状图
    fig.add_trace(go.Bar(
        x=df[source_col],
        y=df[pct_col],
        marker_color=colors,
        text=[f'{p:.1%}' for p in df[pct_col]] if show_values else None,
        textposition='outside',
        textfont=dict(size=14, color=THEME['font']['color']),
        hovertemplate='<b>%{x}</b><br>分位数: %{y:.1%}<extra></extra>'
    ))
    
    # 添加警戒线
    fig = add_threshold_lines(fig)
    
    # 更新布局
    layout = get_base_layout(title, height, show_legend=False)
    layout.update({
        'xaxis': {
            'title': '交易所',
            'showgrid': False,
            'tickfont': dict(size=14),
        },
        'yaxis': {
            'title': '历史分位 (%)',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '.0%',
            'range': [0, 1.15],  # 留空间给文字标签
        },
        'bargap': 0.3,
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 图表三：价格走势线图 =================
def plot_price_trend(
    df: pd.DataFrame,
    date_col: str = 'date',
    price_col: str = 'price',
    title: str = "价格走势 (Price Trend)",
    height: int = 350,
    metal: str = None,
    unit: str = "USD"
) -> go.Figure:
    """
    绘制价格走势线图
    
    Args:
        df: 包含日期和价格的 DataFrame
        date_col: 日期列名
        price_col: 价格列名
        title: 图表标题
        height: 图表高度
        metal: 金属类型
        unit: 价格单位
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 选择颜色
    line_color = THEME['metal_colors'].get(metal, THEME['colors']['primary'])
    
    # 创建图表
    fig = go.Figure()
    
    # 添加价格线
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[price_col],
        mode='lines',
        name='价格',
        line=dict(color=line_color, width=2),
        hovertemplate='%{x|%Y-%m-%d}<br>价格: $%{y:,.2f}<extra></extra>'
    ))
    
    # 更新布局
    layout = get_base_layout(title, height, show_legend=False)
    layout.update({
        'xaxis': {
            'title': '日期',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '%Y-%m',
        },
        'yaxis': {
            'title': f'价格 ({unit})',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickprefix': '$',
            'tickformat': ',.0f',
        }
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 图表四：库存堆叠图 =================
def plot_inventory_stacked(
    df: pd.DataFrame,
    date_col: str = 'date',
    source_cols: list = None,
    title: str = "全球库存结构 (Global Inventory Structure)",
    height: int = 400,
    unit: str = "mt"
) -> go.Figure:
    """
    绘制库存堆叠面积图
    
    Args:
        df: 包含日期和各来源库存的 DataFrame
        date_col: 日期列名
        source_cols: 来源列名列表
        title: 图表标题
        height: 图表高度
        unit: 库存单位
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if df.empty or source_cols is None:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 创建图表
    fig = go.Figure()
    
    # 添加各来源的堆叠面积
    for source in source_cols:
        if source in df.columns:
            color = THEME['source_colors'].get(source, THEME['colors']['primary'])
            fig.add_trace(go.Scatter(
                x=df[date_col],
                y=df[source],
                mode='lines',
                name=source,
                stackgroup='one',
                line=dict(width=0.5, color=color),
                fillcolor=f'rgba{tuple(list(px.colors.hex_to_rgb(color)) + [0.7])}',
                hovertemplate=f'<b>{source}</b><br>' + '%{x|%Y-%m-%d}<br>库存: %{y:,.0f}<extra></extra>'
            ))
    
    # 更新布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {
            'title': '日期',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '%Y-%m',
        },
        'yaxis': {
            'title': f'库存量 ({unit})',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': ',.0f',
        }
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 图表五：热力图 =================
def plot_heatmap(
    df: pd.DataFrame,
    title: str = "全球库存压力热力图 (Global Inventory Heatmap)",
    height: int = 300
) -> go.Figure:
    """
    绘制库存分位热力图
    
    Args:
        df: 行=金属, 列=交易所, 值=分位数 的 DataFrame
        title: 图表标题
        height: 图表高度
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 准备数据
    z_values = df.values
    x_labels = df.columns.tolist()
    y_labels = df.index.tolist()
    
    # 创建文本标注
    text_values = [[f'{v:.0%}' if not pd.isna(v) else '-' for v in row] for row in z_values]
    
    # 创建图表
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=x_labels,
        y=y_labels,
        colorscale='RdYlGn_r',  # 红绿反转：低值(0)是绿，高值(1)是红
        zmin=0,
        zmax=1,
        text=text_values,
        texttemplate='%{text}',
        textfont=dict(size=14, color='black'),
        hovertemplate='<b>%{y} - %{x}</b><br>分位数: %{z:.1%}<extra></extra>',
        colorbar=dict(
            title='分位数',
            tickformat='.0%',
            tickvals=[0, 0.25, 0.5, 0.75, 1],
        )
    ))
    
    # 更新布局
    layout = get_base_layout(title, height, show_legend=False)
    layout.update({
        'xaxis': {
            'title': '交易所 / 数据源',
            'tickfont': dict(size=12),
            'side': 'bottom',
        },
        'yaxis': {
            'title': '金属',
            'tickfont': dict(size=12),
            'autorange': 'reversed',  # 从上到下
        }
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 图表六：多来源分位对比线图 =================
def plot_multi_source_percentile(
    data: dict,
    title: str = "分交易所分位走势对比",
    height: int = 400
) -> go.Figure:
    """
    绘制多来源分位数走势对比线图
    
    Args:
        data: {source: DataFrame} 字典，每个 DataFrame 包含 date, percentile 列
        title: 图表标题
        height: 图表高度
    
    Returns:
        go.Figure: Plotly 图表对象
    """
    if not data:
        fig = go.Figure()
        fig.add_annotation(text="暂无数据", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        fig.update_layout(**get_base_layout(title, height))
        return fig
    
    # 创建图表
    fig = go.Figure()
    
    for source, df in data.items():
        if df.empty:
            continue
        color = THEME['source_colors'].get(source, THEME['colors']['primary'])
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['percentile'],
            mode='lines',
            name=source,
            line=dict(color=color, width=2),
            hovertemplate=f'<b>{source}</b><br>' + '%{x|%Y-%m-%d}<br>分位数: %{y:.1%}<extra></extra>'
        ))
    
    # 添加警戒线
    fig = add_threshold_lines(fig)
    
    # 更新布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {
            'title': '日期',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '%Y-%m',
        },
        'yaxis': {
            'title': '历史分位 (%)',
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickformat': '.0%',
            'range': [-0.05, 1.05],  # 扩展范围让0%和100%更明显
        }
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 复合图表模板 (Composite Charts) =================
# 用于衍生因子的可视化：双轴图、正负柱状图、堆叠面积图等

def plot_combo_ratio_price(
    df: pd.DataFrame,
    date_col: str = 'date',
    ratio_col: str = 'ratio',
    price_col: str = 'price',
    title: str = "比率与价格对比",
    ratio_name: str = "比率",
    height: int = 400,
    ratio_threshold: float = None,
    fill_area: bool = True
) -> go.Figure:
    """
    组合图：比率(面积/柱状) + 价格(线图) 双轴
    
    用途: LME Cancelled Ratio, GLD Fund Flows 等
    
    Args:
        df: 数据框
        date_col: 日期列名
        ratio_col: 比率列名 (左轴)
        price_col: 价格列名 (右轴)
        title: 图表标题
        ratio_name: 比率名称
        height: 图表高度
        ratio_threshold: 警戒线阈值 (如 0.4 = 40%)
        fill_area: 是否填充面积
    
    Returns:
        go.Figure
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 左轴: 比率 (面积图)
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[ratio_col],
            mode='lines',
            name=ratio_name,
            line=dict(color=THEME['colors']['primary'], width=2),
            fill='tozeroy' if fill_area else None,
            fillcolor='rgba(31, 119, 180, 0.3)' if fill_area else None,
            hovertemplate='%{x|%Y-%m-%d}<br>' + ratio_name + ': %{y:.1%}<extra></extra>'
        ),
        secondary_y=False
    )
    
    # 右轴: 价格 (线图)
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[price_col],
            mode='lines',
            name='价格',
            line=dict(color=THEME['colors']['secondary'], width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>价格: $%{y:,.2f}<extra></extra>'
        ),
        secondary_y=True
    )
    
    # 添加警戒线
    if ratio_threshold is not None:
        fig.add_hline(
            y=ratio_threshold,
            line_dash="dash",
            line_color=THEME['colors']['danger'],
            line_width=1.5,
            annotation_text=f"{ratio_threshold:.0%} 警戒",
            annotation_position="left",
            secondary_y=False
        )
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': {'text': '日期', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': {'text': ratio_name, 'font': THEME['font']}, 'tickformat': '.0%', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis2': {'title': {'text': '价格 (USD)', 'font': THEME['font']}, 'showgrid': False},
    })
    fig.update_layout(**layout)
    
    return fig


def plot_flow_bar(
    df: pd.DataFrame,
    date_col: str = 'date',
    in_col: str = 'delivered_in',
    out_col: str = 'delivered_out',
    title: str = "库存流动分析 (In vs Out)",
    height: int = 400,
    unit: str = 'mt'
) -> go.Figure:
    """
    正负柱状图：入库(正/绿) vs 出库(负/红)
    
    用途: LME Flow Analysis
    
    Args:
        df: 数据框
        date_col: 日期列名
        in_col: 入库列名 (正值)
        out_col: 出库列名 (会被转为负值显示)
        title: 图表标题
        height: 图表高度
        unit: 单位
    
    Returns:
        go.Figure
    """
    fig = go.Figure()
    
    # 入库 (正值, 绿色)
    fig.add_trace(go.Bar(
        x=df[date_col],
        y=df[in_col],
        name='入库 (Delivered In)',
        marker_color=THEME['colors']['success'],
        hovertemplate='%{x|%Y-%m-%d}<br>入库: %{y:,.0f} ' + unit + '<extra></extra>'
    ))
    
    # 出库 (负值, 红色)
    fig.add_trace(go.Bar(
        x=df[date_col],
        y=-df[out_col],  # 转为负值
        name='出库 (Delivered Out)',
        marker_color=THEME['colors']['danger'],
        hovertemplate='%{x|%Y-%m-%d}<br>出库: %{y:,.0f} ' + unit + '<extra></extra>'
    ))
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': {'text': '日期', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': {'text': f'流量 ({unit})', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'barmode': 'relative',
    })
    fig.update_layout(**layout)
    
    return fig


def plot_stacked_area_structure(
    df: pd.DataFrame,
    date_col: str = 'date',
    bottom_col: str = 'eligible',
    top_col: str = 'registered',
    title: str = "库存结构 (Registered vs Eligible)",
    height: int = 400,
    unit: str = 'mt',
    bottom_name: str = 'Eligible',
    top_name: str = 'Registered',
    bottom_color: str = '#999999',
    top_color: str = None
) -> go.Figure:
    """
    堆叠面积图：底层(灰色/非活性) + 顶层(亮色/活性)
    
    用途: COMEX Reg/Elig 结构, COMEX Free/Pledged
    
    Args:
        df: 数据框
        date_col: 日期列名
        bottom_col: 底层数据列名
        top_col: 顶层数据列名
        title: 图表标题
        height: 图表高度
        unit: 单位
        bottom_name: 底层名称
        top_name: 顶层名称
        bottom_color: 底层颜色
        top_color: 顶层颜色 (默认使用主色)
    
    Returns:
        go.Figure
    """
    if top_color is None:
        top_color = THEME['colors']['primary']
    
    fig = go.Figure()
    
    # 底层 (灰色)
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[bottom_col],
        mode='lines',
        name=bottom_name,
        stackgroup='one',
        fillcolor=bottom_color,
        line=dict(width=0.5, color=bottom_color),
        hovertemplate='%{x|%Y-%m-%d}<br>' + bottom_name + ': %{y:,.0f} ' + unit + '<extra></extra>'
    ))
    
    # 顶层 (亮色)
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[top_col],
        mode='lines',
        name=top_name,
        stackgroup='one',
        fillcolor=top_color,
        line=dict(width=0.5, color=top_color),
        hovertemplate='%{x|%Y-%m-%d}<br>' + top_name + ': %{y:,.0f} ' + unit + '<extra></extra>'
    ))
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': {'text': '日期', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': {'text': f'库存量 ({unit})', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
    })
    fig.update_layout(**layout)
    
    return fig


def plot_dual_axis_lines(
    df: pd.DataFrame,
    date_col: str = 'date',
    y1_col: str = 'price',
    y2_col: str = 'open_interest',
    title: str = "价格与持仓量",
    height: int = 400,
    y1_name: str = '价格',
    y2_name: str = '持仓量',
    y1_unit: str = 'USD',
    y2_unit: str = 'mt',
    y1_color: str = None,
    y2_color: str = None
) -> go.Figure:
    """
    双轴线图：两条线分别使用不同Y轴
    
    用途: Price vs OI, SLV vs COMEX Registered
    
    Args:
        df: 数据框
        date_col: 日期列名
        y1_col: 左轴数据列名
        y2_col: 右轴数据列名
        title: 图表标题
        height: 图表高度
        y1_name: 左轴名称
        y2_name: 右轴名称
        y1_unit: 左轴单位
        y2_unit: 右轴单位
        y1_color: 左轴颜色
        y2_color: 右轴颜色
    
    Returns:
        go.Figure
    """
    if y1_color is None:
        y1_color = THEME['colors']['primary']
    if y2_color is None:
        y2_color = THEME['colors']['secondary']
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 左轴
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[y1_col],
            mode='lines',
            name=y1_name,
            line=dict(color=y1_color, width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>' + y1_name + ': %{y:,.2f}<extra></extra>'
        ),
        secondary_y=False
    )
    
    # 右轴
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[y2_col],
            mode='lines',
            name=y2_name,
            line=dict(color=y2_color, width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>' + y2_name + ': %{y:,.0f}<extra></extra>'
        ),
        secondary_y=True
    )
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': {'text': '日期', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': {'text': f'{y1_name} ({y1_unit})', 'font': THEME['font']}, 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis2': {'title': {'text': f'{y2_name} ({y2_unit})', 'font': THEME['font']}, 'showgrid': False},
    })
    fig.update_layout(**layout)
    
    return fig


def plot_fund_flows_bar(
    df: pd.DataFrame,
    date_col: str = 'date',
    change_col: str = 'holdings_change',
    price_col: str = 'price',
    title: str = "ETF 资金流向",
    height: int = 400,
    unit: str = 'oz'
) -> go.Figure:
    """
    资金流向组合图：红绿柱状图(净变化) + 价格线
    
    用途: GLD Fund Flows, LBMA Flows
    
    Args:
        df: 数据框
        date_col: 日期列名
        change_col: 变化量列名 (正=流入绿, 负=流出红)
        price_col: 价格列名
        title: 图表标题
        height: 图表高度
        unit: 单位
    
    Returns:
        go.Figure
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 根据正负值设置颜色
    colors = [THEME['colors']['success'] if v >= 0 else THEME['colors']['danger'] 
              for v in df[change_col]]
    
    # 左轴: 净变化柱状图
    fig.add_trace(
        go.Bar(
            x=df[date_col],
            y=df[change_col],
            name='净流向',
            marker_color=colors,
            hovertemplate='%{x|%Y-%m-%d}<br>变化: %{y:,.0f} ' + unit + '<extra></extra>'
        ),
        secondary_y=False
    )
    
    # 右轴: 价格线
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[price_col],
            mode='lines',
            name='价格',
            line=dict(color=THEME['colors']['secondary'], width=2),
            hovertemplate='%{x|%Y-%m-%d}<br>价格: $%{y:,.2f}<extra></extra>'
        ),
        secondary_y=True
    )
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': '日期', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': f'净变化 ({unit})', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis2': {'title': '价格 (USD)', 'showgrid': False},
    })
    fig.update_layout(**layout)
    
    return fig


def plot_normalized_area(
    df: pd.DataFrame,
    date_col: str = 'date',
    pct1_col: str = 'lbma_pct',
    pct2_col: str = 'comex_pct',
    title: str = "库存占比对比",
    height: int = 400,
    name1: str = 'LBMA',
    name2: str = 'COMEX',
    color1: str = None,
    color2: str = None
) -> go.Figure:
    """
    归一化堆叠面积图 (100% Stacked Area)
    
    用途: LBMA vs COMEX 占比
    
    Args:
        df: 数据框
        date_col: 日期列名
        pct1_col: 占比1列名
        pct2_col: 占比2列名
        title: 图表标题
        height: 图表高度
        name1: 名称1
        name2: 名称2
        color1: 颜色1
        color2: 颜色2
    
    Returns:
        go.Figure
    """
    if color1 is None:
        color1 = THEME['source_colors'].get(name1, THEME['colors']['primary'])
    if color2 is None:
        color2 = THEME['source_colors'].get(name2, THEME['colors']['secondary'])
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[pct1_col],
        mode='lines',
        name=name1,
        stackgroup='one',
        groupnorm='percent',
        fillcolor=color1,
        line=dict(width=0.5, color=color1),
        hovertemplate='%{x|%Y-%m-%d}<br>' + name1 + ': %{y:.1%}<extra></extra>'
    ))
    
    fig.add_trace(go.Scatter(
        x=df[date_col],
        y=df[pct2_col],
        mode='lines',
        name=name2,
        stackgroup='one',
        fillcolor=color2,
        line=dict(width=0.5, color=color2),
        hovertemplate='%{x|%Y-%m-%d}<br>' + name2 + ': %{y:.1%}<extra></extra>'
    ))
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': '日期', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {'title': '占比 (%)', 'tickformat': '.0%', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
    })
    fig.update_layout(**layout)
    
    return fig


def plot_squeeze_divergence(
    df: pd.DataFrame,
    date_col: str = 'date',
    y1_col: str = 'slv_holdings',
    y2_col: str = 'comex_registered',
    title: str = "逼空监控 (SLV vs COMEX Registered)",
    height: int = 400,
    y1_name: str = 'SLV Holdings',
    y2_name: str = 'COMEX Registered',
    y1_unit: str = 'oz',
    y2_unit: str = 'oz'
) -> go.Figure:
    """
    逼空监控双轴图：寻找"鳄鱼大开口"背离
    
    用途: 白银 SLV vs COMEX Registered
    
    Args:
        df: 数据框
        date_col: 日期列名
        y1_col: SLV持仓列名 (左轴)
        y2_col: COMEX注册列名 (右轴)
        title: 图表标题
        height: 图表高度
    
    Returns:
        go.Figure
    """
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # SLV (左轴, 绿色)
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[y1_col],
            mode='lines',
            name=y1_name,
            line=dict(color=THEME['colors']['success'], width=2.5),
            fill='tozeroy',
            fillcolor='rgba(44, 160, 44, 0.2)',
            hovertemplate='%{x|%Y-%m-%d}<br>' + y1_name + ': %{y:,.0f} ' + y1_unit + '<extra></extra>'
        ),
        secondary_y=False
    )
    
    # COMEX Registered (右轴, 红色)
    fig.add_trace(
        go.Scatter(
            x=df[date_col],
            y=df[y2_col],
            mode='lines',
            name=y2_name,
            line=dict(color=THEME['colors']['danger'], width=2.5),
            hovertemplate='%{x|%Y-%m-%d}<br>' + y2_name + ': %{y:,.0f} ' + y2_unit + '<extra></extra>'
        ),
        secondary_y=True
    )
    
    # 布局
    layout = get_base_layout(title, height)
    layout.update({
        'xaxis': {'title': '日期', 'showgrid': True, 'gridcolor': THEME['colors']['grid']},
        'yaxis': {
            # ✅ 修改点 1：title 变成字典，包含 text 和 font
            'title': {
                'text': f'{y1_name} ({y1_unit})',
                'font': {'color': THEME['colors']['success']}
            },
            'showgrid': True,
            'gridcolor': THEME['colors']['grid'],
            'tickfont': {'color': THEME['colors']['success']}
        },
        'yaxis2': {
            # ✅ 修改点 2：title 变成字典，包含 text 和 font
            'title': {
                'text': f'{y2_name} ({y2_unit})',
                'font': {'color': THEME['colors']['danger']}
            },
            'showgrid': False,
            'tickfont': {'color': THEME['colors']['danger']}
        },
    })
    fig.update_layout(**layout)
    
    return fig


# ================= 信号灯卡片 =================
def create_signal_card_html(
    metal: str,
    percentile: float,
    signal: str,
    color: str
) -> str:
    """
    创建信号灯卡片的 HTML
    
    Args:
        metal: 金属名称
        percentile: 分位数
        signal: 信号文本
        color: 信号颜色
    
    Returns:
        str: HTML 字符串
    """
    metal_display = {
        'COPPER': '🟤 铜 (Copper)',
        'GOLD': '🟡 金 (Gold)',
        'SILVER': '⚪ 银 (Silver)'
    }.get(metal, metal)
    
    return f"""
    <div style="
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    ">
        <h3 style="margin: 0 0 10px 0; color: #333;">{metal_display}</h3>
        <h2 style="margin: 0; color: {color}; font-size: 24px;">{signal}</h2>
        <p style="margin: 10px 0 0 0; color: #666; font-size: 18px;">
            分位数: <strong>{percentile:.1%}</strong>
        </p>
    </div>
    """


# ================= 测试入口 =================
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent))
    
    from factors import (
        calculate_global_percentile,
        calculate_regional_percentiles,
        get_price_data,
        get_heatmap_data,
        get_dashboard_signals,
        calculate_source_percentile_trend
    )
    
    print("=" * 60)
    print("绘图工具模块测试")
    print("=" * 60)
    
    # 测试1: 分位数走势图
    print("\n1. 生成分位数走势图 (COPPER)...")
    copper_global = calculate_global_percentile('COPPER')
    fig1 = plot_percentile_trend(copper_global, title="铜 - 全球库存分位走势", metal='COPPER')
    fig1.write_html("test_percentile_trend.html")
    print("   ✓ 已保存: test_percentile_trend.html")
    
    # 测试2: 区域柱状图
    print("\n2. 生成区域分位柱状图 (COPPER)...")
    copper_regional = calculate_regional_percentiles('COPPER')
    fig2 = plot_regional_bar(copper_regional, title="铜 - 分交易所库存分位")
    fig2.write_html("test_regional_bar.html")
    print("   ✓ 已保存: test_regional_bar.html")
    
    # 测试3: 价格走势图
    print("\n3. 生成价格走势图 (GOLD)...")
    gold_price = get_price_data('GOLD')
    fig3 = plot_price_trend(gold_price, title="黄金 - 价格走势", metal='GOLD')
    fig3.write_html("test_price_trend.html")
    print("   ✓ 已保存: test_price_trend.html")
    
    # 测试4: 库存堆叠图
    print("\n4. 生成库存堆叠图 (COPPER)...")
    fig4 = plot_inventory_stacked(
        copper_global, 
        source_cols=['LME', 'COMEX', 'SHFE'],
        title="铜 - 全球库存结构"
    )
    fig4.write_html("test_inventory_stacked.html")
    print("   ✓ 已保存: test_inventory_stacked.html")
    
    # 测试5: 热力图
    print("\n5. 生成热力图...")
    heatmap_data = get_heatmap_data()
    fig5 = plot_heatmap(heatmap_data)
    fig5.write_html("test_heatmap.html")
    print("   ✓ 已保存: test_heatmap.html")
    
    # 测试6: 多来源对比线图
    print("\n6. 生成多来源对比线图 (GOLD)...")
    multi_data = {}
    for source in ['COMEX', 'LBMA', 'GLD']:
        multi_data[source] = calculate_source_percentile_trend('GOLD', source)
    fig6 = plot_multi_source_percentile(multi_data, title="黄金 - 分交易所分位走势对比")
    fig6.write_html("test_multi_source.html")
    print("   ✓ 已保存: test_multi_source.html")
    
    print("\n" + "=" * 60)
    print("测试完成! 请在浏览器中打开 test_*.html 查看图表")
    print("=" * 60)
