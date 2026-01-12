"""
策略引擎模块 (Strategy Engine)
================================
第一阶段：信号开发 (Signal Generation)

将 0-1 的因子分位数翻译成交易信号:
- LONG  = +1 (做多)
- FLAT  =  0 (空仓)
- SHORT = -1 (做空)

策略类型:
1. 趋势策略 (Beta Strategy) - 基于全球库存分位
2. 套利策略 (Arbitrage Strategy) - 基于交易所价差
3. 事件驱动策略 (Event Strategy) - 基于逼空监控
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import IntEnum
import sys

# 添加项目根目录到路径
ROOT_DIR = Path(__file__).parent
sys.path.insert(0, str(ROOT_DIR))

from factors import (
    calculate_global_percentile,
    calculate_regional_percentiles,
    calculate_source_percentile_trend,
    get_slv_vs_comex_squeeze,
    get_metric_series,
    get_price_series_from_db,
    METAL_CONFIG,
)


# ================= 信号定义 =================
class Signal(IntEnum):
    """交易信号枚举"""
    SHORT = -1  # 做空
    FLAT = 0    # 空仓
    LONG = 1    # 做多


# ================= 策略参数配置 =================
@dataclass
class BetaStrategyParams:
    """
    趋势策略参数 (Beta Strategy)
    基于全球库存分位数
    """
    # 做多阈值
    long_entry: float = 0.05     # 分位 < 5% 触发做多
    long_exit: float = 0.30      # 分位回升到 > 30% 平仓
    
    # 做空阈值
    short_entry: float = 0.95    # 分位 > 95% 触发做空
    short_exit: float = 0.70     # 分位回落到 < 70% 平仓
    
    # 策略名称
    name: str = "Beta_Inventory"


@dataclass
class ArbitrageStrategyParams:
    """
    套利策略参数 (Arbitrage Strategy)
    基于交易所价差 (COMEX vs LME)
    """
    # 价差阈值
    spread_long_entry: float = -0.20   # COMEX - LME < -20% 做多价差
    spread_long_exit: float = 0.0      # 价差回归到0附近平仓
    spread_short_entry: float = 0.20   # COMEX - LME > 20% 做空价差
    spread_short_exit: float = 0.0     # 价差回归到0附近平仓
    
    # 策略名称
    name: str = "Arbitrage_Spread"


@dataclass
class EventStrategyParams:
    """
    事件驱动策略参数 (Event Strategy)
    白银逼空监控 (SLV vs COMEX)
    """
    # 背离度阈值 (标准化后的divergence)
    divergence_long_entry: float = 1.5    # 背离度 > 1.5σ 做多
    divergence_long_exit: float = 0.5     # 背离度回落到 < 0.5σ 平仓
    
    # SLV变化率阈值 (可选辅助条件)
    slv_change_threshold: float = 0.02    # SLV单日增幅 > 2%
    comex_change_threshold: float = -0.02 # COMEX单日降幅 > 2%
    
    # 策略名称
    name: str = "Event_Squeeze"


# ================= 策略基类 =================
class BaseStrategy:
    """策略基类"""
    
    def __init__(self, metal: str, name: str = "BaseStrategy"):
        self.metal = metal
        self.name = name
        self._position = Signal.FLAT
        self._signals: pd.DataFrame = pd.DataFrame()
    
    @property
    def position(self) -> Signal:
        """当前持仓"""
        return self._position
    
    @property
    def signals(self) -> pd.DataFrame:
        """历史信号序列"""
        return self._signals
    
    def generate_signals(self) -> pd.DataFrame:
        """生成信号序列 (子类实现)"""
        raise NotImplementedError
    
    def get_current_signal(self) -> Dict:
        """获取最新信号"""
        if self._signals.empty:
            self.generate_signals()
        
        if self._signals.empty:
            return {
                'metal': self.metal,
                'strategy': self.name,
                'signal': Signal.FLAT,
                'signal_name': 'FLAT',
                'date': datetime.now().date(),
                'reason': '无数据'
            }
        
        latest = self._signals.iloc[-1]
        signal_val = int(latest.get('signal', 0))
        
        return {
            'metal': self.metal,
            'strategy': self.name,
            'signal': signal_val,
            'signal_name': Signal(signal_val).name,
            'date': latest.get('date', datetime.now().date()),
            'reason': latest.get('reason', '')
        }


# ================= 策略1: 趋势策略 (Beta) =================
class BetaStrategy(BaseStrategy):
    """
    趋势策略 (Beta Strategy)
    
    逻辑: 库存是价格的反向指标，库存极低时价格易涨难跌
    
    规则:
    - 做多: Global Inventory Percentile < long_entry (默认10%)
    - 平仓: Global Inventory Percentile > long_exit (默认30%)
    - 做空: Global Inventory Percentile > short_entry (默认90%)
    - 平仓: Global Inventory Percentile < short_exit (默认70%)
    """
    
    def __init__(self, metal: str, params: BetaStrategyParams = None):
        super().__init__(metal, name="Beta_Inventory")
        self.params = params or BetaStrategyParams()
        self.name = self.params.name
    
    def generate_signals(self) -> pd.DataFrame:
        """
        生成趋势策略信号
        
        Returns:
            pd.DataFrame: 包含 date, percentile, signal, reason 的数据框
        """
        # 获取全球库存分位数
        global_pct = calculate_global_percentile(self.metal)
        
        if global_pct.empty:
            self._signals = pd.DataFrame()
            return self._signals
        
        # 初始化信号列
        signals = []
        position = Signal.FLAT
        
        for idx, row in global_pct.iterrows():
            date = row['date']
            pct = row['percentile']
            
            # 处理NaN
            if pd.isna(pct):
                signals.append({
                    'date': date,
                    'percentile': pct,
                    'signal': position,
                    'reason': '数据缺失，维持仓位'
                })
                continue
            
            reason = ''
            
            # 状态机逻辑
            if position == Signal.FLAT:
                # 空仓状态：检查是否触发开仓
                if pct < self.params.long_entry:
                    position = Signal.LONG
                    reason = f'库存分位 {pct:.1%} < {self.params.long_entry:.0%}，触发做多'
                elif pct > self.params.short_entry:
                    position = Signal.SHORT
                    reason = f'库存分位 {pct:.1%} > {self.params.short_entry:.0%}，触发做空'
                else:
                    reason = f'库存分位 {pct:.1%}，观望'
                    
            elif position == Signal.LONG:
                # 多头状态：检查是否平仓或反转
                if pct > self.params.long_exit:
                    if pct > self.params.short_entry:
                        position = Signal.SHORT
                        reason = f'库存分位 {pct:.1%} > {self.params.short_entry:.0%}，多翻空'
                    else:
                        position = Signal.FLAT
                        reason = f'库存分位回升到 {pct:.1%} > {self.params.long_exit:.0%}，平多'
                else:
                    reason = f'库存分位 {pct:.1%}，持有多头'
                    
            elif position == Signal.SHORT:
                # 空头状态：检查是否平仓或反转
                if pct < self.params.short_exit:
                    if pct < self.params.long_entry:
                        position = Signal.LONG
                        reason = f'库存分位 {pct:.1%} < {self.params.long_entry:.0%}，空翻多'
                    else:
                        position = Signal.FLAT
                        reason = f'库存分位回落到 {pct:.1%} < {self.params.short_exit:.0%}，平空'
                else:
                    reason = f'库存分位 {pct:.1%}，持有空头'
            
            signals.append({
                'date': date,
                'percentile': pct,
                'signal': int(position),
                'reason': reason
            })
        
        self._signals = pd.DataFrame(signals)
        self._position = position
        
        return self._signals


# ================= 策略2: 套利策略 (Arbitrage) =================
class ArbitrageStrategy(BaseStrategy):
    """
    套利策略 (Arbitrage Strategy)
    
    逻辑: 利用 LME 和 COMEX 的供需错配进行跨市套利
    
    规则:
    - 做多价差: COMEX Pct - LME Pct < -20% (COMEX紧缺，LME充裕)
    - 平仓: 价差回归到 0 附近
    - 做空价差: COMEX Pct - LME Pct > 20% (COMEX充裕，LME紧缺)
    
    操作含义:
    - 做多价差 = 做多COMEX + 做空LME
    - 做空价差 = 做空COMEX + 做多LME
    """
    
    def __init__(self, metal: str = 'COPPER', params: ArbitrageStrategyParams = None):
        super().__init__(metal, name="Arbitrage_Spread")
        self.params = params or ArbitrageStrategyParams()
        self.name = self.params.name
        
        # 验证金属类型 (套利策略主要用于铜)
        if metal not in METAL_CONFIG:
            raise ValueError(f"不支持的金属类型: {metal}")
    
    def _get_source_percentiles(self) -> pd.DataFrame:
        """
        获取各交易所的分位数时间序列
        
        Returns:
            pd.DataFrame: date, comex_pct, lme_pct, spread
        """
        config = METAL_CONFIG[self.metal]
        sources = config['sources']
        
        # 需要 COMEX 和 LME 数据
        required_sources = ['COMEX', 'LME']
        if not all(s in sources for s in required_sources):
            return pd.DataFrame()
        
        # 获取各来源分位数走势
        comex_trend = calculate_source_percentile_trend(self.metal, 'COMEX')
        lme_trend = calculate_source_percentile_trend(self.metal, 'LME')
        
        if comex_trend.empty or lme_trend.empty:
            return pd.DataFrame()
        
        # 合并数据
        comex_trend = comex_trend.set_index('date')[['percentile']].rename(
            columns={'percentile': 'comex_pct'}
        )
        lme_trend = lme_trend.set_index('date')[['percentile']].rename(
            columns={'percentile': 'lme_pct'}
        )
        
        merged = comex_trend.join(lme_trend, how='outer').ffill().dropna()
        merged['spread'] = merged['comex_pct'] - merged['lme_pct']
        merged = merged.reset_index()
        
        return merged
    
    def generate_signals(self) -> pd.DataFrame:
        """
        生成套利策略信号
        
        Returns:
            pd.DataFrame: 包含 date, comex_pct, lme_pct, spread, signal, reason
        """
        pct_data = self._get_source_percentiles()
        
        if pct_data.empty:
            self._signals = pd.DataFrame()
            return self._signals
        
        signals = []
        position = Signal.FLAT
        
        for idx, row in pct_data.iterrows():
            date = row['date']
            comex_pct = row['comex_pct']
            lme_pct = row['lme_pct']
            spread = row['spread']
            
            if pd.isna(spread):
                signals.append({
                    'date': date,
                    'comex_pct': comex_pct,
                    'lme_pct': lme_pct,
                    'spread': spread,
                    'signal': int(position),
                    'reason': '数据缺失，维持仓位'
                })
                continue
            
            reason = ''
            
            # 状态机逻辑
            if position == Signal.FLAT:
                if spread < self.params.spread_long_entry:
                    position = Signal.LONG
                    reason = f'价差 {spread:.1%} < {self.params.spread_long_entry:.0%}，做多价差(多COMEX空LME)'
                elif spread > self.params.spread_short_entry:
                    position = Signal.SHORT
                    reason = f'价差 {spread:.1%} > {self.params.spread_short_entry:.0%}，做空价差(空COMEX多LME)'
                else:
                    reason = f'价差 {spread:.1%}，观望'
                    
            elif position == Signal.LONG:
                # 做多价差状态
                if spread >= self.params.spread_long_exit:
                    if spread > self.params.spread_short_entry:
                        position = Signal.SHORT
                        reason = f'价差 {spread:.1%}，多翻空'
                    else:
                        position = Signal.FLAT
                        reason = f'价差收敛到 {spread:.1%}，平仓'
                else:
                    reason = f'价差 {spread:.1%}，持有多头'
                    
            elif position == Signal.SHORT:
                # 做空价差状态
                if spread <= self.params.spread_short_exit:
                    if spread < self.params.spread_long_entry:
                        position = Signal.LONG
                        reason = f'价差 {spread:.1%}，空翻多'
                    else:
                        position = Signal.FLAT
                        reason = f'价差收敛到 {spread:.1%}，平仓'
                else:
                    reason = f'价差 {spread:.1%}，持有空头'
            
            signals.append({
                'date': date,
                'comex_pct': comex_pct,
                'lme_pct': lme_pct,
                'spread': spread,
                'signal': int(position),
                'reason': reason
            })
        
        self._signals = pd.DataFrame(signals)
        self._position = position
        
        return self._signals


# ================= 策略3: 事件驱动策略 (Event) =================
class EventStrategy(BaseStrategy):
    """
    事件驱动策略 (Event Strategy) - 白银逼空监控
    
    逻辑: 当 SLV Holdings 增加且 COMEX Registered 减少时，
         形成"鳄鱼大开口"，是逼空信号
    
    规则:
    - 做多: 标准化背离度 > divergence_long_entry (默认1.5σ)
    - 平仓: 背离度回落到 < divergence_long_exit (默认0.5σ)
    
    注意: 此策略仅适用于白银 (SILVER)
    """
    
    def __init__(self, params: EventStrategyParams = None):
        super().__init__('SILVER', name="Event_Squeeze")
        self.params = params or EventStrategyParams()
        self.name = self.params.name
    
    def _calculate_squeeze_signals(self) -> pd.DataFrame:
        """
        计算逼空信号数据
        
        Returns:
            pd.DataFrame: date, slv, comex, slv_change, comex_change, divergence
        """
        # 获取SLV和COMEX数据
        squeeze_data = get_slv_vs_comex_squeeze()
        
        if squeeze_data.empty:
            return pd.DataFrame()
        
        df = squeeze_data.copy()
        
        # 计算变化率
        df['slv_change'] = df['slv_holdings'].pct_change()
        df['comex_change'] = df['comex_registered'].pct_change()
        
        return df
    
    def generate_signals(self) -> pd.DataFrame:
        """
        生成事件驱动策略信号
        
        Returns:
            pd.DataFrame: 包含完整信号信息的数据框
        """
        data = self._calculate_squeeze_signals()
        
        if data.empty:
            self._signals = pd.DataFrame()
            return self._signals
        
        signals = []
        position = Signal.FLAT
        
        for idx, row in data.iterrows():
            date = row['date']
            divergence = row['divergence']
            slv_change = row.get('slv_change', 0)
            comex_change = row.get('comex_change', 0)
            
            if pd.isna(divergence):
                signals.append({
                    'date': date,
                    'slv_holdings': row['slv_holdings'],
                    'comex_registered': row['comex_registered'],
                    'divergence': divergence,
                    'signal': int(position),
                    'reason': '数据缺失，维持仓位'
                })
                continue
            
            reason = ''
            
            # 辅助条件：SLV增加 且 COMEX减少
            auxiliary_condition = (
                pd.notna(slv_change) and pd.notna(comex_change) and
                slv_change > self.params.slv_change_threshold and 
                comex_change < self.params.comex_change_threshold
            )
            
            # 状态机逻辑
            if position == Signal.FLAT:
                # 主条件：背离度超过阈值
                if divergence > self.params.divergence_long_entry:
                    position = Signal.LONG
                    reason = f'背离度 {divergence:.2f}σ > {self.params.divergence_long_entry}σ，逼空信号，做多'
                elif auxiliary_condition:
                    # 辅助条件也可触发（较保守）
                    position = Signal.LONG
                    reason = f'SLV增{slv_change:.1%} + COMEX降{comex_change:.1%}，做多'
                else:
                    reason = f'背离度 {divergence:.2f}σ，观望'
                    
            elif position == Signal.LONG:
                # 多头状态：检查是否平仓
                if divergence < self.params.divergence_long_exit:
                    position = Signal.FLAT
                    reason = f'背离度回落到 {divergence:.2f}σ < {self.params.divergence_long_exit}σ，平仓'
                else:
                    reason = f'背离度 {divergence:.2f}σ，持有多头'
            
            signals.append({
                'date': date,
                'slv_holdings': row['slv_holdings'],
                'comex_registered': row['comex_registered'],
                'divergence': divergence,
                'signal': int(position),
                'reason': reason
            })
        
        self._signals = pd.DataFrame(signals)
        self._position = position
        
        return self._signals


# ================= 策略聚合器 =================
class StrategyEngine:
    """
    策略引擎 - 聚合多个策略的信号
    """
    
    def __init__(self):
        self.strategies: Dict[str, BaseStrategy] = {}
    
    def add_strategy(self, strategy: BaseStrategy, key: str = None):
        """添加策略"""
        key = key or f"{strategy.metal}_{strategy.name}"
        self.strategies[key] = strategy
    
    def remove_strategy(self, key: str):
        """移除策略"""
        if key in self.strategies:
            del self.strategies[key]
    
    def generate_all_signals(self) -> Dict[str, pd.DataFrame]:
        """生成所有策略的信号"""
        results = {}
        for key, strategy in self.strategies.items():
            try:
                signals = strategy.generate_signals()
                results[key] = signals
            except Exception as e:
                print(f"策略 {key} 生成信号失败: {e}")
                results[key] = pd.DataFrame()
        return results
    
    def get_all_current_signals(self) -> pd.DataFrame:
        """获取所有策略的当前信号"""
        current_signals = []
        for key, strategy in self.strategies.items():
            try:
                signal = strategy.get_current_signal()
                signal['strategy_key'] = key
                current_signals.append(signal)
            except Exception as e:
                print(f"策略 {key} 获取当前信号失败: {e}")
        
        return pd.DataFrame(current_signals)
    
    def get_signal_summary(self) -> Dict:
        """
        获取信号汇总
        
        Returns:
            dict: {
                'total_strategies': int,
                'long_count': int,
                'short_count': int,
                'flat_count': int,
                'by_metal': {metal: {signal_type: count}}
            }
        """
        signals_df = self.get_all_current_signals()
        
        if signals_df.empty:
            return {
                'total_strategies': 0,
                'long_count': 0,
                'short_count': 0,
                'flat_count': 0,
                'by_metal': {}
            }
        
        summary = {
            'total_strategies': len(signals_df),
            'long_count': (signals_df['signal'] == Signal.LONG).sum(),
            'short_count': (signals_df['signal'] == Signal.SHORT).sum(),
            'flat_count': (signals_df['signal'] == Signal.FLAT).sum(),
            'by_metal': {}
        }
        
        # 按金属分组
        for metal in signals_df['metal'].unique():
            metal_signals = signals_df[signals_df['metal'] == metal]
            summary['by_metal'][metal] = {
                'long': (metal_signals['signal'] == Signal.LONG).sum(),
                'short': (metal_signals['signal'] == Signal.SHORT).sum(),
                'flat': (metal_signals['signal'] == Signal.FLAT).sum(),
            }
        
        return summary


# ================= 便捷函数 =================
def create_default_engine() -> StrategyEngine:
    """
    创建默认策略引擎（包含所有金属的趋势策略 + 铜套利 + 白银事件）
    
    Returns:
        StrategyEngine: 配置好的策略引擎
    """
    engine = StrategyEngine()
    
    # 添加趋势策略 (Beta) - 三个金属
    for metal in ['COPPER', 'GOLD', 'SILVER']:
        engine.add_strategy(BetaStrategy(metal))
    
    # 添加套利策略 - 铜
    engine.add_strategy(ArbitrageStrategy('COPPER'))
    
    # 添加事件驱动策略 - 白银
    engine.add_strategy(EventStrategy())
    
    return engine


def get_quick_signals() -> pd.DataFrame:
    """
    快速获取所有策略的当前信号
    
    Returns:
        pd.DataFrame: 所有策略的当前信号
    """
    engine = create_default_engine()
    return engine.get_all_current_signals()


# ================= 测试入口 =================
if __name__ == "__main__":
    print("=" * 70)
    print("策略引擎测试 - 第一阶段：信号开发")
    print("=" * 70)
    
    # 测试1: 趋势策略 (Beta)
    print("\n" + "=" * 50)
    print("测试1: 趋势策略 (Beta Strategy)")
    print("=" * 50)
    
    for metal in ['COPPER', 'GOLD', 'SILVER']:
        print(f"\n--- {metal} ---")
        try:
            beta = BetaStrategy(metal)
            signals = beta.generate_signals()
            current = beta.get_current_signal()
            
            print(f"信号数量: {len(signals)}")
            print(f"当前信号: {current['signal_name']}")
            print(f"信号原因: {current['reason']}")
            
            if not signals.empty:
                print(f"最近5条信号:")
                print(signals.tail()[['date', 'percentile', 'signal', 'reason']].to_string(index=False))
        except Exception as e:
            print(f"错误: {e}")
    
    # 测试2: 套利策略 (Arbitrage)
    print("\n" + "=" * 50)
    print("测试2: 套利策略 (Arbitrage Strategy) - COPPER")
    print("=" * 50)
    
    try:
        arb = ArbitrageStrategy('COPPER')
        signals = arb.generate_signals()
        current = arb.get_current_signal()
        
        print(f"信号数量: {len(signals)}")
        print(f"当前信号: {current['signal_name']}")
        print(f"信号原因: {current['reason']}")
        
        if not signals.empty:
            print(f"\n最近5条信号:")
            print(signals.tail()[['date', 'spread', 'signal', 'reason']].to_string(index=False))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试3: 事件驱动策略 (Event)
    print("\n" + "=" * 50)
    print("测试3: 事件驱动策略 (Event Strategy) - SILVER")
    print("=" * 50)
    
    try:
        event = EventStrategy()
        signals = event.generate_signals()
        current = event.get_current_signal()
        
        print(f"信号数量: {len(signals)}")
        print(f"当前信号: {current['signal_name']}")
        print(f"信号原因: {current['reason']}")
        
        if not signals.empty:
            print(f"\n最近5条信号:")
            print(signals.tail()[['date', 'divergence', 'signal', 'reason']].to_string(index=False))
    except Exception as e:
        print(f"错误: {e}")
    
    # 测试4: 策略引擎聚合
    print("\n" + "=" * 50)
    print("测试4: 策略引擎聚合 (Strategy Engine)")
    print("=" * 50)
    
    try:
        engine = create_default_engine()
        
        # 生成所有信号
        all_signals = engine.generate_all_signals()
        print(f"\n共 {len(all_signals)} 个策略")
        
        # 获取当前信号汇总
        current_signals = engine.get_all_current_signals()
        print("\n当前信号汇总:")
        print(current_signals[['metal', 'strategy', 'signal_name', 'reason']].to_string(index=False))
        
        # 信号统计
        summary = engine.get_signal_summary()
        print(f"\n信号统计:")
        print(f"  做多: {summary['long_count']}")
        print(f"  做空: {summary['short_count']}")
        print(f"  观望: {summary['flat_count']}")
        
    except Exception as e:
        print(f"错误: {e}")
    
    print("\n" + "=" * 70)
    print("测试完成!")
    print("=" * 70)
