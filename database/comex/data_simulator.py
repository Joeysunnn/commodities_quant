"""
COMEX 历史数据模拟器
用于生成 2021-01-01 至 2025-12-31 的模拟测试数据
数据格式与 clean_observations.csv 保持一致

作者: 数据模拟工具
日期: 2026-01-08
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime, timedelta

# 添加项目根目录到路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from db_utils import DatabaseSession, start_load_run, finish_load_run

# ============== 模拟参数配置 ==============

# 日期范围
START_DATE = "2021-01-01"
END_DATE = "2025-12-18"

# 各金属各指标的基准值和波动范围（参考2026年1月的实际数据量级）
SIMULATION_CONFIG = {
    'GOLD': {
        'unit': 'oz',
        'metrics': {
            'comex_registered_oz': {
                'base': 19_000_000,      # 约1900万盎司
                'volatility': 0.15,       # 15%波动
                'trend': 0.0001           # 微弱上升趋势
            },
            'comex_eligible_oz': {
                'base': 17_000_000,      # 约1700万盎司
                'volatility': 0.12,
                'trend': 0.00005
            },
            'comex_pledged_oz': {
                'base': 1_800_000,       # 约180万盎司
                'volatility': 0.20,
                'trend': 0.0
            },
            'comex_total_oz': {
                'is_sum': True,           # 由其他指标计算得出
                'components': ['comex_registered_oz', 'comex_eligible_oz']
            }
        }
    },
    'SILVER': {
        'unit': 'oz',
        'metrics': {
            'comex_registered_oz': {
                'base': 127_000_000,     # 约1.27亿盎司
                'volatility': 0.10,
                'trend': -0.00005         # 微弱下降趋势
            },
            'comex_eligible_oz': {
                'base': 318_000_000,     # 约3.18亿盎司
                'volatility': 0.08,
                'trend': 0.00003
            },
            'comex_total_oz': {
                'is_sum': True,
                'components': ['comex_registered_oz', 'comex_eligible_oz']
            }
        }
    },
    'COPPER': {
        'unit': 'mt',
        'metrics': {
            'comex_registered_mt': {
                'base': 290_000,          # 约29万公吨
                'volatility': 0.18,
                'trend': 0.00008
            },
            'comex_eligible_mt': {
                'base': 174_000,          # 约17.4万公吨
                'volatility': 0.15,
                'trend': 0.00005
            },
            'comex_total_mt': {
                'is_sum': True,
                'components': ['comex_registered_mt', 'comex_eligible_mt']
            }
        }
    }
}

# 随机种子（确保可重复性）
RANDOM_SEED = 42


class ComexDataSimulator:
    """COMEX 数据模拟器"""
    
    def __init__(self, start_date: str, end_date: str, seed: int = RANDOM_SEED):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.seed = seed
        np.random.seed(seed)
        
        # 生成交易日序列（排除周末）
        self.trading_days = self._generate_trading_days()
        print(f"[INFO] 模拟日期范围: {start_date} 至 {end_date}")
        print(f"[INFO] 共 {len(self.trading_days)} 个交易日")
    
    def _generate_trading_days(self) -> pd.DatetimeIndex:
        """生成交易日序列（排除周末和主要假日）"""
        all_days = pd.date_range(self.start_date, self.end_date, freq='D')
        
        # 排除周末
        trading_days = all_days[all_days.weekday < 5]
        
        # 简单排除一些主要美国假日（简化处理）
        # 新年、独立日、感恩节、圣诞节等
        holidays = []
        for year in range(self.start_date.year, self.end_date.year + 1):
            holidays.extend([
                f"{year}-01-01",  # 新年
                f"{year}-07-04",  # 独立日
                f"{year}-12-25",  # 圣诞节
            ])
        
        holidays = pd.to_datetime(holidays)
        trading_days = trading_days[~trading_days.isin(holidays)]
        
        return trading_days
    
    def _generate_random_walk(self, base: float, volatility: float, 
                               trend: float, n_days: int) -> np.ndarray:
        """
        生成随机游走序列
        
        Parameters:
        -----------
        base : float
            基准值
        volatility : float
            波动率（相对于基准值的比例）
        trend : float
            每日趋势系数
        n_days : int
            天数
        """
        # 生成日收益率
        daily_returns = np.random.normal(trend, volatility / np.sqrt(252), n_days)
        
        # 累积收益
        cumulative_returns = np.cumprod(1 + daily_returns)
        
        # 计算值序列
        values = base * cumulative_returns
        
        # 添加一些均值回归特性
        mean_value = base * (1 + trend * n_days / 2)
        reversion_factor = 0.001
        
        for i in range(1, len(values)):
            deviation = (values[i] - mean_value) / mean_value
            values[i] = values[i] * (1 - reversion_factor * deviation)
        
        # 确保值为正
        values = np.maximum(values, base * 0.3)
        
        return values
    
    def simulate_metal(self, metal: str, load_run_id: int) -> pd.DataFrame:
        """为单个金属生成模拟数据
        
        Parameters:
        -----------
        metal : str
            金属类型 (GOLD, SILVER, COPPER)
        load_run_id : int
            数据库中的加载运行ID
        """
        config = SIMULATION_CONFIG[metal]
        unit = config['unit']
        metrics_config = config['metrics']
        n_days = len(self.trading_days)
        
        print(f"\n[{metal}] 开始模拟 {n_days} 天数据...")
        
        # 先生成非计算字段
        simulated_values = {}
        
        for metric, params in metrics_config.items():
            if not params.get('is_sum', False):
                values = self._generate_random_walk(
                    base=params['base'],
                    volatility=params['volatility'],
                    trend=params['trend'],
                    n_days=n_days
                )
                simulated_values[metric] = values
                print(f"  [{metric}] 均值: {np.mean(values):,.2f}, 范围: {np.min(values):,.2f} - {np.max(values):,.2f}")
        
        # 计算合计字段
        for metric, params in metrics_config.items():
            if params.get('is_sum', False):
                components = params['components']
                total = sum(simulated_values[comp] for comp in components)
                simulated_values[metric] = total
                print(f"  [{metric}] 均值: {np.mean(total):,.2f} (计算自 {components})")
        
        # 构建 DataFrame
        observations = []
        
        for i, date in enumerate(self.trading_days):
            date_str = date.strftime('%Y-%m-%d')
            
            for metric, values in simulated_values.items():
                obs = {
                    'metal': metal,
                    'source': 'COMEX',
                    'freq': 'D',
                    'as_of_date': date_str,
                    'metric': metric,
                    'value': round(values[i], 6),
                    'unit': unit,
                    'is_imputed': False,
                    'method': 'simulated',
                    'quality': 'ok',
                    'quality_notes': 'Simulated data for testing',
                    'load_run_id': load_run_id,
                    'raw_file': f'{metal}_Stocks_SIMULATED.xls',
                    'raw_checksum': 'simulated_data_no_checksum'
                }
                observations.append(obs)
        
        df = pd.DataFrame(observations)
        print(f"  [OK] 生成 {len(df)} 条记录")
        
        return df
    
    def simulate_all(self, load_run_id: int = None) -> pd.DataFrame:
        """生成所有金属的模拟数据
        
        Parameters:
        -----------
        load_run_id : int, optional
            数据库中的加载运行ID，如果不提供将使用0
        """
        if load_run_id is None:
            load_run_id = 0  # 默认值，仅用于CSV生成模式
            
        all_data = []
        
        for metal in SIMULATION_CONFIG.keys():
            df = self.simulate_metal(metal, load_run_id)
            all_data.append(df)
        
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.sort_values(['as_of_date', 'metal', 'metric']).reset_index(drop=True)
        
        return final_df


def print_summary(df: pd.DataFrame):
    """打印模拟数据汇总"""
    print("\n" + "=" * 70)
    print("[SUMMARY] 模拟数据汇总")
    print("=" * 70)
    
    print(f"\n总记录数: {len(df):,}")
    print(f"日期范围: {df['as_of_date'].min()} 至 {df['as_of_date'].max()}")
    
    unique_dates = df['as_of_date'].nunique()
    print(f"交易日数: {unique_dates}")
    
    print("\n按金属和指标分布:")
    for metal in df['metal'].unique():
        metal_df = df[df['metal'] == metal]
        unit = metal_df['unit'].iloc[0]
        print(f"\n  [{metal}] ({unit})")
        
        for metric in metal_df['metric'].unique():
            metric_df = metal_df[metal_df['metric'] == metric]
            values = metric_df['value']
            print(f"    {metric}:")
            print(f"      记录数: {len(metric_df):,}")
            print(f"      均值: {values.mean():,.2f}")
            print(f"      最小: {values.min():,.2f}")
            print(f"      最大: {values.max():,.2f}")


def upload_in_batches(df: pd.DataFrame, batch_size: int = 50):
    """
    分批上传数据到数据库，避免SQL参数限制
    
    注意: PostgreSQL 有参数数量限制，每行14列，所以batch_size=50意味着700个参数
    
    Parameters:
    -----------
    df : pd.DataFrame
        要上传的数据
    batch_size : int
        每批上传的记录数，默认50 (50行 × 14列 = 700参数，安全范围内)
    """
    from sqlalchemy import create_engine
    from db_utils import get_db_url, TABLE_CONFIG, insert_on_conflict_nothing
    
    total_rows = len(df)
    n_batches = (total_rows + batch_size - 1) // batch_size
    
    print(f"[INFO] 总记录数: {total_rows:,}, 分 {n_batches} 批上传，每批 {batch_size} 条")
    
    engine = create_engine(get_db_url())
    
    for i in range(n_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        print(f"  [批次 {i+1}/{n_batches}] 上传记录 {start_idx+1} - {end_idx}...", end=" ")
        
        batch_df.to_sql(
            name=TABLE_CONFIG['name'],
            con=engine,
            schema=TABLE_CONFIG['schema'],
            if_exists='append',
            index=False,
            method=insert_on_conflict_nothing
        )
        print("OK")
    
    print(f"[OK] 全部 {total_rows:,} 条记录上传完成！")


def main():
    """主函数"""
    print("\n" + "#" * 70)
    print("#  COMEX 历史数据模拟器")
    print("#  生成 2021-01-01 至 2025-12-31 的测试数据")
    print("#" * 70)
    
    # 创建模拟器
    simulator = ComexDataSimulator(START_DATE, END_DATE)
    
    # 生成模拟数据
    print("\n开始生成模拟数据...")
    df_simulated = simulator.simulate_all()
    
    # 打印汇总
    print_summary(df_simulated)
    
    # 保存到本地 CSV（备份）
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'simulated_observations.csv')
    df_simulated.to_csv(output_file, index=False)
    print(f"\n[SAVE] 模拟数据已保存至: {output_file}")
    
    # 询问是否上传到数据库
    print("\n" + "=" * 70)
    user_input = input("是否将模拟数据上传到数据库? (y/n): ").strip().lower()
    
    if user_input == 'y':
        print("\n[DB] 正在分批上传到数据库...")
        upload_in_batches(df_simulated, batch_size=2000)
        
        print("\n" + "=" * 70)
        print("[OK] 模拟数据已成功上传到数据库！")
        print("=" * 70)
    else:
        print("\n[SKIP] 已跳过数据库上传")
        print("如需稍后上传，可使用:")
        print("  from db_utils import save_from_csv")
        print(f"  save_from_csv('{output_file}')")
    
    return df_simulated


def upload_only():
    """仅上传已生成的模拟数据到数据库"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file = os.path.join(script_dir, 'simulated_observations.csv')
    
    if not os.path.exists(csv_file):
        print(f"[ERROR] 找不到模拟数据文件: {csv_file}")
        print("请先运行 main() 生成模拟数据")
        return
    
    print(f"[INFO] 正在读取: {csv_file}")
    df = pd.read_csv(csv_file)
    print(f"[INFO] 共 {len(df):,} 条记录")
    
    # 创建数据库加载运行记录，获取真正的整数 load_run_id
    print("\n[DB] 创建加载运行记录...")
    run_id = start_load_run("COMEX_data_simulator_upload")
    
    # 更新 DataFrame 中的 load_run_id
    df['load_run_id'] = run_id
    
    print("\n[DB] 正在分批上传到数据库...")
    try:
        upload_in_batches(df)  # 使用默认的小批次大小
        finish_load_run(run_id, 'success')
        print("\n[OK] 上传完成！")
    except Exception as e:
        finish_load_run(run_id, 'failed', str(e))
        print(f"\n[ERROR] 上传失败: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='COMEX 数据模拟器')
    parser.add_argument('--upload-only', action='store_true', 
                        help='仅上传已生成的CSV到数据库')
    
    args = parser.parse_args()
    
    if args.upload_only:
        upload_only()
    else:
        main()
