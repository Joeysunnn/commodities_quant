#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
上海期货交易所铜库存周报 - 自动更新脚本
每次运行自动抓取最近2周的数据，清洗后追加到clean_observations.csv
避免重复数据，自动去重
"""

import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# 添加项目根目录到路径，以便导入 db_utils
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

# 导入抓取模块
from grab_api import SHFEAPIFetcher

# 导入清洗模块
from shex_clean import clean_shfe_data

# 导入数据库工具模块
from db_utils import DatabaseSession


def get_latest_three_weeks():
    """
    获取最近2周的日期范围（从最近的周五往前推2周）
    返回: (start_date_str, end_date_str) 格式: 'YYYY-MM-DD'
    """
    today = datetime.now()
    
    # 找到最近的周五（包括今天）
    days_since_friday = (today.weekday() - 4) % 7
    if days_since_friday == 0 and today.hour < 18:  # 如果是周五但还没到晚上，往前推一周
        last_friday = today - timedelta(days=7)
    else:
        last_friday = today - timedelta(days=days_since_friday)
    
    # 往前推2周（14天）
    start_date = last_friday - timedelta(days=14)
    end_date = today
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def fetch_recent_data(output_file='temp_weekly_stock_data.csv'):
    """
    抓取最近2周的数据
    返回: 抓取的数据文件路径，如果失败返回None
    """
    print("=" * 70)
    print("步骤 1/3: 抓取最近2周的数据")
    print("=" * 70)
    
    start_date, end_date = get_latest_three_weeks()
    print(f"\n时间范围: {start_date} 至 {end_date}")
    print(f"说明: 抓取最近2周数据以避免遗漏\n")
    
    # 初始化抓取器
    fetcher = SHFEAPIFetcher()
    
    # 批量抓取数据
    results = fetcher.fetch_data_range(start_date, end_date)
    
    if not results:
        print("\n✗ 未获取到任何数据")
        return None
    
    # 保存到临时文件
    fetcher.save_to_csv(results, output_file)
    
    return output_file


def clean_recent_data(raw_file, output_file='temp_clean_observations.csv'):
    """
    清洗抓取的原始数据
    返回: 清洗后的DataFrame
    """
    print("\n" + "=" * 70)
    print("步骤 2/3: 清洗数据")
    print("=" * 70)
    
    if not os.path.exists(raw_file):
        print(f"✗ 原始数据文件 {raw_file} 不存在")
        return None
    
    # 读取原始数据，检查是否为空
    raw_df = pd.read_csv(raw_file, encoding='utf-8-sig')
    if len(raw_df) == 0:
        print("✗ 原始数据文件为空")
        return None
    
    print(f"原始数据记录数: {len(raw_df)}")
    print(f"日期范围: {raw_df['日期'].min()} 至 {raw_df['日期'].max()}")
    
    # 清洗数据
    clean_df = clean_shfe_data(
        input_file=raw_file,
        freq='W',
        unit='mt',
        method='w_fri_last'
    )
    
    print(f"\n清洗后观测数: {len(clean_df)}")
    print(f"  - 实际数据点: {(~clean_df['is_imputed']).sum()}")
    print(f"  - 前向填充点: {clean_df['is_imputed'].sum()}")
    
    # 保存到临时文件
    clean_df.to_csv(output_file, index=False)
    
    return clean_df


def append_to_clean_csv(new_data_df, target_file='clean_observations.csv'):
    """
    将新数据追加到clean_observations.csv，自动去重
    
    参数:
    - new_data_df: 新的清洗后的DataFrame
    - target_file: 目标文件路径
    
    返回: 合并后的DataFrame
    """
    print("\n" + "=" * 70)
    print("步骤 3/3: 追加到clean_observations.csv并去重")
    print("=" * 70)
    
    # 检查目标文件是否存在
    if os.path.exists(target_file):
        try:
            existing_df = pd.read_csv(target_file)
            print(f"\n已有数据记录数: {len(existing_df)}")
            
            # 转换日期列为datetime以便比较
            existing_df['as_of_date'] = pd.to_datetime(existing_df['as_of_date'])
            new_data_df['as_of_date'] = pd.to_datetime(new_data_df['as_of_date'])
            
            # 显示已有数据的日期范围
            print(f"已有数据日期范围: {existing_df['as_of_date'].min().date()} 至 {existing_df['as_of_date'].max().date()}")
            
            # 合并数据
            combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
            
            # 去重：基于 as_of_date 和 metric 去重，保留最新的 load_run_id
            print("\n执行去重...")
            before_dedup = len(combined_df)
            
            # 按 load_run_id 排序（降序），这样在drop_duplicates时会保留最新的
            combined_df = combined_df.sort_values('load_run_id', ascending=False)
            
            # 根据 as_of_date 和 metric 去重
            combined_df = combined_df.drop_duplicates(
                subset=['as_of_date', 'metric'],
                keep='first'  # 保留第一个（即最新的load_run_id）
            )
            
            after_dedup = len(combined_df)
            duplicates_removed = before_dedup - after_dedup
            
            print(f"  去重前: {before_dedup} 条")
            print(f"  去重后: {after_dedup} 条")
            print(f"  移除重复: {duplicates_removed} 条")
            
        except Exception as e:
            print(f"✗ 读取现有文件失败: {e}")
            print(f"将创建新文件")
            combined_df = new_data_df.copy()
            combined_df['as_of_date'] = pd.to_datetime(combined_df['as_of_date'])
    else:
        print(f"\n文件 {target_file} 不存在，将创建新文件")
        combined_df = new_data_df.copy()
        combined_df['as_of_date'] = pd.to_datetime(combined_df['as_of_date'])
    
    # 按日期和metric排序
    combined_df = combined_df.sort_values(['as_of_date', 'metric']).reset_index(drop=True)
    
    # 保存到文件
    combined_df.to_csv(target_file, index=False)
    
    print(f"\n[OK] 数据已保存到 {target_file}")
    print(f"  总记录数: {len(combined_df)}")
    print(f"  日期范围: {combined_df['as_of_date'].min().date()} 至 {combined_df['as_of_date'].max().date()}")
    
    # 显示最近的数据
    print("\n最近5个日期的数据 (shfe_total_mt):")
    recent_data = combined_df[combined_df['metric'] == 'shfe_total_mt'].tail(5)
    for _, row in recent_data.iterrows():
        imputed_flag = " (前向填充)" if row['is_imputed'] else ""
        print(f"  {row['as_of_date'].date()}: {row['value']:,.0f} {row['unit']}{imputed_flag}")
    
    return combined_df


def cleanup_temp_files():
    """清理临时文件"""
    temp_files = ['temp_weekly_stock_data.csv', 'temp_clean_observations.csv']
    for file in temp_files:
        if os.path.exists(file):
            try:
                os.remove(file)
                print(f"[OK] 已清理临时文件: {file}")
            except Exception as e:
                print(f"[WARN] 无法删除临时文件 {file}: {e}")


def main():
    """主函数"""
    print("=" * 70)
    print("上海期货交易所铜库存周报 - 自动更新")
    print("=" * 70)
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 获取脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    try:
        # 步骤1: 抓取最近3周的数据
        raw_file = fetch_recent_data('temp_weekly_stock_data.csv')
        if not raw_file:
            print("\n[FAIL] 抓取数据失败")
            return
        
        # 步骤2: 清洗数据
        clean_df = clean_recent_data(raw_file, 'temp_clean_observations.csv')
        if clean_df is None or len(clean_df) == 0:
            print("\n[FAIL] 清洗数据失败")
            cleanup_temp_files()
            return
        
        # 步骤3: 追加到clean_observations.csv并去重
        final_df = append_to_clean_csv(clean_df, 'clean_observations.csv')
        
        # 步骤4: 存入数据库
        print("\n" + "=" * 70)
        print("步骤 4/4: 存入数据库")
        print("=" * 70)
        
        with DatabaseSession("shex_auto_update.py") as db:
            db.save(clean_df)
        
        # 清理临时文件
        print("\n清理临时文件...")
        cleanup_temp_files()
        
        print("\n" + "=" * 70)
        print("[OK] 自动更新完成！（已同步到数据库）")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n[FAIL] 发生错误: {e}")
        import traceback
        traceback.print_exc()
        cleanup_temp_files()


if __name__ == '__main__':
    main()
