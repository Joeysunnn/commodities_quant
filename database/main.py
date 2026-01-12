"""
数据清洗主程序
使用 db_utils 模块将各数据源的清洗数据存入数据库

使用方法:
---------
1. 运行此文件批量处理所有数据源
2. 或在各个清洗程序中单独调用 db_utils

示例（在清洗程序中使用）:
------------------------
from db_utils import save_to_database, DatabaseSession

# 方法1: 直接保存
df_clean = clean_my_data(...)
save_to_database(df_clean, "my_clean_script.py")

# 方法2: 使用上下文管理器（推荐）
with DatabaseSession("my_clean_script.py") as db:
    df_clean = clean_my_data(...)
    db.save(df_clean)
"""

import os
import sys

# 添加项目根目录到路径
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT_DIR)

from db_utils import save_to_database, DatabaseSession, test_connection, save_from_csv


def process_all_sources():
    """
    处理所有数据源并存入数据库
    """
    print("=" * 60)
    print("批量数据入库程序")
    print("=" * 60)
    
    # 测试数据库连接
    if not test_connection():
        print("数据库连接失败，请检查配置")
        return
    
    # 定义所有数据源的清洗数据路径
    data_sources = {
        'COMEX': os.path.join(ROOT_DIR, 'rawdata', 'comex', 'clean_observations.csv'),
        'LBMA': os.path.join(ROOT_DIR, 'rawdata', 'LBMA', 'clean_observations.csv'),
        'GLD': os.path.join(ROOT_DIR, 'rawdata', 'GLD', 'clean_observations.csv'),
        'SLV': os.path.join(ROOT_DIR, 'rawdata', 'SLV', 'clean_observations.csv'),
        'LME': os.path.join(ROOT_DIR, 'rawdata', 'LME', 'clean_observations.csv'),
        'SHEX': os.path.join(ROOT_DIR, 'rawdata', 'shex', 'clean_observations.csv'),
        'LME_3M_Price': os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'LME_3M', 'clean_observations.csv'),
        'XAUUSD': os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'XAUUSD', 'clean_observations.csv'),
        'XAGUSD': os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'XAGUSD', 'clean_observations.csv'),
    }
    
    # 使用上下文管理器批量处理
    with DatabaseSession("batch_import_all_sources") as db:
        for source_name, csv_path in data_sources.items():
            print(f"\n处理 {source_name}...")
            
            if not os.path.exists(csv_path):
                print(f"  ⚠ 文件不存在: {csv_path}")
                continue
            
            try:
                import pandas as pd
                df = pd.read_csv(csv_path)
                db.save(df)
                print(f"  ✓ {source_name}: {len(df)} 行数据已入库")
            except Exception as e:
                print(f"  ✗ {source_name} 处理失败: {e}")
    
    print("\n" + "=" * 60)
    print("批量入库完成！")
    print("=" * 60)


def demo_usage():
    """
    演示如何在清洗程序中使用 db_utils
    """
    print("\n" + "=" * 60)
    print("db_utils 使用演示")
    print("=" * 60)
    
    print("""
【方式1】在清洗程序末尾直接调用:
--------------------------------
# 在 comex_clean.py 中
from db_utils import save_to_database

def main():
    # ... 清洗逻辑 ...
    clean_df = clean_comex_data(...)
    
    # 直接存入数据库（无需指定 CSV 路径）
    save_to_database(clean_df, "comex_clean.py")
    
    return clean_df

【方式2】使用上下文管理器（推荐）:
--------------------------------
from db_utils import DatabaseSession

def main():
    with DatabaseSession("lbma_clean.py") as db:
        clean_df = clean_lbma_data(...)
        db.save(clean_df)  # 直接存入数据库
    
    return clean_df

【方式3】快速保存（无日志，适合测试）:
------------------------------------
from db_utils import quick_save

df = pd.DataFrame(...)  # 测试数据
quick_save(df)

【方式4】从 CSV 文件保存（兼容旧方式）:
-------------------------------------
from db_utils import save_from_csv
save_from_csv("path/to/clean_observations.csv")
    """)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='数据入库工具')
    parser.add_argument('--all', action='store_true', help='处理所有数据源')
    parser.add_argument('--demo', action='store_true', help='显示使用方法')
    parser.add_argument('--test', action='store_true', help='测试数据库连接')
    parser.add_argument('--csv', type=str, help='指定 CSV 文件路径入库')
    
    args = parser.parse_args()
    
    if args.test:
        test_connection()
    elif args.all:
        process_all_sources()
    elif args.csv:
        save_from_csv(args.csv)
    elif args.demo:
        demo_usage()
    else:
        # 默认显示帮助
        print("数据入库工具")
        print("-" * 40)
        print("用法:")
        print("  python main.py --test     测试数据库连接")
        print("  python main.py --all      处理所有数据源")
        print("  python main.py --csv PATH 指定CSV文件入库")
        print("  python main.py --demo     显示使用方法")
        print("\n或在清洗程序中直接导入 db_utils 模块使用")
