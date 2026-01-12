"""
GLD ETF 每日数据获取与清洗工具
- 每日下载最新的GLD持仓数据
- 只保留近一个月的数据
- 清洗并转换为标准化长格式
- 存入数据库
"""

import requests
import pandas as pd
import numpy as np
import hashlib
import os
import sys
from datetime import datetime, timedelta
from io import StringIO

# 添加项目根目录到路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from db_utils import DatabaseSession


def calculate_checksum(data_bytes):
    """计算数据的MD5校验和"""
    hash_md5 = hashlib.md5()
    hash_md5.update(data_bytes)
    return hash_md5.hexdigest()


def download_gld_data():
    """下载GLD ETF数据"""
    url = "https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.spdrgoldshares.com/usa/historical-data/",
        "Accept": "text/csv,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始下载 GLD 数据...")

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        print(f"[OK] 下载成功！数据大小: {len(response.content)} 字节")
        return response.content
        
    except Exception as e:
        print(f"[FAIL] 下载失败: {e}")
        return None


def clean_gld_data(raw_data, days_back=30):
    """
    清洗GLD数据，只保留最近N天的数据
    严格遵守gld_clean.py的清洗方法和格式
    
    Parameters:
    -----------
    raw_data : bytes
        原始CSV数据
    days_back : int
        保留最近多少天的数据（默认30天）
    
    Returns:
    --------
    pd.DataFrame
        清洗后的长格式数据
    """
    
    # 生成load_run_id
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 计算原始数据校验和
    raw_checksum = calculate_checksum(raw_data)
    
    # 读取CSV数据
    try:
        csv_data = raw_data.decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
    except Exception as e:
        print(f"读取CSV失败: {e}")
        return None
    
    # 清理列名
    df.columns = df.columns.str.strip()
    
    # 选择并重命名列
    df_clean = df[['Date', 'Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT']].copy()
    df_clean.columns = ['Date', 'gld_holdings_oz']
    
    # 转换日期列
    df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%d-%b-%Y', errors='coerce')
    
    # 移除日期为NaT的行（HOLIDAY行）
    df_clean = df_clean.dropna(subset=['Date'])
    
    # 计算起始日期（最近N天）
    cutoff_date = datetime.now() - timedelta(days=days_back)
    df_clean = df_clean[df_clean['Date'] >= cutoff_date]
    
    # 转换持仓量为数值
    df_clean['gld_holdings_oz'] = pd.to_numeric(df_clean['gld_holdings_oz'], errors='coerce')
    
    # 移除持仓量为NaN的行
    df_clean = df_clean.dropna(subset=['gld_holdings_oz'])
    
    # 按日期排序
    df_clean = df_clean.sort_values('Date').reset_index(drop=True)
    
    # 检查重复日期
    df_clean['is_duplicate'] = df_clean['Date'].duplicated(keep='first')
    
    # 初始化观测列表
    observations = []
    
    # 跟踪前一个日期用于单调性检查
    prev_date = None
    
    # 处理每一行并转换为长格式
    for idx, row in df_clean.iterrows():
        as_of_date = row['Date']
        value = row['gld_holdings_oz']
        is_duplicate = row['is_duplicate']
        
        # 确定质量
        quality = 'ok'
        quality_notes = None
        
        # 检查重复
        if is_duplicate:
            quality = 'warn'
            quality_notes = f'Duplicate date: {as_of_date.date()}'
            # 跳过重复行
            continue
        
        # 检查非单调日期
        if prev_date is not None and as_of_date <= prev_date:
            quality = 'warn'
            quality_notes = f'Non-monotonic date: {as_of_date.date()} <= {prev_date.date()}'
        
        prev_date = as_of_date
        
        # 创建观测记录
        obs = {
            'metal': 'GOLD',
            'source': 'GLD',
            'freq': 'D',
            'as_of_date': as_of_date,
            'metric': 'gld_holdings_oz',
            'value': value,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'daily',
            'quality': quality,
            'quality_notes': quality_notes,
            'load_run_id': load_run_id,
            'raw_file': 'GLD_US_archive_EN.csv',
            'raw_checksum': raw_checksum
        }
        
        observations.append(obs)
    
    # 转换为DataFrame
    clean_df = pd.DataFrame(observations)
    
    # 按日期排序
    clean_df = clean_df.sort_values('as_of_date').reset_index(drop=True)
    
    return clean_df


def main():
    """主函数"""
    print("\n" + "#"*60)
    print("# GLD ETF 每日数据获取与清洗工具")
    print(f"# 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("#"*60)
    
    # 1. 下载数据
    raw_data = download_gld_data()
    
    if raw_data is None:
        print("\n[FAIL] 数据下载失败")
        return None
    
    # 2. 清洗数据（只保留最近30天）
    print("\n开始清洗数据...")
    clean_df = clean_gld_data(raw_data, days_back=30)
    
    if clean_df is None or clean_df.empty:
        print("[FAIL] 数据清洗失败")
        return None
    
    print(f"[OK] 成功清洗 {len(clean_df)} 条观测记录")
    print(f"   日期范围: {clean_df['as_of_date'].min().strftime('%Y-%m-%d')} 至 {clean_df['as_of_date'].max().strftime('%Y-%m-%d')}")
    
    # 质量汇总
    quality_summary = clean_df['quality'].value_counts()
    print("\n质量检查结果:")
    for quality, count in quality_summary.items():
        status = "[OK]" if quality == 'ok' else "[WARN]"
        print(f"  {status} {quality}: {count} 条")
    
    # 显示警告详情
    warnings = clean_df[clean_df['quality'] == 'warn']
    if not warnings.empty:
        print("\n警告详情:")
        unique_notes = warnings['quality_notes'].unique()
        for note in unique_notes[:5]:  # 显示前5个
            if note:
                print(f"  - {note}")
    
    # 3. 保存到本地CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'clean_observations.csv')
    clean_df.to_csv(output_file, index=False)
    print(f"\n[SAVE] 清洗后数据已保存至: {output_file}")
    
    # 4. 存入数据库
    print("\n[DB] 正在存入数据库...")
    with DatabaseSession("daily_fetch_gld.py") as db:
        db.save(clean_df)
    
    print("\n" + "="*60)
    print("[OK] GLD数据每日更新完成！（已同步到数据库）")
    print("="*60)
    
    return clean_df


if __name__ == "__main__":
    main()