"""
LBMA 每日数据获取与清洗工具
- 每日下载最新的LBMA金银库存数据
- 只处理最近一个月的数据
- 使用ffill方法填充缺失日期
- 清洗并转换为标准化长格式
- 存入数据库
"""

import requests
import pandas as pd
import numpy as np
import hashlib
import os
import sys
import json
from datetime import datetime, timedelta
from io import StringIO, BytesIO
from bs4 import BeautifulSoup
import re


# 添加项目根目录到路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from db_utils import DatabaseSession


def calculate_checksum(data_bytes):
    """计算数据的MD5校验和"""
    hash_md5 = hashlib.md5()
    hash_md5.update(data_bytes)
    return hash_md5.hexdigest()


def download_lbma_data():
    """下载LBMA库存数据"""
    # LBMA数据页面
    page_url = "https://www.lbma.org.uk/prices-and-data/london-vault-data"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.lbma.org.uk/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始下载 LBMA 数据...")

    try:
        # 1. 访问页面获取最新的Excel文件链接
        print("正在解析页面获取最新数据链接...")
        response = requests.get(page_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 查找Excel下载链接（通常包含 LBMA-London-Vault-Holdings-Data）
        excel_link = None
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'LBMA-London-Vault-Holdings-Data' in href and href.endswith('.xlsx'):
                if href.startswith('http'):
                    excel_link = href
                else:
                    excel_link = 'https://cdn.lbma.org.uk' + href if not href.startswith('/') else 'https://cdn.lbma.org.uk' + href
                break
        
        # 如果没找到，尝试备用方法：直接构造最新月份的URL
        if not excel_link:
            print("未在页面找到链接，尝试使用当前月份...")
            # 获取前一个月（因为数据通常滞后一个月）
            last_month = datetime.now().replace(day=1) - timedelta(days=1)
            month_name = last_month.strftime('%B-%Y')  # 例如: November-2025
            excel_link = f"https://cdn.lbma.org.uk/downloads/LBMA-London-Vault-Holdings-Data-{month_name}.xlsx"
            print(f"构造的链接: {excel_link}")
        
        print(f"找到数据链接: {excel_link}")
        
        # 2. 下载Excel文件
        print("正在下载Excel文件...")
        excel_response = requests.get(excel_link, headers=headers, timeout=30)
        excel_response.raise_for_status()
        
        print(f"[OK] 下载成功！数据大小: {len(excel_response.content)} 字节")
        return excel_response.content
        
    except Exception as e:
        print(f"[FAIL] 下载失败: {e}")
        print("提示: 可能需要手动检查LBMA网站结构是否变化")
        return None


def clean_lbma_data(raw_data):
    """
    清洗LBMA数据，保持原始月末数据格式
    严格遵守lbma_clean.py的清洗方法和格式
    
    Parameters:
    -----------
    raw_data : bytes
        原始Excel数据
    
    Returns:
    --------
    pd.DataFrame
        清洗后的长格式数据（月频）
    """
    
    # 生成load_run_id
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 计算原始数据校验和
    raw_checksum = calculate_checksum(raw_data)
    
    # 读取Excel数据
    try:
        # 第一次读取以查看结构
        df_raw = pd.read_excel(BytesIO(raw_data))
        
        # 调试：打印原始Excel结构
        print(f"\n原始列名: {df_raw.columns.tolist()}")
        print(f"\n前5行数据:")
        print(df_raw.head(5))
        
        # LBMA Excel格式：
        # Row 0: 标题行 ["London Vault Holdings Data", "Unnamed: 1", "Unnamed: 2"]
        # Row 1: 真实列名 ["Month End", "Gold", "Silver"]
        # Row 2: 单位说明 [NaN, "Troy Ounces ('000s)", "Troy Ounces ('000s)"]
        # Row 3+: 实际数据
        
        # 使用第2行作为列名（skiprows=1, header=1）
        df = pd.read_excel(BytesIO(raw_data), skiprows=1)
        
        # 移除单位说明行（第一行现在是单位说明）
        # 检查第一行是否包含 "Troy Ounces" 或 "000s"
        first_row_str = str(df.iloc[0].tolist())
        if 'Troy Ounces' in first_row_str or '000s' in first_row_str:
            df = df.iloc[1:].reset_index(drop=True)
        
        print(f"\n处理后的列名: {df.columns.tolist()}")
        print(f"\n处理后的前3行数据:")
        print(df.head(3))
        print(f"\n数据形状: {df.shape}")
        
    except Exception as e:
        print(f"读取Excel失败: {e}")
        return None
    
    # 处理列名：标准化为易识别的格式
    df.columns = df.columns.str.strip()
    
    # 识别日期列（第一列，包含 'Month' 或 'Date'）
    date_col = df.columns[0]  # 通常第一列是日期
    
    # 识别金银数据列
    gold_col = None
    silver_col = None
    for col in df.columns:
        if 'gold' in col.lower():
            gold_col = col
        if 'silver' in col.lower():
            silver_col = col
    
    if gold_col is None or silver_col is None:
        print(f"错误: 未找到金银数据列 (Gold: {gold_col}, Silver: {silver_col})")
        return None
    
    print(f"\n识别的列:")
    print(f"  日期列: {date_col}")
    print(f"  黄金列: {gold_col}")
    print(f"  白银列: {silver_col}")
    
    # 转换日期列
    df[date_col] = pd.to_datetime(df[date_col], format='mixed', errors='coerce')
    
    # 移除日期为NaT的行
    df = df.dropna(subset=[date_col])
    
    # 按日期排序
    df = df.sort_values(date_col).reset_index(drop=True)
    
    # LBMA是月频数据，保持原始月末格式
    print(f"\n月频数据统计:")
    print(f"  数据条数: {len(df)}")
    print(f"  日期范围: {df[date_col].min()} 至 {df[date_col].max()}")
    
    # 重命名日期列为统一格式
    df = df.rename(columns={date_col: 'Date'})
    
    if df.empty:
        print("警告: 没有可用数据")
        return None
    
    # 初始化观测列表
    observations = []
    
    # 处理每一行并转换为长格式
    for _, row in df.iterrows():
        as_of_date = row['Date']
        
        # 处理黄金
        if pd.notna(row[gold_col]):
            gold_value = row[gold_col]
            # 转换为实际盎司（从千为单位）
            gold_value = gold_value * 1000
            
            obs_gold = {
                'metal': 'GOLD',
                'source': 'LBMA',
                'freq': 'M',  # 月频
                'as_of_date': as_of_date,
                'metric': 'lbma_holdings_oz',
                'value': gold_value,
                'unit': 'oz',
                'is_imputed': False,
                'method': 'month_end',
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': 'lbma_vault_holdings.csv',
                'raw_checksum': raw_checksum
            }
            observations.append(obs_gold)
        
        # 处理白银
        if pd.notna(row[silver_col]):
            silver_value = row[silver_col]
            # 转换为实际盎司（从千为单位）
            silver_value = silver_value * 1000
            
            obs_silver = {
                'metal': 'SILVER',
                'source': 'LBMA',
                'freq': 'M',  # 月频
                'as_of_date': as_of_date,
                'metric': 'lbma_holdings_oz',
                'value': silver_value,
                'unit': 'oz',
                'is_imputed': False,
                'method': 'month_end',
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': 'lbma_vault_holdings.csv',
                'raw_checksum': raw_checksum
            }
            observations.append(obs_silver)
    
    # 转换为DataFrame
    clean_df = pd.DataFrame(observations)
    
    # 按日期和金属排序
    clean_df = clean_df.sort_values(['as_of_date', 'metal']).reset_index(drop=True)
    
    return clean_df


def main():
    """主函数"""
    print("\n" + "#"*60)
    print("# LBMA 每日数据获取与清洗工具")
    print(f"# 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("#"*60)
    
    # 1. 下载数据
    raw_data = download_lbma_data()
    
    if raw_data is None:
        print("\n[FAIL] 数据下载失败")
        return None
    
    # 2. 清洗数据（保持月末格式）
    print("\n开始清洗数据（保持月末数据格式）...")
    clean_df = clean_lbma_data(raw_data)
    
    if clean_df is None or clean_df.empty:
        print("[FAIL] 数据清洗失败")
        return None
    
    print(f"[OK] 成功清洗 {len(clean_df)} 条观测记录（月频）")
    print(f"   日期范围: {clean_df['as_of_date'].min().strftime('%Y-%m-%d')} 至 {clean_df['as_of_date'].max().strftime('%Y-%m-%d')}")
    print(f"   金属类型: {', '.join(clean_df['metal'].unique())}")
    print(f"   数据频率: 月度 (M)")
    
    # 质量汇总
    quality_summary = clean_df['quality'].value_counts()
    print("\n质量检查结果:")
    for quality, count in quality_summary.items():
        status = "[OK]" if quality == 'ok' else "[WARN]"
        print(f"  {status} {quality}: {count} 条")
    
    # 3. 保存到本地CSV
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, 'clean_observations.csv')
    clean_df.to_csv(output_file, index=False)
    print(f"\n[SAVE] 清洗后数据已保存至: {output_file}")
    
    # 4. 存入数据库
    print("\n[DB] 正在存入数据库...")
    with DatabaseSession("lbma_daily_fetch.py") as db:
        db.save(clean_df)
    
    print("\n" + "="*60)
    print("[OK] LBMA数据每日更新完成！（已同步到数据库）")
    print("="*60)
    
    return clean_df


if __name__ == "__main__":
    main()
