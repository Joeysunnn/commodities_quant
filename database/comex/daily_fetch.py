"""
COMEX 贵金属库存数据每日抓取与清洗工具
支持: 黄金(Gold)、白银(Silver)、铜(Copper)
数据来源: CME Group

输出格式: 标准化长格式 (Long Format)
"""

import pandas as pd
import numpy as np
import requests
import io
import os
import sys
import re
import hashlib
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# 添加项目根目录到路径，以便导入 db_utils
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

# 导入数据库工具模块
from db_utils import DatabaseSession

# ============== 配置部分 ==============
METALS_CONFIG = {
    'GOLD': {
        'name': '黄金',
        'url': 'https://www.cmegroup.com/delivery_reports/Gold_Stocks.xls',
        'history_file': 'comex_gold_history.csv',
        'unit': 'oz',
        'freq': 'D',
        'method': 'daily',
        'target_labels': [
            ('TOTAL REGISTERED', 'Registered', 'comex_registered_oz'),
            ('TOTAL PLEDGED', 'Pledged', 'comex_pledged_oz'),
            ('TOTAL ELIGIBLE', 'Eligible', 'comex_eligible_oz'),
            ('COMBINED TOTAL', 'Combined_Total', 'comex_total_oz')
        ]
    },
    'SILVER': {
        'name': '白银',
        'url': 'https://www.cmegroup.com/delivery_reports/Silver_stocks.xls',
        'history_file': 'comex_silver_history.csv',
        'unit': 'oz',
        'freq': 'D',
        'method': 'daily',
        'target_labels': [
            ('TOTAL REGISTERED', 'Registered', 'comex_registered_oz'),
            ('TOTAL ELIGIBLE', 'Eligible', 'comex_eligible_oz'),
            ('COMBINED TOTAL', 'Combined_Total', 'comex_total_oz')
        ]
    },
    'COPPER': {
        'name': '铜',
        'url': 'https://www.cmegroup.com/delivery_reports/Copper_Stocks.xls',
        'history_file': 'comex_copper_history.csv',
        'unit': 'mt',  # metric tonnes
        'freq': 'D',
        'method': 'daily',
        'target_labels': [
            ('TOTAL REGISTERED', 'Registered', 'comex_registered_mt'),
            ('TOTAL ELIGIBLE', 'Eligible', 'comex_eligible_mt'),
            ('TOTAL COPPER', 'Total_Copper', 'comex_total_mt')
        ]
    }
}

# 请求头 - 伪装浏览器
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 短吨到公吨的转换系数
SHORT_TON_TO_MT = 0.90718474


def calculate_checksum(data: bytes) -> str:
    """计算数据的MD5校验和"""
    return hashlib.md5(data).hexdigest()


class ComexDataFetcher:
    """COMEX数据抓取和清洗类"""
    
    def __init__(self, metal_key: str):
        if metal_key not in METALS_CONFIG:
            raise ValueError(f"不支持的金属类型: {metal_key}，可选: {list(METALS_CONFIG.keys())}")
        
        self.metal_key = metal_key
        self.config = METALS_CONFIG[metal_key]
        self.name = self.config['name']
        self.url = self.config['url']
        self.history_file = self.config['history_file']
        self.target_labels = self.config['target_labels']
        self.unit = self.config['unit']
        self.freq = self.config['freq']
        self.method = self.config['method']
        
        # 用于存储原始数据的校验和
        self.raw_checksum = None
        self.raw_content = None
    
    def fetch_raw_data(self) -> Optional[pd.DataFrame]:
        """从CME下载原始Excel数据"""
        print(f"[{self.name}] 正在请求数据: {self.url}")
        
        try:
            response = requests.get(self.url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            self.raw_content = response.content
            self.raw_checksum = calculate_checksum(self.raw_content)
        except requests.exceptions.RequestException as e:
            print(f"[{self.name}] 下载失败: {e}")
            return None
        
        try:
            df_raw = pd.read_excel(io.BytesIO(self.raw_content), header=None, engine='xlrd')
            print(f"[{self.name}] 成功读取Excel，共 {len(df_raw)} 行")
            return df_raw
        except Exception as e:
            print(f"[{self.name}] 读取Excel失败: {e}")
            return None
    
    def extract_report_date(self, df_raw: pd.DataFrame) -> str:
        """从原始数据中提取报告日期"""
        for index, row in df_raw.head(10).iterrows():
            row_str = ' '.join([str(cell) for cell in row if pd.notna(cell)])
            date_match = re.search(r"Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})", row_str)
            if date_match:
                report_date_str = date_match.group(1)
                report_date = datetime.strptime(report_date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
                print(f"[{self.name}] 检测到报告日期: {report_date}")
                return report_date
        
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"[{self.name}] 未找到报告日期，使用当天日期: {today}")
        return today
    
    def extract_raw_data(self, df_raw: pd.DataFrame, report_date: str) -> Dict:
        """从原始数据中提取关键指标（原始格式）"""
        extracted_data = {'Date': report_date}
        
        for index, row in df_raw.iterrows():
            row_str = ' '.join([str(cell) for cell in row if pd.notna(cell)]).upper()
            
            for search_label, column_name, metric_name in self.target_labels:
                if search_label in row_str and column_name not in extracted_data:
                    last_valid_number = None
                    for cell in reversed(row.tolist()):
                        if pd.notna(cell):
                            try:
                                if isinstance(cell, (int, float)):
                                    value = float(cell)
                                else:
                                    cell_str = str(cell).strip().replace(',', '')
                                    value = float(cell_str)
                                
                                if value >= 0:
                                    last_valid_number = value
                                    break
                            except (ValueError, TypeError):
                                continue
                    
                    if last_valid_number is not None:
                        extracted_data[column_name] = last_valid_number
                        print(f"[{self.name}] 提取 {search_label}: {last_valid_number:,.3f}")
        
        return extracted_data
    
    def clean_to_long_format(self, raw_data: Dict, load_run_id: str) -> pd.DataFrame:
        """
        将原始数据清洗为标准化长格式
        
        输出列:
        - metal: 金属名称 (GOLD, SILVER, COPPER)
        - source: 数据来源 (COMEX)
        - freq: 频率 (D)
        - as_of_date: 数据日期
        - metric: 指标名称 (registered_inventory, eligible_inventory, etc.)
        - value: 数值
        - unit: 单位 (oz, mt)
        - is_imputed: 是否插值
        - method: 采集方法
        - quality: 质量标记
        - quality_notes: 质量说明
        - load_run_id: 加载批次ID
        - raw_file: 原始文件名
        - raw_checksum: 原始文件校验和
        """
        observations = []
        as_of_date = raw_data['Date']
        
        # 用于质量检查的变量
        registered = None
        eligible = None
        total = None
        
        for search_label, column_name, metric_name in self.target_labels:
            if column_name not in raw_data:
                continue
            
            value = raw_data[column_name]
            
            # 铜需要从短吨转换为公吨
            if self.metal_key == 'COPPER':
                value = value * SHORT_TON_TO_MT
            
            # 记录用于质量检查
            if 'registered' in metric_name:
                registered = value
            elif 'eligible' in metric_name:
                eligible = value
            elif 'total' in metric_name:
                total = value
            
            obs = {
                'metal': self.metal_key,
                'source': 'COMEX',
                'freq': self.freq,
                'as_of_date': as_of_date,
                'metric': metric_name,
                'value': value,
                'unit': self.unit,
                'is_imputed': False,
                'method': self.method,
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': f"{self.metal_key}_Stocks.xls",
                'raw_checksum': self.raw_checksum
            }
            observations.append(obs)
        
        clean_df = pd.DataFrame(observations)
        
        # 质量检查: registered + eligible 是否接近 total
        if registered is not None and eligible is not None and total is not None:
            # 对于有 pledged 的情况 (黄金)，total = registered + eligible（pledged已包含在registered中）
            # 对于没有 pledged 的情况，total = registered + eligible
            calculated_total = registered + eligible
            difference = abs(total - calculated_total)
            threshold = max(total * 0.001, 1000)
            
            if difference > threshold:
                clean_df['quality'] = 'warn'
                clean_df['quality_notes'] = f'Total mismatch: |total - (reg+elig)| = {difference:.2f} > {threshold:.2f}'
        
        return clean_df
    
    def run(self, load_run_id: str) -> Optional[pd.DataFrame]:
        """执行完整的抓取和清洗流程，返回清洗后的长格式数据"""
        print(f"\n{'='*50}")
        print(f"开始处理: {self.name}")
        print('='*50)
        
        # 1. 下载数据
        df_raw = self.fetch_raw_data()
        if df_raw is None:
            return None
        
        # 2. 提取日期
        report_date = self.extract_report_date(df_raw)
        
        # 3. 提取原始数据
        raw_data = self.extract_raw_data(df_raw, report_date)
        
        if len(raw_data) <= 1:
            print(f"[{self.name}] 未提取到有效数据")
            return None
        
        # 4. 清洗为长格式（不再保存历史文件，数据直接存入数据库）
        clean_df = self.clean_to_long_format(raw_data, load_run_id)
        print(f"[{self.name}] 生成 {len(clean_df)} 条清洗后的观测记录")
        
        return clean_df


def fetch_and_clean_all_metals() -> Optional[pd.DataFrame]:
    """抓取所有金属的数据并清洗为统一的长格式"""
    
    # 生成本次运行的批次ID
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    all_clean_data = []
    
    for metal_key in METALS_CONFIG.keys():
        fetcher = ComexDataFetcher(metal_key)
        clean_df = fetcher.run(load_run_id)
        if clean_df is not None:
            all_clean_data.append(clean_df)
    
    if all_clean_data:
        # 合并所有数据
        final_df = pd.concat(all_clean_data, ignore_index=True)
        
        # 按日期和金属排序
        final_df = final_df.sort_values(['as_of_date', 'metal', 'metric']).reset_index(drop=True)
        
        return final_df
    
    return None


def print_clean_summary(df: pd.DataFrame):
    """打印清洗后数据的汇总信息"""
    print("\n" + "="*70)
    print("[SUMMARY] 数据清洗汇总")
    print("="*70)
    
    print(f"\n总观测记录数: {len(df)}")
    print(f"日期范围: {df['as_of_date'].min()} 至 {df['as_of_date'].max()}")
    
    print("\n按金属分类:")
    for metal in df['metal'].unique():
        metal_df = df[df['metal'] == metal]
        name = METALS_CONFIG[metal]['name']
        print(f"  [{name}] {len(metal_df)} 条记录")
    
    print("\n质量检查结果:")
    quality_summary = df['quality'].value_counts()
    for quality, count in quality_summary.items():
        status = "[OK]" if quality == 'ok' else "[WARN]"
        print(f"  {status} {quality}: {count} 条")
    
    # 显示警告详情
    warnings = df[df['quality'] == 'warn']
    if not warnings.empty:
        print("\n警告详情:")
        for note in warnings['quality_notes'].unique():
            if note:
                print(f"  - {note}")


def main():
    """主函数"""
    print("\n" + "#"*70)
    print("#  COMEX 贵金属库存数据 - 每日抓取与清洗工具")
    print("#  支持: 黄金、白银、铜")
    print("#  输出: 标准化长格式 (Long Format)")
    print("#"*70)
    
    # 抓取并清洗所有金属数据
    clean_df = fetch_and_clean_all_metals()
    
    if clean_df is not None:
        # 打印汇总
        print_clean_summary(clean_df)
        
        # 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 保存清洗后的数据
        output_file = os.path.join(script_dir, 'clean_observations.csv')
        clean_df.to_csv(output_file, index=False)
        print(f"\n[SAVE] 清洗后数据已保存至: {output_file}")
        
        # 存入数据库
        print("\n[DB] 正在存入数据库...")
        with DatabaseSession("comex_daily_fetch.py") as db:
            db.save(clean_df)
        
        print("\n" + "="*70)
        print("[OK] 每日抓取与清洗完成！（已同步到数据库）")
        print("="*70)
        
        return clean_df
    else:
        print("\n[FAIL] 未能获取任何数据，请检查网络连接或数据源")
        return None


if __name__ == "__main__":
    main()
