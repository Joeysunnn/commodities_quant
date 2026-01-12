"""
SLV ETF Daily Data Fetcher
每日从 iShares 网站抓取 SLV 白银 ETF 持仓数据，清洗后存入数据库
"""

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import hashlib
import sys
import os

# 添加父目录到路径，以便导入 db_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from db_utils import save_to_database


def calculate_checksum(content: str) -> str:
    """计算内容的 MD5 校验和"""
    return hashlib.md5(content.encode('utf-8')).hexdigest()


def get_slv_key_facts():
    """
    从 iShares 网站抓取 SLV ETF 的 Ounces in Trust 数据
    
    Returns:
    --------
    tuple: (ounces, data_date_str, raw_content) 或 (None, None, None)
    """
    # SLV 产品主页 URL
    url = "https://www.ishares.com/us/products/239855/ishares-silver-trust-fund/"
    
    # === 关键点 1：伪装成浏览器 ===
    # iShares 反爬比较严，必须带上完整的 User-Agent
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        # 发送请求
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        raw_content = response.text
        
        # 解析 HTML
        soup = BeautifulSoup(raw_content, 'html.parser')
        
        # === 关键点 2：基于文本内容的模糊定位 ===
        label_tag = soup.find(string=re.compile("Ounces in Trust"))
        
        if not label_tag:
            print("[FAIL] 未找到 'Ounces in Trust' 标签，可能是网页改版或反爬拦截")
            return None, None, None

        # === 关键点 3：寻找兄弟节点中的数值 ===
        current = label_tag.find_parent()
        row_text = None
        ounces = None
        
        for _ in range(5):  # 最多向上查找5层
            if current is None:
                break
            row_text = current.get_text(separator='|', strip=True)
            # 查找形如 "518,210,742.60" 的数字 (千分位+小数)
            match = re.search(r'([\d,]+\.\d+)', row_text)
            if match and 'Ounces in Trust' in row_text:
                raw_value = match.group(1)
                ounces = float(raw_value.replace(',', ''))
                break
            current = current.find_parent()
        
        if ounces:
            # 获取日期
            date_match = re.search(r'as of\s+([A-Za-z]+\s+\d+,?\s*\d*)', row_text)
            data_date_str = date_match.group(1) if date_match else None
            
            print(f"[OK] 抓取成功! SLV 库存: {ounces:,.2f} 盎司 (网页日期: {data_date_str})")
            return ounces, data_date_str, raw_content
        else:
            print(f"[WARN] 找到了标签，但没提取到数字。最终行内容: {row_text}")
            return None, None, None

    except Exception as e:
        print(f"[FAIL] 爬虫出错: {e}")
        return None, None, None


def parse_webpage_date(date_str: str) -> datetime:
    """
    解析网页上的日期字符串
    
    Parameters:
    -----------
    date_str : str
        如 "Jan 06, 2026" 或 "Jan 06 2026"
        
    Returns:
    --------
    datetime
    """
    if not date_str:
        return datetime.now()
    
    # 尝试多种格式
    formats = [
        '%b %d, %Y',   # Jan 06, 2026
        '%b %d %Y',    # Jan 06 2026
        '%B %d, %Y',   # January 06, 2026
        '%B %d %Y',    # January 06 2026
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    # 如果都失败，返回当前日期
    print(f"[WARN] 无法解析日期 '{date_str}'，使用当前日期")
    return datetime.now()


def clean_to_standard_format(ounces: float, data_date_str: str, raw_content: str) -> pd.DataFrame:
    """
    将抓取的数据清洗成标准格式（与 slv_clean.py 一致）
    
    Parameters:
    -----------
    ounces : float
        持仓量（盎司）
    data_date_str : str
        网页上的日期字符串
    raw_content : str
        原始网页内容（用于计算校验和）
        
    Returns:
    --------
    pd.DataFrame
        标准格式的 DataFrame
    """
    # 解析日期
    as_of_date = parse_webpage_date(data_date_str)
    
    # 计算校验和
    raw_checksum = calculate_checksum(raw_content)
    
    # 生成 load_run_id
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 构建标准格式 DataFrame
    df = pd.DataFrame([{
        'metal': 'SILVER',
        'source': 'SLV',
        'freq': 'D',  # 日频
        'as_of_date': as_of_date.date(),
        'metric': 'slv_holdings_oz',
        'value': ounces,
        'unit': 'oz',
        'is_imputed': False,
        'method': 'daily_scrape',
        'quality': 'ok',
        'quality_notes': f'Scraped from iShares website, page date: {data_date_str}',
        'load_run_id': load_run_id,
        'raw_file': 'ishares_slv_webpage',
        'raw_checksum': raw_checksum
    }])
    
    return df


def daily_update(save_to_db: bool = True) -> pd.DataFrame:
    """
    每日更新主函数
    
    Parameters:
    -----------
    save_to_db : bool
        是否保存到数据库，默认 True
        
    Returns:
    --------
    pd.DataFrame
        清洗后的数据
    """
    print("=" * 50)
    print(f"SLV ETF 每日数据更新 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    # 1. 抓取数据
    print("\n[1/3] 正在抓取 iShares 网页...")
    ounces, data_date_str, raw_content = get_slv_key_facts()
    
    if ounces is None:
        print("[FAIL] 抓取失败，终止更新")
        return None
    
    # 2. 清洗数据
    print("\n[2/3] 正在清洗数据...")
    df_clean = clean_to_standard_format(ounces, data_date_str, raw_content)
    print(f"  - 日期: {df_clean['as_of_date'].iloc[0]}")
    print(f"  - 持仓量: {df_clean['value'].iloc[0]:,.2f} 盎司")
    
    # 3. 保存到数据库
    if save_to_db:
        print("\n[3/3] 正在保存到数据库...")
        try:
            save_to_database(df_clean, script_name="daily_slv.py")
            print("[OK] 数据库更新完成！")
        except Exception as e:
            print(f"[FAIL] 数据库保存失败: {e}")
            # 即使数据库失败，也返回清洗后的数据
    else:
        print("\n[3/3] 跳过数据库保存（save_to_db=False）")
    
    print("\n" + "=" * 50)
    print("更新完成！")
    print("=" * 50)
    
    return df_clean


# --- 主程序 ---
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='SLV ETF 每日数据更新')
    parser.add_argument('--no-db', action='store_true', help='不保存到数据库（仅测试）')
    args = parser.parse_args()
    
    df = daily_update(save_to_db=not args.no_db)
    
    if df is not None:
        print("\n清洗后的数据:")
        print(df.to_string(index=False))