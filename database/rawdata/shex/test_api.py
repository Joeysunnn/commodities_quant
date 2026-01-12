#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试API响应格式
"""

import requests
import json

url1 = "https://www.shfe.com.cn/data/tradedata/future/weeklydata/20241011weeklystock.dat"
url2 = "https://www.shfe.com.cn/data/tradedata/future/stockdata/weeklystock_20251205/ZH/all.html"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://www.shfe.com.cn/reports/tradedata/dailyandweeklydata/',
}

print("测试旧API（.dat格式）:")
print("=" * 60)
try:
    response = requests.get(url1, headers=headers, timeout=10)
    print(f"状态码: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"\n前500字符:")
    print(response.text[:500])
    
    # 尝试解析为JSON
    try:
        data = response.json()
        print(f"\nJSON结构:")
        print(f"类型: {type(data)}")
        if isinstance(data, dict):
            print(f"Keys: {list(data.keys())[:10]}")
        with open('test_dat_response.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("完整数据已保存到 test_dat_response.json")
    except:
        print("\n不是JSON格式")
        with open('test_dat_response.txt', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("完整数据已保存到 test_dat_response.txt")
except Exception as e:
    print(f"错误: {e}")

print("\n\n测试新API（.html格式）:")
print("=" * 60)
try:
    response = requests.get(url2, headers=headers, timeout=10)
    print(f"状态码: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"\n前500字符:")
    print(response.text[:500])
    
    with open('test_html_response.html', 'w', encoding='utf-8') as f:
        f.write(response.text)
    print("完整数据已保存到 test_html_response.html")
except Exception as e:
    print(f"错误: {e}")
