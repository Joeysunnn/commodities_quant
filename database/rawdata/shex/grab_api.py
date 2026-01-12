#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
上海期货交易所库存周报数据 - API直接抓取
支持两种API格式：
1. 旧格式（2025-10-31之前）：.dat文件
2. 新格式（2025-10-31之后）：.html文件
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime, timedelta
import time
import json

class SHFEAPIFetcher:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://www.shfe.com.cn/reports/tradedata/dailyandweeklydata/',
        }
        # API格式切换的分界日期（2025年10月31日）
        self.api_change_date = datetime(2025, 10, 31)
    
    def get_fridays_in_range(self, start_date, end_date):
        """
        获取指定日期范围内的所有周五
        """
        fridays = []
        current = start_date
        
        # 如果起始日期不是周五，找到下一个周五
        if current.weekday() != 4:
            days_until_friday = (4 - current.weekday()) % 7
            if days_until_friday == 0:
                days_until_friday = 7
            current = current + timedelta(days=days_until_friday)
        
        while current <= end_date:
            fridays.append(current)
            current = current + timedelta(days=7)
        
        return fridays
    
    def fetch_old_api(self, date_obj):
        """
        获取旧API格式的数据（.dat格式）
        URL: https://www.shfe.com.cn/data/tradedata/future/weeklydata/YYYYMMDDweeklystock.dat
        """
        date_str = date_obj.strftime('%Y%m%d')
        url = f"https://www.shfe.com.cn/data/tradedata/future/weeklydata/{date_str}weeklystock.dat"
        
        # 添加时间戳参数
        timestamp = int(time.time() * 1000)
        url_with_params = f"{url}?params={timestamp}"
        
        print(f"  请求旧API: {url}")
        
        try:
            response = requests.get(url_with_params, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                # .dat文件通常是JSON格式
                try:
                    data = response.json()
                    return self.parse_dat_format(data, date_str)
                except json.JSONDecodeError:
                    # 如果不是JSON，尝试作为文本解析
                    return self.parse_text_format(response.text, date_str)
            else:
                print(f"  ✗ 请求失败: {response.status_code}")
                return None
        except Exception as e:
            print(f"  ✗ 请求出错: {e}")
            return None
    
    def fetch_new_api(self, date_obj):
        """
        获取新API格式的数据（.html格式）
        URL: https://www.shfe.com.cn/data/tradedata/future/stockdata/weeklystock_YYYYMMDD/ZH/all.html
        """
        date_str = date_obj.strftime('%Y%m%d')
        url = f"https://www.shfe.com.cn/data/tradedata/future/stockdata/weeklystock_{date_str}/ZH/all.html"
        
        # 添加时间戳参数
        timestamp = int(time.time() * 1000)
        url_with_params = f"{url}?params={timestamp}"
        
        print(f"  请求新API: {url}")
        
        try:
            response = requests.get(url_with_params, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                return self.parse_html_format(response.text, date_str)
            else:
                print(f"  ✗ 请求失败: {response.status_code}")
                return None
        except Exception as e:
            print(f"  ✗ 请求出错: {e}")
            return None
    
    def parse_dat_format(self, data, date_str):
        """
        解析.dat格式（JSON）的数据
        """
        print("  解析.dat格式数据...")
        # 根据实际的JSON结构解析
        # 这里需要根据实际返回的数据结构调整
        try:
            # 假设JSON中有表格数据
            if isinstance(data, dict) and 'o_cursor' in data:
                return self.parse_cursor_data(data['o_cursor'], date_str)
            elif isinstance(data, list):
                return self.parse_list_data(data, date_str)
            else:
                print(f"  ✗ 未知的JSON结构: {type(data)}")
                return None
        except Exception as e:
            print(f"  ✗ 解析出错: {e}")
            return None
    
    def parse_text_format(self, text, date_str):
        """
        解析文本格式的数据
        """
        print("  解析文本格式数据...")
        soup = BeautifulSoup(text, 'html.parser')
        return self.extract_summary_rows(soup, date_str)
    
    def parse_html_format(self, html_content, date_str):
        """
        解析.html格式的数据
        """
        print("  解析HTML格式数据...")
        soup = BeautifulSoup(html_content, 'html.parser')
        return self.extract_summary_rows(soup, date_str)
    
    def extract_summary_rows(self, soup, date_str):
        """
        从BeautifulSoup对象中提取最后三行汇总数据
        """
        result_data = {}
        tables = soup.find_all('table')
        
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) > 0:
                    first_text = cells[0].get_text(strip=True)
                    second_text = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                    
                    if '保税商品总计' in first_text or '保税商品总计' in second_text:
                        row_data = self.extract_row_data(cells)
                        if row_data:
                            result_data['保税商品总计'] = row_data
                    
                    elif '完税商品总计' in first_text or '完税商品总计' in second_text:
                        row_data = self.extract_row_data(cells)
                        if row_data:
                            result_data['完税商品总计'] = row_data
                    
                    elif first_text == '总计' or second_text == '总计':
                        row_data = self.extract_row_data(cells)
                        if row_data:
                            result_data['总计'] = row_data
        
        if len(result_data) == 3:
            print(f"  ✓ 成功解析数据")
            return (date_str, result_data)
        else:
            print(f"  ✗ 只找到 {len(result_data)} 行数据")
            return None
    
    def parse_cursor_data(self, cursor_data, date_str):
        """
        解析cursor格式的JSON数据，提取铜的三行总计数据
        通过WHABBRNAME字段区分：保税商品总计、完税商品总计、总计
        """
        try:
            print(f"  cursor数据条数: {len(cursor_data)}")
            
            # 查找铜的三行总计数据（ROWSTATUS=2且VARID=cu）
            copper_totals = {}
            
            for item in cursor_data:
                if item.get('ROWSTATUS') == '2' and item.get('VARID') == 'cu':  # 铜的总计行
                    whabbrname = item.get('WHABBRNAME', '').split('$$')[0]  # 取中文名
                    
                    copper_totals[whabbrname] = {
                        '上周小计': item.get('PRESPOTWGHTS', 0),
                        '上周期货': item.get('PREWRTWGHTS', 0),
                        '本周小计': item.get('SPOTWGHTS', 0),
                        '本周期货': item.get('WRTWGHTS', 0),
                        '小计增减': item.get('SPOTCHANGE', 0),
                        '期货增减': item.get('WRTCHANGE', 0),
                        '可用库容上周': int(item.get('PREWHSTOCKS', 0)) if item.get('PREWHSTOCKS') else 0,
                        '可用库容本周': int(item.get('WHSTOCKS', 0)) if item.get('WHSTOCKS') else 0,
                        '可用库容增减': int(item.get('WHSTOCKCHANGE', 0)) if item.get('WHSTOCKCHANGE') else 0
                    }
            
            print(f"  找到铜的总计记录数: {len(copper_totals)}")
            
            # 检查是否找到了所需的三行数据
            if len(copper_totals) >= 3 and all(key in copper_totals for key in ['保税商品总计', '完税商品总计', '总计']):
                result_data = {
                    '保税商品总计': copper_totals['保税商品总计'],
                    '完税商品总计': copper_totals['完税商品总计'],
                    '总计': copper_totals['总计']
                }
                
                print(f"  ✓ 成功提取铜的三行总计数据")
                return (date_str, result_data)
            else:
                print(f"  ✗ 未找到完整的三行数据")
                return None
                
        except Exception as e:
            print(f"  ✗ 解析cursor数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def parse_list_data(self, list_data, date_str):
        """
        解析列表格式的JSON数据
        """
        # 根据实际数据结构实现
        print("  解析列表数据...")
        return None
    
    def extract_row_data(self, cells):
        """
        从单元格列表中提取数值数据
        """
        numeric_data = []
        for cell in cells:
            text = cell.get_text(strip=True)
            if text and text not in ['保税商品总计', '完税商品总计', '总计']:
                try:
                    text = text.replace(',', '')
                    if text and text != '':
                        num = float(text)
                        numeric_data.append(num)
                except ValueError:
                    continue
        
        if len(numeric_data) >= 9:
            return {
                '上周小计': numeric_data[0],
                '上周期货': numeric_data[1],
                '本周小计': numeric_data[2],
                '本周期货': numeric_data[3],
                '小计增减': numeric_data[4],
                '期货增减': numeric_data[5],
                '可用库容上周': numeric_data[6],
                '可用库容本周': numeric_data[7],
                '可用库容增减': numeric_data[8]
            }
        return None
    
    def fetch_data_for_date(self, date_obj):
        """
        根据日期自动选择API格式并获取数据
        """
        print(f"\n正在抓取 {date_obj.strftime('%Y-%m-%d')} 的数据...")
        
        # 根据日期选择API
        if date_obj <= self.api_change_date:
            result = self.fetch_old_api(date_obj)
        else:
            result = self.fetch_new_api(date_obj)
        
        # 如果失败，尝试另一种API
        if not result:
            print("  尝试使用备用API格式...")
            if date_obj <= self.api_change_date:
                result = self.fetch_new_api(date_obj)
            else:
                result = self.fetch_old_api(date_obj)
        
        return result
    
    def fetch_data_range(self, start_date_str, end_date_str):
        """
        批量获取日期范围内所有周五的数据
        start_date_str: 'YYYY-MM-DD'
        end_date_str: 'YYYY-MM-DD'
        """
        print("=" * 60)
        print("上海期货交易所库存周报 - API批量抓取")
        print("=" * 60)
        
        # 解析日期
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        print(f"\n时间范围: {start_date_str} 至 {end_date_str}")
        
        # 获取所有周五
        fridays = self.get_fridays_in_range(start_date, end_date)
        print(f"找到 {len(fridays)} 个周五")
        
        # 批量抓取
        all_results = []
        success_count = 0
        fail_count = 0
        
        for friday in fridays:
            result = self.fetch_data_for_date(friday)
            if result:
                all_results.append(result)
                success_count += 1
            else:
                fail_count += 1
            
            # 避免请求过快
            time.sleep(0.5)
        
        print("\n" + "=" * 60)
        print(f"抓取完成: 成功 {success_count} 个, 失败 {fail_count} 个")
        print("=" * 60)
        
        return all_results
    
    def save_to_csv(self, all_results, output_file='weekly_stock_data.csv'):
        """
        保存数据到CSV（累积模式）
        """
        if not all_results:
            print("没有数据可保存")
            return
        
        # 检查文件是否被占用
        import os
        if os.path.exists(output_file):
            try:
                # 尝试以追加模式打开文件来检测是否被占用
                with open(output_file, 'a', encoding='utf-8-sig'):
                    pass
            except PermissionError:
                import sys
                print(f"\n✗ 错误：文件 '{output_file}' 正在被其他程序使用")
                print(f"  解决方法：")
                print(f"  1. 关闭 VS Code 或 Excel 中打开的该文件")
                print(f"  2. 或使用不同的文件名重新运行：")
                if len(sys.argv) >= 3:
                    print(f"     python grab_api.py {sys.argv[1]} {sys.argv[2]} 新文件名.csv")
                else:
                    print(f"     修改代码中的 output_file 参数")
                return
        
        # 读取已有数据
        try:
            existing_df = pd.read_csv(output_file, encoding='utf-8-sig')
            existing_dates = set(existing_df['日期'].astype(str).tolist())
            print(f"\n已加载现有数据: {len(existing_df)} 条记录")
        except FileNotFoundError:
            existing_df = pd.DataFrame()
            existing_dates = set()
            print(f"\n创建新文件: {output_file}")
        
        # 构建新数据
        new_rows = []
        skip_count = 0
        
        for date_str, data_dict in all_results:
            if date_str in existing_dates:
                skip_count += 1
                continue
            
            row = {'日期': date_str}
            for row_type in ['保税商品总计', '完税商品总计', '总计']:
                if row_type in data_dict:
                    for col_name, value in data_dict[row_type].items():
                        full_col_name = f"{row_type}_{col_name}"
                        row[full_col_name] = value
            new_rows.append(row)
        
        if not new_rows:
            print(f"所有数据已存在，跳过 {skip_count} 条")
            return
        
        # 合并数据
        new_df = pd.DataFrame(new_rows)
        if len(existing_df) > 0:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        # 确保日期列是字符串类型再排序
        combined_df['日期'] = combined_df['日期'].astype(str)
        combined_df = combined_df.sort_values('日期')
        combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ 数据已保存")
        print(f"  新增记录: {len(new_rows)}")
        print(f"  跳过重复: {skip_count}")
        print(f"  总记录数: {len(combined_df)}")
        
        # 显示最新数据
        if len(combined_df) > 0:
            print("\n最近5条记录:")
            recent = combined_df.tail(5)[['日期', '总计_本周小计', '总计_本周期货']]
            print(recent.to_string(index=False))


def main():
    """
    主程序入口
    """
    import sys
    
    fetcher = SHFEAPIFetcher()
    
    # 检查是否使用更新模式
    if len(sys.argv) >= 2 and sys.argv[1] in ['--update', '-u', 'update']:
        # 更新模式：从CSV中最后一个日期开始抓取到今天
        output_file = sys.argv[2] if len(sys.argv) > 2 else 'weekly_stock_data.csv'
        
        print("=" * 60)
        print("更新模式：抓取最新数据")
        print("=" * 60)
        
        import os
        if not os.path.exists(output_file):
            print(f"\n✗ 文件 {output_file} 不存在，请先使用完整模式抓取数据")
            print(f"  使用方法: python grab_api.py 2025-01-01 2025-12-31")
            return
        
        # 读取现有数据，获取最新日期
        try:
            existing_df = pd.read_csv(output_file, encoding='utf-8-sig')
            if len(existing_df) == 0:
                print("\n✗ CSV文件为空")
                return
            
            # 获取最新日期
            latest_date_str = existing_df['日期'].astype(str).max()
            latest_date = datetime.strptime(latest_date_str, '%Y%m%d')
            
            # 从最新日期的下一天开始抓取到今天
            start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            print(f"\n现有数据最新日期: {latest_date_str}")
            print(f"将抓取 {start_date} 至 {end_date} 的数据")
            
        except Exception as e:
            print(f"\n✗ 读取文件失败: {e}")
            return
    
    elif len(sys.argv) >= 3:
        # 完整模式：使用命令行参数指定时间范围
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        output_file = sys.argv[3] if len(sys.argv) > 3 else 'weekly_stock_data.csv'
    else:
        # 默认：测试10月和11月的数据
        print("默认测试模式：抓取2025年10月-11月的数据")
        print("=" * 60)
        print("使用方法：")
        print("  更新模式: python grab_api.py --update [输出文件名]")
        print("  完整模式: python grab_api.py 开始日期 结束日期 [输出文件名]")
        print("=" * 60)
        start_date = '2025-10-01'
        end_date = '2025-11-30'
        output_file = 'weekly_stock_data.csv'
    
    # 批量抓取
    results = fetcher.fetch_data_range(start_date, end_date)
    
    # 保存数据
    if results:
        fetcher.save_to_csv(results, output_file)
        print("\n✓ 全部完成！")
    else:
        print("\n✗ 未获取到任何数据")


if __name__ == '__main__':
    main()
