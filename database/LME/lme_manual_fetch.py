"""
LME 手动获取工具
- 选择特定日期范围下载报告
- 自动下载缺失的报告
- 提取并清洗铜(Copper)数据
- 转换为标准化长格式并存入数据库
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import os
import sys
import time
import re
import hashlib
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urljoin

# 添加项目根目录到路径
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT_DIR)

from db_utils import DatabaseSession


class LMEManualFetcher:
    """LME数据手动获取类"""
    
    def __init__(self, download_folder="lme_reports", headless=False):
        self.base_url = "https://www.lme.com"
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.download_folder = os.path.join(self.script_dir, download_folder)
        
        # 创建下载文件夹
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
        
        # 设置Chrome选项
        chrome_options = Options()
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--window-size=1920,1080')
        
        if headless:
            chrome_options.add_argument('--headless')
        
        # 配置Chrome自动下载
        prefs = {
            "download.default_directory": self.download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                '''
            })
            print("Chrome浏览器启动成功")
        except Exception as e:
            print(f"启动Chrome失败: {e}")
            raise
        
        # 创建requests session用于下载
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def close_cookie_popup(self):
        """关闭cookie弹窗"""
        try:
            cookie_buttons = [
                (By.ID, "onetrust-accept-btn-handler"),
                (By.CLASS_NAME, "onetrust-close-btn-handler"),
                (By.XPATH, "//button[contains(text(), 'Accept')]"),
            ]
            
            for by, value in cookie_buttons:
                try:
                    button = WebDriverWait(self.driver, 2).until(
                        EC.element_to_be_clickable((by, value))
                    )
                    button.click()
                    print("已关闭cookie弹窗")
                    time.sleep(1)
                    return
                except:
                    continue
        except Exception:
            pass
    
    def get_page(self, url):
        """使用Selenium获取页面内容"""
        try:
            print(f"正在访问: {url}")
            self.driver.get(url)
            time.sleep(3)
            self.close_cookie_popup()
            
            # 逐步滚动页面以触发懒加载内容
            print("正在逐步滚动页面加载所有报告...")
            
            # 获取页面总高度
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            scroll_step = 600  # 每次滚动600像素
            scroll_pause = 1.5  # 每次滚动后暂停1.5秒
            
            # 逐步向下滚动
            while current_position < total_height:
                # 滚动指定距离
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(scroll_pause)
                
                # 重新获取页面高度（因为懒加载可能增加页面高度）
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height > total_height:
                    total_height = new_height
                    print(f"  检测到新内容，页面高度: {total_height}px")
            
            print(f"  滚动完成，共滚动至 {current_position}px")
            
            # 滚动回顶部
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "search-results__page"))
                )
            except TimeoutException:
                pass
            
            return self.driver.page_source
        except Exception as e:
            print(f"获取页面失败: {e}")
            return None
    
    def parse_report_cards(self, soup):
        """解析报告卡片"""
        reports = []
        search_results = soup.find('ul', class_='search-results__page')
        
        if not search_results:
            return reports
        
        report_cards = search_results.find_all('li', class_='report-card')
        
        for card in report_cards:
            try:
                title_link = card.find('a', class_='report-card__title-link')
                if not title_link:
                    continue
                
                title = title_link.get_text(strip=True)
                href = title_link.get('href', '')
                
                if not href:
                    download_btn = card.find('a', class_='button-secondary')
                    if download_btn:
                        href = download_btn.get('href', '')
                
                if not href:
                    continue
                
                file_url = href if href.startswith('http') else urljoin(self.base_url, href)
                
                reports.append({
                    'title': title,
                    'url': file_url
                })
                
            except Exception as e:
                continue
        
        return reports
    
    def parse_date_from_title(self, title):
        """从报告标题中解析日期"""
        match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', title)
        if not match:
            return None
        
        day = int(match.group(1))
        month_str = match.group(2)
        year = int(match.group(3))
        
        month_map = {
            'jan': 1, 'january': 1, 'feb': 2, 'february': 2,
            'mar': 3, 'march': 3, 'apr': 4, 'april': 4,
            'may': 5, 'jun': 6, 'june': 6,
            'jul': 7, 'july': 7, 'aug': 8, 'august': 8,
            'sep': 9, 'september': 9, 'oct': 10, 'october': 10,
            'nov': 11, 'november': 11, 'dec': 12, 'december': 12,
        }
        
        month = month_map.get(month_str.lower())
        if not month:
            return None
        
        try:
            return datetime(year, month, day)
        except ValueError:
            return None
    
    def get_local_downloaded_files(self):
        """获取本地已下载的报告文件名集合"""
        downloaded = set()
        if not os.path.exists(self.download_folder):
            return downloaded
        
        for filename in os.listdir(self.download_folder):
            if filename.endswith('.xls'):
                downloaded.add(filename)
        
        return downloaded
    
    def get_months_in_range(self, start_date, end_date):
        """获取日期范围内的所有月份"""
        months = []
        current = start_date.replace(day=1)
        end_month = end_date.replace(day=1)
        
        while current <= end_month:
            months.append((str(current.year), current.strftime('%B')))
            current += relativedelta(months=1)
        
        return months
    
    def click_next_page(self):
        """点击下一页按钮进行翻页"""
        try:
            # 尝试多种方式找到并点击下一页按钮
            next_selectors = [
                "a.pagination__next",
                "a[rel='next']",
                "li.pagination__item--next a",
                "a[aria-label='Next']",
                "button.pagination__next",
            ]
            
            for selector in next_selectors:
                try:
                    next_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if next_btn and next_btn.is_displayed() and next_btn.is_enabled():
                        # 滚动到按钮位置
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                        time.sleep(0.5)
                        next_btn.click()
                        time.sleep(2)  # 等待页面加载
                        return True
                except:
                    continue
            
            # 尝试通过XPath查找
            xpath_selectors = [
                "//a[contains(@class, 'pagination') and contains(@class, 'next')]",
                "//a[contains(text(), 'Next')]",
                "//a[contains(text(), '›')]",
                "//button[contains(text(), 'Next')]",
            ]
            
            for xpath in xpath_selectors:
                try:
                    next_btn = self.driver.find_element(By.XPATH, xpath)
                    if next_btn and next_btn.is_displayed() and next_btn.is_enabled():
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
                        time.sleep(0.5)
                        next_btn.click()
                        time.sleep(2)
                        return True
                except:
                    continue
            
            return False
        except Exception as e:
            print(f"    翻页失败: {e}")
            return False
    
    def fetch_reports_in_range(self, start_date, end_date):
        """获取指定日期范围内的报告（支持分页）"""
        
        # 获取需要检查的所有月份
        months_to_check = self.get_months_in_range(start_date, end_date)
        
        print(f"\n{'='*60}")
        print(f"获取指定日期范围的报告")
        print(f"日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        print(f"需要检查的月份: {months_to_check}")
        print(f"{'='*60}")
        
        all_reports = []
        seen_titles = set()  # 用于去重
        
        for year, month in months_to_check:
            base_url = f"{self.base_url}/Market-data/Reports-and-data/Warehouse-and-stocks-reports/Stock-breakdown-report"
            
            print(f"\n正在获取 {month} {year} 的报告...")
            
            # 首先访问第一页
            url = f"{base_url}?page=1&facetFilterPairs=report_friendly_date%2C{month}+{year}"
            
            html = self.get_page(url)
            if not html:
                continue
            
            page_num = 1
            month_total_reports = 0
            max_pages = 5  # 安全限制，防止无限循环
            
            while page_num <= max_pages:
                print(f"  📄 第 {page_num} 页...")
                
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
                page_reports = self.parse_report_cards(soup)
                
                if not page_reports:
                    if page_num == 1:
                        print(f"  未找到报告")
                    break
                
                # 检查是否有新报告（去重检测）
                new_reports_count = 0
                for report in page_reports:
                    if report['title'] not in seen_titles:
                        new_reports_count += 1
                
                if new_reports_count == 0 and page_num > 1:
                    print(f"    检测到重复内容，翻页可能失败，停止当前月份")
                    break
                
                print(f"    找到 {len(page_reports)} 个报告 (新增 {new_reports_count} 个)")
                month_total_reports += new_reports_count
                
                # 筛选日期范围内的报告
                for report in page_reports:
                    if report['title'] in seen_titles:
                        continue
                    seen_titles.add(report['title'])
                    
                    report_date = self.parse_date_from_title(report['title'])
                    if report_date:
                        report['date'] = report_date
                        if start_date <= report_date <= end_date:
                            all_reports.append(report)
                            print(f"      ✓ {report['title']}")
                        else:
                            print(f"      - 跳过(超出范围): {report['title']}")
                
                # 如果当前页报告数少于10个，说明是最后一页
                if len(page_reports) < 10:
                    print(f"    当前页报告数 < 10，已到最后一页")
                    break
                
                # 尝试点击下一页
                print(f"    尝试翻到第 {page_num + 1} 页...")
                if self.click_next_page():
                    page_num += 1
                    time.sleep(1)
                else:
                    print(f"    未找到下一页按钮或已是最后一页")
                    break
            
            print(f"  ✅ {month} {year} 共 {page_num} 页, {month_total_reports} 个报告")
        
        all_reports.sort(key=lambda x: x.get('date', datetime.min), reverse=True)
        return all_reports
    
    def sanitize_filename(self, filename):
        """清理文件名"""
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename
    
    def download_file(self, url, filename):
        """下载文件"""
        try:
            print(f"  正在下载: {filename}")
            
            # 获取selenium的cookies
            cookies = self.driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
            # 添加完整的请求头，模拟浏览器请求
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': 'https://www.lme.com/Market-data/Reports-and-data/Warehouse-and-stocks-reports/Stock-breakdown-report',
                'Origin': 'https://www.lme.com',
                'Connection': 'keep-alive',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = self.session.get(url, headers=headers, stream=True, timeout=30)
            response.raise_for_status()
            
            filepath = os.path.join(self.download_folder, filename)
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"  ✓ 下载成功: {filename}")
            return True
        except Exception as e:
            print(f"  ✗ 下载失败: {filename}, 错误: {e}")
            # 如果requests失败，尝试使用selenium直接下载
            return self.download_with_selenium(url, filename)
    
    def download_with_selenium(self, url, filename):
        """使用Selenium直接下载文件（备用方法）"""
        try:
            print(f"  尝试使用浏览器直接下载...")
            
            # 记录下载前的文件列表
            before_files = set(os.listdir(self.download_folder))
            
            # 使用selenium访问下载链接
            self.driver.get(url)
            
            # 等待下载完成（最多等待30秒）
            for i in range(30):
                time.sleep(1)
                after_files = set(os.listdir(self.download_folder))
                new_files = after_files - before_files
                
                # 检查是否有新文件（排除.crdownload临时文件）
                completed_files = [f for f in new_files if not f.endswith('.crdownload')]
                
                if completed_files:
                    downloaded_file = completed_files[0]
                    print(f"  ✓ 浏览器下载成功: {downloaded_file}")
                    
                    # 如果需要重命名
                    if downloaded_file != filename:
                        old_path = os.path.join(self.download_folder, downloaded_file)
                        new_path = os.path.join(self.download_folder, filename)
                        try:
                            if os.path.exists(new_path):
                                os.remove(new_path)
                            os.rename(old_path, new_path)
                            print(f"  已重命名为: {filename}")
                        except:
                            pass
                    return True
                
                # 检查是否还在下载中
                downloading = [f for f in new_files if f.endswith('.crdownload')]
                if downloading:
                    continue
            
            print(f"  下载超时")
            return False
                
        except Exception as e:
            print(f"  Selenium下载也失败: {e}")
            return False
    
    def extract_copper_from_report(self, filepath):
        """从单个报告中提取铜数据"""
        try:
            # 读取Excel文件
            df = pd.read_excel(filepath, sheet_name=0, header=None)
            
            # 找到Copper数据区域
            copper_start_row = None
            for idx, row in df.iterrows():
                row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
                if 'copper' in row_str:
                    copper_start_row = idx
                    break
            
            if copper_start_row is None:
                return None
            
            # 找到表头行
            header_row = None
            for idx in range(copper_start_row, min(copper_start_row + 10, len(df))):
                row_str = ' '.join([str(cell).lower() for cell in df.iloc[idx] if pd.notna(cell)])
                if 'country' in row_str or 'region' in row_str or 'opening stock' in row_str:
                    header_row = idx
                    break
            
            if header_row is None:
                header_row = copper_start_row + 2
            
            # 找到Total行（数据结束）
            total_row = None
            for idx in range(header_row + 1, min(header_row + 50, len(df))):
                first_cell = str(df.iloc[idx, 0]).strip().lower()
                if first_cell == 'total':
                    total_row = idx
                    break
            
            if total_row is None:
                return None
            
            # 提取Total行的数据
            total_data = df.iloc[total_row].tolist()
            
            # 解析数值（通常是：[Total, Opening, In, Out, Closing, Open, Cancelled]）
            values = []
            for cell in total_data[1:]:  # 跳过第一列（Total标签）
                try:
                    if pd.notna(cell):
                        values.append(float(cell))
                except:
                    continue
            
            if len(values) >= 6:
                return {
                    'Opening_Stock': values[0],
                    'Delivered_In': values[1],
                    'Delivered_Out': values[2],
                    'Closing_Stock': values[3],
                    'Open_Tonnage': values[4],
                    'Cancelled_Tonnage': values[5]
                }
            
            return None
            
        except Exception as e:
            print(f"  提取数据失败: {e}")
            return None
    
    def parse_date_from_filename(self, filename):
        """从文件名中解析日期"""
        patterns = [
            r'Metals Reports (\d{2}) (\w+) (\d{4})\.xls',
            r'Metals-Reports-(\d{2})-(\w+)-(\d{4})\.xls',
        ]
        
        month_map = {
            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
        }
        
        for pattern in patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                day = match.group(1)
                month_str = match.group(2)
                year = match.group(3)
                month = month_map.get(month_str, month_map.get(month_str.capitalize()))
                if month:
                    return f"{year}{month}{day}"
        
        return None
    
    def process_downloaded_reports(self, start_date=None, end_date=None):
        """处理下载的报告并生成汇总数据（可选：仅处理指定日期范围）"""
        files = [f for f in os.listdir(self.download_folder) if f.endswith('.xls')]
        
        all_data = []
        
        for filename in files:
            date_str = self.parse_date_from_filename(filename)
            if not date_str:
                continue
            
            # 如果指定了日期范围，则过滤
            if start_date and end_date:
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    if not (start_date <= file_date <= end_date):
                        continue
                except:
                    continue
            
            filepath = os.path.join(self.download_folder, filename)
            copper_data = self.extract_copper_from_report(filepath)
            
            if copper_data:
                copper_data['Date'] = date_str
                all_data.append(copper_data)
                print(f"  ✓ 提取成功: {filename}")
            else:
                print(f"  ✗ 提取失败: {filename}")
        
        if not all_data:
            return None
        
        df = pd.DataFrame(all_data)
        df = df.sort_values('Date')
        
        return df
    
    def calculate_checksum(self, filepath):
        """计算文件MD5校验和"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def clean_to_long_format(self, summary_df, load_run_id):
        """将汇总数据清洗为标准化长格式"""
        observations = []
        
        # 保存汇总文件用于后续参考
        summary_file = os.path.join(self.script_dir, 'copper_data', 'copper_daily_summary.csv')
        os.makedirs(os.path.dirname(summary_file), exist_ok=True)
        summary_df.to_csv(summary_file, index=False)
        raw_checksum = self.calculate_checksum(summary_file)
        
        metric_columns = {
            'Opening_Stock': 'lme_opening_mt',
            'Delivered_In': 'lme_delivered_in_mt',
            'Delivered_Out': 'lme_delivered_out_mt',
            'Closing_Stock': 'lme_closing_mt',
            'Open_Tonnage': 'lme_open_interest_mt',
            'Cancelled_Tonnage': 'lme_cancelled_mt'
        }
        
        for _, row in summary_df.iterrows():
            as_of_date = pd.to_datetime(row['Date'], format='%Y%m%d').strftime('%Y-%m-%d')
            
            opening = row.get('Opening_Stock')
            delivered_in = row.get('Delivered_In')
            delivered_out = row.get('Delivered_Out')
            closing = row.get('Closing_Stock')
            
            # 质量检查
            quality = 'ok'
            quality_notes = None
            
            if pd.isna(closing):
                quality = 'bad'
                quality_notes = 'Closing stock is null'
            else:
                if not pd.isna(opening) and not pd.isna(delivered_in) and not pd.isna(delivered_out):
                    balance_check = opening + delivered_in - delivered_out - closing
                    
                    if abs(balance_check) > 1e-6:
                        quality = 'warn'
                        quality_notes = f'Balance check failed: |opening + in - out - closing| = {abs(balance_check):.6f} > 1e-6'
            
            for col, metric_name in metric_columns.items():
                if col not in summary_df.columns:
                    continue
                
                value = row[col]
                
                if pd.isna(value):
                    continue
                
                obs = {
                    'metal': 'COPPER',
                    'source': 'LME',
                    'freq': 'D',
                    'as_of_date': as_of_date,
                    'metric': metric_name,
                    'value': value,
                    'unit': 'mt',
                    'is_imputed': False,
                    'method': 'daily',
                    'quality': quality,
                    'quality_notes': quality_notes,
                    'load_run_id': load_run_id,
                    'raw_file': 'copper_daily_summary.csv',
                    'raw_checksum': raw_checksum
                }
                
                observations.append(obs)
        
        clean_df = pd.DataFrame(observations)
        clean_df = clean_df.sort_values(['as_of_date', 'metric']).reset_index(drop=True)
        
        return clean_df
    
    def close(self):
        """关闭浏览器"""
        if hasattr(self, 'driver'):
            self.driver.quit()
            print("\n浏览器已关闭")
    
    def run(self, start_date, end_date, skip_download=False):
        """运行手动获取流程
        
        参数:
            start_date: 开始日期 (datetime 或 字符串 'YYYY-MM-DD')
            end_date: 结束日期 (datetime 或 字符串 'YYYY-MM-DD')
            skip_download: 是否跳过下载步骤（仅处理已有文件）
        """
        try:
            # 解析日期
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d')
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
            
            print("\n" + "#"*60)
            print("# LME 铜数据手动获取工具")
            print(f"# 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"# 目标日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
            print("#"*60)
            
            if not skip_download:
                # 1. 获取本地已下载的文件
                local_files = self.get_local_downloaded_files()
                print(f"\n本地已有 {len(local_files)} 个报告文件")
                
                # 2. 获取指定日期范围的在线报告
                target_reports = self.fetch_reports_in_range(start_date, end_date)
                
                if not target_reports:
                    print("\n未找到任何报告")
                    return None
                
                print(f"\n目标日期范围内共有 {len(target_reports)} 个报告")
                
                # 3. 筛选需要下载的新报告
                new_reports = []
                for report in target_reports:
                    filename = self.sanitize_filename(report['title']) + '.xls'
                    if filename not in local_files:
                        report['filename'] = filename
                        new_reports.append(report)
                
                if new_reports:
                    print(f"\n发现 {len(new_reports)} 个新报告需要下载:")
                    for i, report in enumerate(new_reports, 1):
                        date_str = report['date'].strftime('%Y-%m-%d')
                        print(f"  {i}. {report['title']} ({date_str})")
                    
                    print("\n开始下载...")
                    success_count = 0
                    for i, report in enumerate(new_reports, 1):
                        print(f"\n[{i}/{len(new_reports)}]")
                        if self.download_file(report['url'], report['filename']):
                            success_count += 1
                        time.sleep(0.5)
                    
                    print(f"\n下载完成! 成功: {success_count}/{len(new_reports)}")
                else:
                    print("\n所有报告都已下载")
            else:
                print("\n跳过下载步骤，仅处理已有文件...")
            
            # 4. 处理报告提取数据（仅处理指定日期范围）
            print("\n" + "="*60)
            print("开始处理报告并提取数据...")
            print("="*60)
            
            summary_df = self.process_downloaded_reports(start_date, end_date)
            
            if summary_df is None or summary_df.empty:
                print("未能提取到有效数据")
                return None
            
            print(f"\n成功提取 {len(summary_df)} 条日期记录")
            print(f"日期范围: {summary_df['Date'].min()} 至 {summary_df['Date'].max()}")
            
            # 5. 清洗并转换为长格式
            load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            clean_df = self.clean_to_long_format(summary_df, load_run_id)
            
            print(f"\n生成 {len(clean_df)} 条清洗后的观测记录")
            
            # 质量汇总
            quality_summary = clean_df['quality'].value_counts()
            print("\n质量检查结果:")
            for quality, count in quality_summary.items():
                status = "✅" if quality == 'ok' else ("⚠️" if quality == 'warn' else "❌")
                print(f"  {status} {quality}: {count} 条")
            
            # 6. 保存清洗后的数据
            output_file = os.path.join(self.script_dir, 'clean_observations.csv')
            clean_df.to_csv(output_file, index=False)
            print(f"\n💾 清洗后数据已保存至: {output_file}")
            
            # 7. 存入数据库
            print("\n📤 正在存入数据库...")
            with DatabaseSession("lme_manual_fetch.py") as db:
                db.save(clean_df)
            
            print("\n" + "="*60)
            print("✅ LME数据手动获取完成！（已同步到数据库）")
            print("="*60)
            
            return clean_df
            
        finally:
            self.close()


def main():
    """主函数 - 交互式选择日期范围"""
    print("\n" + "="*60)
    print("LME 铜数据手动获取工具")
    print("="*60)
    
    # 获取用户输入的日期范围
    print("\n请输入要获取的日期范围:")
    
    while True:
        start_input = input("开始日期 (格式: YYYY-MM-DD, 例如 2025-06-01): ").strip()
        try:
            start_date = datetime.strptime(start_input, '%Y-%m-%d')
            break
        except ValueError:
            print("日期格式错误，请重新输入")
    
    while True:
        end_input = input("结束日期 (格式: YYYY-MM-DD, 例如 2025-11-30): ").strip()
        try:
            end_date = datetime.strptime(end_input, '%Y-%m-%d')
            if end_date < start_date:
                print("结束日期不能早于开始日期，请重新输入")
                continue
            break
        except ValueError:
            print("日期格式错误，请重新输入")
    
    # 是否跳过下载
    skip_input = input("\n是否跳过下载步骤，仅处理已有文件? (y/N): ").strip().lower()
    skip_download = skip_input == 'y'
    
    print(f"\n确认信息:")
    print(f"  日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
    print(f"  跳过下载: {'是' if skip_download else '否'}")
    
    confirm = input("\n是否开始执行? (Y/n): ").strip().lower()
    if confirm == 'n':
        print("已取消")
        return
    
    # 执行获取
    fetcher = LMEManualFetcher(headless=False)
    result = fetcher.run(start_date, end_date, skip_download)
    
    if result is not None:
        print(f"\n处理完成，共 {len(result)} 条观测记录")
    else:
        print("\n处理失败")


def fetch_range(start_date, end_date, headless=False, skip_download=False):
    """便捷函数：直接获取指定日期范围的数据
    
    用法示例:
        from lme_manual_fetch import fetch_range
        fetch_range('2025-06-01', '2025-11-30')
    """
    fetcher = LMEManualFetcher(headless=headless)
    return fetcher.run(start_date, end_date, skip_download)


if __name__ == "__main__":
    # 如果命令行参数提供了日期，直接使用
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        skip_download = '--skip-download' in sys.argv
        
        fetcher = LMEManualFetcher(headless=False)
        result = fetcher.run(start_date, end_date, skip_download)
        
        if result is not None:
            print(f"\n处理完成，共 {len(result)} 条观测记录")
        else:
            print("\n处理失败")
    else:
        # 否则进入交互模式
        main()
