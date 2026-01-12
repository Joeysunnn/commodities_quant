"""
LME Stock Breakdown Report 爬虫
爬取 2025年12月 至 2024年5月 的所有报告
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin
import re
import requests


class LMEReportScraper:
    def __init__(self, base_url="https://www.lme.com"):
        self.base_url = base_url
        self.download_folder = "lme_reports_2025_06"
        
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
        # 如果不想看到浏览器界面，取消下面这行的注释
        # chrome_options.add_argument('--headless')
        
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
            print("请确保已安装Chrome浏览器")
            raise
        
        # 创建requests session用于下载
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
            
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "search-results__page"))
                )
            except TimeoutException:
                print("页面加载超时")
            
            return self.driver.page_source
        except Exception as e:
            print(f"获取页面失败: {url}")
            print(f"错误: {e}")
            return None
    
    def parse_report_cards(self, soup):
        """
        解析 report-card 元素，只获取 search-results__page 中的报告
        这些是2024年5月22日之后的报告，需要排除归档区域的报告
        """
        reports = []
        
        # 只查找 search-results__page 内的 report-card
        # 这样可以排除归档区域（accordion__content）中的报告
        search_results = soup.find('ul', class_='search-results__page')
        
        if not search_results:
            return reports
        
        report_cards = search_results.find_all('li', class_='report-card')
        
        for card in report_cards:
            try:
                # 查找标题链接
                title_link = card.find('a', class_='report-card__title-link')
                if not title_link:
                    continue
                
                title = title_link.get_text(strip=True)
                href = title_link.get('href', '')
                
                if not href:
                    # 尝试从下载按钮获取链接
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
                print(f"  解析report-card出错: {e}")
                continue
        
        return reports
    
    def has_next_page(self, soup):
        """检查是否有下一页"""
        # 查找分页导航
        pagination = soup.find('nav', class_='search-results__pagination')
        if not pagination:
            return False
        
        # 查找 Next 链接
        next_link = pagination.find('a', class_='pagination__link--next')
        if not next_link:
            return False
        
        # 检查是否被禁用
        classes = next_link.get('class', [])
        if 'pagination__link--disabled' in classes:
            return False
        
        return True
    
    def click_next_page(self):
        """点击下一页按钮"""
        try:
            # 查找分页导航内的 Next 链接
            next_button = self.driver.find_element(
                By.CSS_SELECTOR, 
                "nav.search-results__pagination a.pagination__link--next"
            )
            
            if next_button and next_button.is_enabled() and next_button.is_displayed():
                # 滚动到按钮位置
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", 
                    next_button
                )
                time.sleep(1)
                
                # 使用JavaScript点击
                self.driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)
                return True
        except NoSuchElementException:
            pass
        except Exception as e:
            print(f"点击下一页失败: {e}")
        
        return False
    
    def parse_month_reports(self, year, month):
        """爬取指定月份的报告"""
        print(f"\n{'='*50}")
        print(f"正在爬取 {month} {year} 的报告...")
        print(f"{'='*50}")
        
        reports = []
        page = 1
        
        # 构建带筛选条件的URL
        base_url = f"{self.base_url}/Market-data/Reports-and-data/Warehouse-and-stocks-reports/Stock-breakdown-report"
        url = f"{base_url}?page={page}&facetFilterPairs=report_friendly_date%2C{month}+{year}"
        
        html = self.get_page(url)
        if not html:
            return reports
        
        while True:
            print(f"正在处理第 {page} 页...")
            
            soup = BeautifulSoup(html, 'html.parser')
            page_reports = self.parse_report_cards(soup)
            
            if not page_reports:
                print(f"  第 {page} 页没有找到报告")
                break
            
            for r in page_reports:
                print(f"  找到: {r['title']}")
            
            reports.extend(page_reports)
            print(f"  本页找到 {len(page_reports)} 个报告")
            
            # 检查是否有下一页
            if not self.has_next_page(soup):
                print("  没有更多页面")
                break
            
            # 点击下一页
            if not self.click_next_page():
                print("  无法点击下一页")
                break
            
            page += 1
            html = self.driver.page_source
        
        print(f"{month} {year}: 共找到 {len(reports)} 个报告")
        return reports
    
    def download_file(self, url, filename):
        """下载文件"""
        try:
            print(f"  正在下载: {filename}")
            
            # 先尝试用selenium的cookies
            cookies = self.driver.get_cookies()
            for cookie in cookies:
                self.session.cookies.set(cookie['name'], cookie['value'])
            
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            filepath = os.path.join(self.download_folder, filename)
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"  下载成功: {filename}")
            return True
        except Exception as e:
            print(f"  下载失败: {filename}, 错误: {e}")
            return False
    
    def sanitize_filename(self, filename):
        """清理文件名"""
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename
    
    def download_all_reports(self, reports):
        """下载所有报告"""
        print(f"\n{'='*60}")
        print(f"开始下载 {len(reports)} 个报告...")
        print(f"{'='*60}\n")
        
        success_count = 0
        for i, report in enumerate(reports, 1):
            title = report['title']
            url = report['url']
            
            # 从标题生成文件名
            filename = self.sanitize_filename(title) + '.xls'
            
            print(f"[{i}/{len(reports)}]")
            if self.download_file(url, filename):
                success_count += 1
            
            time.sleep(0.5)
        
        print(f"\n{'='*60}")
        print(f"下载完成! 成功: {success_count}/{len(reports)}")
        print(f"文件保存在: {os.path.abspath(self.download_folder)}")
        print(f"{'='*60}")
    
    def close(self):
        """关闭浏览器"""
        if hasattr(self, 'driver'):
            self.driver.quit()
            print("\n浏览器已关闭")
    
    def run(self):
        """运行爬虫 - 爬取2025年6月至11月的报告"""
        try:
            all_reports = []
            
            # 2025年6月到11月
            months_to_scrape = [
                ("2025", "November"),
                ("2025", "October"),
                ("2025", "September"),
                ("2025", "August"),
                ("2025", "July"),
                ("2025", "June"),
            ]
            
            print(f"\n{'#'*60}")
            print(f"LME Stock Breakdown Report 爬虫")
            print(f"爬取范围: June 2025 - November 2025")
            print(f"总共 {len(months_to_scrape)} 个月")
            print(f"{'#'*60}")
            
            # 按月份爬取
            for year, month in months_to_scrape:
                month_reports = self.parse_month_reports(year, month)
                all_reports.extend(month_reports)
            
            # 去重
            unique_reports = []
            seen_urls = set()
            for report in all_reports:
                if report['url'] not in seen_urls:
                    unique_reports.append(report)
                    seen_urls.add(report['url'])
            
            print(f"\n{'#'*60}")
            print(f"爬取完成!")
            print(f"总共找到 {len(unique_reports)} 个唯一报告")
            print(f"{'#'*60}\n")
            
            # 询问是否下载
            if unique_reports:
                choice = input("是否开始下载所有报告? (y/n): ")
                if choice.lower() == 'y':
                    self.download_all_reports(unique_reports)
                else:
                    # 保存报告列表到文件
                    with open('report_list.txt', 'w', encoding='utf-8') as f:
                        for r in unique_reports:
                            f.write(f"{r['title']}\t{r['url']}\n")
                    print("报告列表已保存到 report_list.txt")
            else:
                print("未找到任何报告")
        
        finally:
            self.close()


if __name__ == "__main__":
    scraper = LMEReportScraper()
    scraper.run()
