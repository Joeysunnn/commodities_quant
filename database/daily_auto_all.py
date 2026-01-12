"""
每日自动化数据更新脚本
- 自动执行所有数据源的抓取、清洗、上传任务
- 支持错误隔离：单个任务失败不影响其他任务
- 记录详细日志
"""

import logging
import sys
import os
from datetime import datetime

# 添加各个模块的路径
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT_DIR)

# 导入各个数据源的主函数
# 注意：LME已移除，因其依赖Selenium不稳定，将单独创建计划任务
sys.path.insert(0, os.path.join(ROOT_DIR, 'comex'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'GLD'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'LBMA'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'shex'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'XAUUSD'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'XAGUSD'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'rawdata', 'price_data', 'HG_F'))
sys.path.insert(0, os.path.join(ROOT_DIR, 'rawdata', 'SLV'))

# 配置日志
logging.basicConfig(
    filename=os.path.join(ROOT_DIR, 'daily_auto_all.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)

def run_task_safely(task_name, func):
    """
    沙箱模式执行任务
    无论 func() 里面怎么报错，都不会让整个脚本崩溃
    确保 Task A 失败了，Task B 还能继续跑
    """
    print(f"\n{'='*70}")
    print(f"开始执行: {task_name}")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print('='*70)
    
    try:
        func()  # 执行任务函数
        success_msg = f"[OK] {task_name} 执行成功"
        print(f"\n{success_msg}")
        logging.info(success_msg)
        return True
    except Exception as e:
        error_msg = f"[FAIL] {task_name} 执行失败: {str(e)}"
        print(f"\n{error_msg}")
        logging.error(error_msg, exc_info=True)
        return False

if __name__ == "__main__":
    print("\n" + "="*70)
    print("[START] 每日自动化数据更新任务开始")
    print(f"[TIME] 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = datetime.now()
    weekday = start_time.weekday()  # 0是周一, 4是周五, 6是周日
    
    # 统计任务执行结果
    results = {
        'total': 0,
        'success': 0,
        'failed': 0
    }
    
    # ======================================================================
    # 每日必做任务 (Daily Tasks)
    # ======================================================================
    
    # 1. COMEX 库存数据（黄金、白银、铜）
    results['total'] += 1
    try:
        from comex.daily_fetch import main as comex_main
        if run_task_safely("COMEX 库存数据", comex_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入 COMEX 模块: {e}")
        results['failed'] += 1
    
    # 2. GLD ETF 黄金持仓数据（原任务3）
    results['total'] += 1
    try:
        from daily_fetch_gld import main as gld_main
        if run_task_safely("GLD ETF 持仓数据", gld_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入 GLD 模块: {e}")
        results['failed'] += 1
    
    # 3. SLV ETF 白银持仓数据
    results['total'] += 1
    try:
        from daily_slv import daily_update as slv_main
        if run_task_safely("SLV ETF 持仓数据", slv_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入 SLV 模块: {e}")
        results['failed'] += 1
    
    # 4. LBMA 金银库存数据（月频数据，使用ffill扩展为日频）
    results['total'] += 1
    try:
        from lbma_daily_fetch import main as lbma_main
        if run_task_safely("LBMA 金银库存数据", lbma_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入 LBMA 模块: {e}")
        results['failed'] += 1
    
    # 5. 黄金价格数据（yfinance）
    results['total'] += 1
    try:
        from gold_price import main as gold_price_main
        if run_task_safely("黄金价格数据", gold_price_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入黄金价格模块: {e}")
        results['failed'] += 1
    
    # 6. 白银价格数据（yfinance）
    results['total'] += 1
    try:
        from silver_price import main as silver_price_main
        if run_task_safely("白银价格数据", silver_price_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入白银价格模块: {e}")
        results['failed'] += 1
    
    # 7. 铜期货价格数据（yfinance）
    results['total'] += 1
    try:
        from copper_price import main as copper_price_main
        if run_task_safely("铜期货价格数据", copper_price_main):
            results['success'] += 1
        else:
            results['failed'] += 1
    except ImportError as e:
        logging.error(f"无法导入铜价格模块: {e}")
        results['failed'] += 1
    
    # ======================================================================
    # 每周特定任务 (Weekly Tasks)
    # ======================================================================
    
    # 8. SHEX 上期所铜库存（每周五更新，或周六/周日运行）
    if weekday >= 4:  # 周五、周六、周日
        results['total'] += 1
        try:
            from auto_update import main as shex_main
            if run_task_safely("上期所铜库存数据（周报）", shex_main):
                results['success'] += 1
            else:
                results['failed'] += 1
        except ImportError as e:
            logging.error(f"无法导入 SHEX 模块: {e}")
            results['failed'] += 1
    else:
        print(f"\n[SKIP] 上期所铜库存数据跳过（仅周五-周日执行，今天是周{['一','二','三','四','五','六','日'][weekday]}）")
    
    # ======================================================================
    # 任务执行总结
    # ======================================================================
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "="*70)
    print("[SUMMARY] 任务执行总结")
    print("="*70)
    print(f"[OK] 成功: {results['success']}/{results['total']} 个任务")
    print(f"[FAIL] 失败: {results['failed']}/{results['total']} 个任务")
    print(f"[DURATION] 总耗时: {duration}")
    print(f"[END] 完成时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    summary_msg = f"每日自动更新完成 - 成功: {results['success']}, 失败: {results['failed']}, 总耗时: {duration}"
    logging.info(summary_msg)
    
    # 如果所有任务都成功，返回0；否则返回1
    exit_code = 0 if results['failed'] == 0 else 1
    sys.exit(exit_code)