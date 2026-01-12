"""
通用数据库工具模块
用于将清洗后的数据直接存入 PostgreSQL 数据库
可在各个清洗程序中直接调用

使用方法:
---------
1. 在清洗程序中导入:
   from db_utils import save_to_database
   
2. 清洗完数据后直接调用:
   save_to_database(df_clean, script_name="comex_clean.py")
   
3. 或者使用上下文管理器（推荐）:
   with DatabaseSession("comex_clean.py") as db:
       db.save(df_clean)
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from contextlib import contextmanager
from datetime import datetime
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

load_dotenv()
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
# ================= 配置区 =================
# 数据库连接配置（可根据需要修改）
DB_CONFIG = {
    'user': user,
    'password': password,
    'host': host,
    'port': port,
    'database': db_name
}

# 目标表配置
TABLE_CONFIG = {
    'name': 'observations',
    'schema': 'clean',
    'conflict_columns': ['metal', 'source', 'freq', 'as_of_date', 'metric']  # 唯一约束字段
}

# ================= 数据库引擎 =================
def get_db_url():
    """获取数据库连接 URL"""
    # 对密码进行 URL 编码，防止特殊字符导致连接失败
    encoded_password = quote_plus(DB_CONFIG['password']) if DB_CONFIG['password'] else ''
    return f"postgresql://{DB_CONFIG['user']}:{encoded_password}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_engine():
    """获取数据库连接引擎（单例模式）"""
    if not hasattr(get_engine, '_engine'):
        get_engine._engine = create_engine(get_db_url())
    return get_engine._engine


# ================= 日志管理函数 =================
def start_load_run(script_name: str) -> int:
    """
    在数据库里注册一次运行，并返回 run_id
    
    Parameters:
    -----------
    script_name : str
        运行的脚本名称
        
    Returns:
    --------
    int
        本次运行的 ID
    """
    engine = get_engine()
    with engine.begin() as conn:
        sql = text("""
            INSERT INTO clean.load_runs (status, script_version, notes) 
            VALUES ('running', 'v1.0', :name) 
            RETURNING load_run_id
        """)
        result = conn.execute(sql, {'name': f"Running script: {script_name}"})
        run_id = result.scalar()
        print(f">>> 日志已创建，本次运行 ID: {run_id}")
        return run_id


def finish_load_run(run_id: int, status: str = 'success', error_msg: str = None):
    """
    任务结束，更新状态
    
    Parameters:
    -----------
    run_id : int
        运行 ID
    status : str
        状态 ('success' 或 'failed')
    error_msg : str, optional
        错误信息
    """
    engine = get_engine()
    with engine.begin() as conn:
        notes_update = error_msg if error_msg else "Completed successfully"
        
        sql = text("""
            UPDATE clean.load_runs 
            SET status = :st, finished_at = now(), notes = :nt
            WHERE load_run_id = :rid
        """)
        conn.execute(sql, {'st': status, 'nt': notes_update, 'rid': run_id})
        print(f">>> 日志已更新，Run ID {run_id} 状态: {status}")


# ================= 数据插入函数 =================
def insert_on_conflict_nothing(table, conn, keys, data_iter):
    """
    自定义的 SQL 插入方法。
    当遇到主键/唯一约束冲突时，选择"什么都不做"(DO NOTHING)。
    """
    data = [dict(zip(keys, row)) for row in data_iter]
    
    stmt = insert(table.table).values(data)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=TABLE_CONFIG['conflict_columns']
    )
    
    result = conn.execute(stmt)
    return result.rowcount


def save_to_database(df: pd.DataFrame, script_name: str = None, 
                     table_name: str = None, schema: str = None,
                     log_run: bool = True) -> bool:
    """
    将 DataFrame 存入数据库（核心函数）
    
    Parameters:
    -----------
    df : pd.DataFrame
        清洗后的数据 DataFrame
    script_name : str, optional
        脚本名称（用于日志记录），默认使用时间戳
    table_name : str, optional
        目标表名，默认为 'observations'
    schema : str, optional
        数据库 schema，默认为 'clean'
    log_run : bool
        是否记录运行日志，默认 True
        
    Returns:
    --------
    bool
        是否成功
        
    使用示例:
    ---------
    >>> from db_utils import save_to_database
    >>> save_to_database(df_clean, script_name="lbma_clean.py")
    """
    if df is None or df.empty:
        print("警告: 数据为空，跳过存储")
        return False
    
    # 设置默认值
    script_name = script_name or f"manual_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    table_name = table_name or TABLE_CONFIG['name']
    schema = schema or TABLE_CONFIG['schema']
    
    engine = get_engine()
    run_id = None
    
    try:
        # 开始日志记录
        if log_run:
            run_id = start_load_run(script_name)
        
        # 写入数据库
        rows_before = len(df)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            method=insert_on_conflict_nothing
        )
        
        print(f"[OK] 写入完成！共 {rows_before} 行数据（重复数据已自动忽略）")
        
        # 完成日志记录
        if log_run and run_id:
            finish_load_run(run_id, 'success')
        
        return True
        
    except Exception as e:
        print(f"[FAIL] 写入失败: {e}")
        if log_run and run_id:
            finish_load_run(run_id, 'failed', str(e))
        raise


# ================= 上下文管理器（推荐使用）=================
class DatabaseSession:
    """
    数据库会话上下文管理器
    
    使用示例:
    ---------
    >>> from db_utils import DatabaseSession
    >>> 
    >>> # 在清洗程序末尾
    >>> with DatabaseSession("comex_clean.py") as db:
    >>>     db.save(df_clean)
    >>>     db.save(df_another)  # 可以保存多个 DataFrame
    """
    
    def __init__(self, script_name: str = None, log_run: bool = True):
        self.script_name = script_name or f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.log_run = log_run
        self.run_id = None
        self.saved_count = 0
        self.total_rows = 0
        
    def __enter__(self):
        if self.log_run:
            self.run_id = start_load_run(self.script_name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.log_run and self.run_id:
            if exc_type is None:
                finish_load_run(self.run_id, 'success', 
                              f"Saved {self.saved_count} datasets, {self.total_rows} total rows")
            else:
                finish_load_run(self.run_id, 'failed', str(exc_val))
        return False
    
    def save(self, df: pd.DataFrame, table_name: str = None, schema: str = None) -> bool:
        """
        保存 DataFrame 到数据库
        
        Parameters:
        -----------
        df : pd.DataFrame
            要保存的数据
        table_name : str, optional
            表名，默认 'observations'
        schema : str, optional
            schema 名，默认 'clean'
        """
        if df is None or df.empty:
            print("警告: 数据为空，跳过存储")
            return False
        
        table_name = table_name or TABLE_CONFIG['name']
        schema = schema or TABLE_CONFIG['schema']
        engine = get_engine()
        
        rows = len(df)
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='append',
            index=False,
            method=insert_on_conflict_nothing
        )
        
        self.saved_count += 1
        self.total_rows += rows
        print(f"[OK] 已保存 {rows} 行数据到 {schema}.{table_name}")
        
        return True


# ================= 便捷函数 =================
def save_from_csv(csv_path: str, script_name: str = None) -> bool:
    """
    从 CSV 文件读取并存入数据库（兼容旧方式）
    
    Parameters:
    -----------
    csv_path : str
        CSV 文件路径
    script_name : str, optional
        脚本名称
    """
    import os
    if not os.path.exists(csv_path):
        print(f"错误: 文件不存在 - {csv_path}")
        return False
    
    df = pd.read_csv(csv_path)
    script_name = script_name or os.path.basename(csv_path)
    return save_to_database(df, script_name)


def quick_save(df: pd.DataFrame) -> bool:
    """
    快速保存（无日志记录，适合测试）
    """
    return save_to_database(df, log_run=False)


# ================= 测试连接 =================
def test_connection() -> bool:
    """测试数据库连接"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("[OK] 数据库连接成功！")
            return True
    except Exception as e:
        print(f"[FAIL] 数据库连接失败: {e}")
        return False


# ================= 主程序（用于测试）=================
if __name__ == "__main__":
    print("=" * 50)
    print("数据库工具模块测试")
    print("=" * 50)
    
    # 测试连接
    test_connection()
    
    print("\n使用方法:")
    print("-" * 50)
    print("""
方法1: 直接调用函数
    from db_utils import save_to_database
    save_to_database(df_clean, "my_script.py")

方法2: 使用上下文管理器（推荐）
    from db_utils import DatabaseSession
    with DatabaseSession("my_script.py") as db:
        db.save(df_clean)

方法3: 快速保存（无日志）
    from db_utils import quick_save
    quick_save(df_clean)

方法4: 从 CSV 文件保存（兼容旧方式）
    from db_utils import save_from_csv
    save_from_csv("path/to/clean_observations.csv")
    """)
