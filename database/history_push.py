import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import sys

# ================= 1. 配置区域 =================

# --- 本地数据库 (源) ---
LOCAL_CONFIG = {
    'user': 'postgres',
    'password': 'your_local_password_here',
    'host': 'localhost',
    'port': '5432',
    'database': 'commodities_db'
}

# --- Neon 云端数据库 (目标) ---
NEON_CONFIG = {
    'host': 'ep-broad-credit-a1sqbvtj-pooler.ap-southeast-1.aws.neon.tech',
    'database': 'neondb',
    'user': 'neondb_owner',
    'password': 'YourNeonDBPasswordHere',
    'sslmode': 'require'
}

# ================= 2. 引擎创建 =================

def get_local_engine():
    url = f"postgresql://{LOCAL_CONFIG['user']}:{LOCAL_CONFIG['password']}@{LOCAL_CONFIG['host']}:{LOCAL_CONFIG['port']}/{LOCAL_CONFIG['database']}"
    return create_engine(url)

def get_neon_engine():
    pwd = quote_plus(NEON_CONFIG['password'])
    url = f"postgresql://{NEON_CONFIG['user']}:{pwd}@{NEON_CONFIG['host']}/{NEON_CONFIG['database']}?sslmode={NEON_CONFIG['sslmode']}"
    return create_engine(url)

# ================= 3. 执行核爆迁移 =================

def run_nuclear_fix():
    print("="*60)
    print("☢️  开始执行：强制删除旧表并重新上传 (Local -> Neon)")
    print("="*60)

    local_engine = get_local_engine()
    neon_engine = get_neon_engine()

    # --- Step 1: 读取本地数据 ---
    print("\n📥 [1/4] 读取本地全量数据...")
    try:
        df = pd.read_sql("SELECT * FROM clean.observations", local_engine)
        print(f"✅ 成功读取 {len(df)} 条数据")
        print(f"   本地列名 (正确): {df.columns.tolist()}")
    except Exception as e:
        print(f"❌ 读取本地失败: {e}")
        return

    # --- Step 2: 强制删除云端旧表 (关键步骤!) ---
    print("\n💣 [2/4] 正在销毁云端错误的旧表...")
    with neon_engine.begin() as conn:
        # 这里的 CASCADE 会连带删除依赖项，确保删得干干净净
        conn.execute(text("DROP TABLE IF EXISTS clean.observations CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS clean.load_runs CASCADE;")) # 如果有这个表也顺便删了
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS clean;"))
        print("✅ 云端旧表已彻底粉碎。")

    # --- Step 3: 上传数据 (Pandas 会自动新建正确的表) ---
    print(f"\n🚀 [3/4] 正在上传并重建新表 ({len(df)} rows)...")
    try:
        df.to_sql(
            name='observations',
            schema='clean',
            con=neon_engine,
            if_exists='replace',  # 这里用 replace 作为一个双重保险
            index=False,
            method='multi',
            chunksize=1000
        )
        print("✅ 上传成功！")
    except Exception as e:
        print(f"❌ 上传失败: {e}")
        return

    # --- Step 4: 最终验证 ---
    print("\n🔍 [4/4] 验证云端列名...")
    try:
        with neon_engine.connect() as conn:
            # 查询云端的列名
            result = conn.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'observations' AND table_schema = 'clean'
            """))
            cols = [row[0] for row in result.fetchall()]
            
        print(f"云端现有列名: {cols}")
        
        if 'is_imputed' in cols and 'metal' in cols:
            print("\n🎉 修复成功！列名已完全同步。")
        else:
            print("\n⚠️ 警告：列名似乎还是不对，请截图发给我。")
            
    except Exception as e:
        print(f"验证时出错: {e}")

if __name__ == "__main__":
    run_nuclear_fix()