import pandas as pd
import os
import sys

# 修正导入路径，确保可以找到db_utils.py
sys.path.append(os.path.dirname(__file__))
from db_utils import save_to_database


# 1. 读取 copper.csv
copper_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Copper.csv')
copper_df = pd.read_csv(copper_path, header=1)

# 2. 清理copper数据
copper_records = []
for idx, row in copper_df.iterrows():
    date_futs = row['Unnamed: 1']
    futs_price = row['Generic 1st Futures Price']
    date_inv = row['Unnamed: 4']
    inventory = row['Inventory']
    # 期货价格
    if pd.notna(date_futs) and pd.notna(futs_price):
        copper_records.append({
            'metal': 'COPPER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_futs).strftime('%Y-%m-%d'),
            'metric': 'comex_futs_price_usd',
            'value': futs_price,
            'unit': 'usd',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Copper.csv',
            'raw_checksum': ''
        })
    # 库存
    if pd.notna(date_inv) and pd.notna(inventory):
        copper_records.append({
            'metal': 'COPPER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_inv).strftime('%Y-%m-%d'),
            'metric': 'comex_total_mt',
            'value': inventory,
            'unit': 'mt',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Copper.csv',
            'raw_checksum': ''
        })

# 3. 读取 silver.csv
silver_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Silver.csv')
silver_df = pd.read_csv(silver_path, header=1)

# 4. 清理silver数据
silver_records = []
for idx, row in silver_df.iterrows():
    # 期货价格
    date_futs = row['Unnamed: 1']
    futs_price = row['Generic 1st Futures Price']
    if pd.notna(date_futs) and pd.notna(futs_price):
        silver_records.append({
            'metal': 'SILVER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_futs).strftime('%Y-%m-%d'),
            'metric': 'comex_futs_price_usd',
            'value': futs_price,
            'unit': 'usd',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Silver.csv',
            'raw_checksum': ''
        })
    # Inventory
    date_inv = row['Unnamed: 4']
    inventory = row['Inventory']
    if pd.notna(date_inv) and pd.notna(inventory):
        silver_records.append({
            'metal': 'SILVER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_inv).strftime('%Y-%m-%d'),
            'metric': 'comex_total_oz',
            'value': inventory,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Silver.csv',
            'raw_checksum': ''
        })
    # Registered
    date_reg = row['Unnamed: 7']
    registered = row['Registered']
    if pd.notna(date_reg) and pd.notna(registered):
        silver_records.append({
            'metal': 'SILVER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_reg).strftime('%Y-%m-%d'),
            'metric': 'comex_registered_oz',
            'value': registered,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Silver.csv',
            'raw_checksum': ''
        })
    # Eligible
    date_eli = row['Unnamed: 10']
    eligible = row['Eligible']
    if pd.notna(date_eli) and pd.notna(eligible):
        silver_records.append({
            'metal': 'SILVER',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_eli).strftime('%Y-%m-%d'),
            'metric': 'comex_eligible_oz',
            'value': eligible,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Silver.csv',
            'raw_checksum': ''
        })



# 只处理gold.csv
gold_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Gold.csv')
gold_df = pd.read_csv(gold_path, header=1)

gold_records = []
for idx, row in gold_df.iterrows():
    # 期货价格
    date_futs = row['Unnamed: 1']
    futs_price = row['Generic 1st Futures Price']
    if pd.notna(date_futs) and pd.notna(futs_price):
        gold_records.append({
            'metal': 'GOLD',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_futs).strftime('%Y-%m-%d'),
            'metric': 'comex_futs_price_usd',
            'value': futs_price,
            'unit': 'usd',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Gold.csv',
            'raw_checksum': ''
        })
    # Inventory
    date_inv = row['Unnamed: 4']
    inventory = row['Inventory']
    if pd.notna(date_inv) and pd.notna(inventory):
        gold_records.append({
            'metal': 'GOLD',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_inv).strftime('%Y-%m-%d'),
            'metric': 'comex_total_oz',
            'value': inventory,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Gold.csv',
            'raw_checksum': ''
        })
    # Registered
    date_reg = row['Unnamed: 7']
    registered = row['Registered']
    if pd.notna(date_reg) and pd.notna(registered):
        gold_records.append({
            'metal': 'GOLD',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_reg).strftime('%Y-%m-%d'),
            'metric': 'comex_registered_oz',
            'value': registered,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Gold.csv',
            'raw_checksum': ''
        })
    # Eligible
    date_eli = row['Unnamed: 10']
    eligible = row['Eligible']
    if pd.notna(date_eli) and pd.notna(eligible):
        gold_records.append({
            'metal': 'GOLD',
            'source': 'COMEX',
            'freq': 'D',
            'as_of_date': pd.to_datetime(date_eli).strftime('%Y-%m-%d'),
            'metric': 'comex_eligible_oz',
            'value': eligible,
            'unit': 'oz',
            'is_imputed': False,
            'method': 'manual',
            'quality': 'ok',
            'quality_notes': '',
            'load_run_id': None,
            'raw_file': 'Gold.csv',
            'raw_checksum': ''
        })

clean_df = pd.DataFrame(gold_records)
save_to_database(clean_df, script_name='make_up_his.py')
print('黄金数据已清理并上传完成！')
