"""
Copper Price Data Fetcher using yfinance
Fetches daily copper futures price data (HG=F) from 2021-01-01 to present
Note: HG=F is COMEX copper futures, quoted in USD per pound
Converts price from USD per pound to USD per metric tonne (1 lb = 0.000453592 mt)
Conversion factor: USD/lb * 2204.62 = USD/mt
Outputs in standardized clean format and uploads to database
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import os
import sys

# 添加父目录到路径以导入db_utils
# 添加父目录到路径以导入db_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
from db_utils import DatabaseSession


def calculate_checksum(data_str):
    """Calculate MD5 checksum of data"""
    hash_md5 = hashlib.md5()
    hash_md5.update(data_str.encode('utf-8'))
    return hash_md5.hexdigest()


def get_latest_date_from_csv(csv_file='clean_observations.csv'):
    """Get the latest date from existing CSV file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, csv_file)
    
    if not os.path.exists(csv_path):
        return None
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return None
        df['as_of_date'] = pd.to_datetime(df['as_of_date'])
        latest_date = df['as_of_date'].max()
        return latest_date
    except Exception as e:
        print(f"Error reading existing CSV: {e}")
        return None


def fetch_copper_price(start_date='2021-01-01', end_date=None, incremental=True):
    """
    Fetch daily copper futures price data using yfinance
    Note: HG=F represents COMEX copper futures quoted in USD per pound
    
    Parameters:
    -----------
    start_date : str or datetime
        Start date in format 'YYYY-MM-DD' (default: '2021-01-01')
    end_date : str or None
        End date in format 'YYYY-MM-DD' (default: None, which means today)
    incremental : bool
        If True, only fetch data after the latest date in existing CSV
    
    Returns:
    --------
    pd.DataFrame
        DataFrame with copper price data
    """
    
    # Handle incremental update
    if incremental:
        latest_date = get_latest_date_from_csv()
        if latest_date is not None:
            # Start from the day after the latest date
            start_date = (latest_date + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"Incremental update: fetching data from {start_date}")
    
    # If end_date is None, use today
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"Fetching copper futures price data from {start_date} to {end_date}...")
    
    # Fetch data using yfinance
    # Copper futures ticker: HG=F (COMEX Copper Futures)
    ticker = yf.Ticker("HG=F")
    
    # Download historical data
    df = ticker.history(start=start_date, end=end_date, interval='1d')
    
    if df.empty:
        print("Error: No data could be retrieved")
        return None
    
    # Reset index to make Date a column
    df = df.reset_index()
    
    # Keep only date and close price
    df = df.rename(columns={'Date': 'date', 'Close': 'close'})
    df = df[['date', 'close']]
    
    # Convert date to datetime and remove timezone
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    
    # Sort by date
    df = df.sort_values('date').reset_index(drop=True)
    
    print(f"Successfully fetched {len(df)} records")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    
    return df


def clean_copper_price_data(df, raw_data_str):
    """
    Convert raw price data to standardized clean format
    
    Parameters:
    -----------
    df : pd.DataFrame
        Raw price data with date and close columns
    raw_data_str : str
        String representation of raw data for checksum
    
    Returns:
    --------
    pd.DataFrame
        Cleaned data in standardized long format
    """
    if df is None or df.empty:
        return None
    
    # Calculate raw data checksum
    raw_checksum = calculate_checksum(raw_data_str)
    
    # Generate load_run_id (timestamp of this cleaning run)
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Conversion factor: 1 metric tonne = 2204.62 pounds
    LB_TO_MT_FACTOR = 2204.62
    
    # Process each row
    for idx, row in df.iterrows():
        as_of_date = row['date']
        value_usd_per_lb = row['close']
        
        # Skip rows with NaN values
        if pd.isna(value_usd_per_lb):
            continue
        
        # Convert from USD per pound to USD per metric tonne
        value_usd_per_mt = value_usd_per_lb * LB_TO_MT_FACTOR
        
        # Create observation in standardized format
        obs = {
            'metal': 'COPPER',
            'source': 'YFINANCE',
            'freq': 'D',
            'as_of_date': as_of_date,
            'metric': 'price_futures_usd',
            'value': value_usd_per_mt,
            'unit': 'usd_per_mt',
            'is_imputed': False,
            'method': 'daily',
            'quality': 'ok',
            'quality_notes': None,
            'load_run_id': load_run_id,
            'raw_file': 'yfinance_HG=F',
            'raw_checksum': raw_checksum
        }
        
        observations.append(obs)
    
    # Convert to DataFrame
    clean_df = pd.DataFrame(observations)
    
    return clean_df


def save_copper_price_data(df, output_file='clean_observations.csv'):
    """
    Save cleaned copper price data to CSV file
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with cleaned copper price data
    output_file : str
        Output filename (default: 'clean_observations.csv')
    """
    if df is None or df.empty:
        print("No data to save")
        return
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, output_file)
    
    # Save to CSV
    df.to_csv(output_path, index=False)
    
    print(f"\nData saved to: {output_path}")
    print(f"Total records: {len(df)}")
    
    # Print summary statistics
    print(f"\nPrice Summary:")
    print(f"  Latest close: ${df['value'].iloc[-1]:.2f}")
    print(f"  Highest: ${df['value'].max():.2f} on {df.loc[df['value'].idxmax(), 'as_of_date'].date()}")
    print(f"  Lowest: ${df['value'].min():.2f} on {df.loc[df['value'].idxmin(), 'as_of_date'].date()}")
    print(f"  Average close: ${df['value'].mean():.2f}")
    
    # Print quality summary
    quality_summary = df['quality'].value_counts()
    print(f"\nQuality summary:")
    for quality, count in quality_summary.items():
        print(f"  {quality}: {count}")


def main():
    """Main function to fetch and save copper futures price data"""
    
    print("=" * 60)
    print("Copper Price Data Fetcher - Incremental Update")
    print("=" * 60)
    
    # Fetch copper futures price data (incremental update)
    df_raw = fetch_copper_price(start_date='2021-01-01', incremental=True)
    
    if df_raw is not None and not df_raw.empty:
        # Create raw data string for checksum
        raw_data_str = df_raw.to_string()
        
        # Clean the data to standardized format
        df_clean = clean_copper_price_data(df_raw, raw_data_str)
        
        if df_clean is not None and not df_clean.empty:
            # Display first and last few records
            print(f"\nNew data preview:")
            print(df_clean[['as_of_date', 'metric', 'value', 'unit', 'quality']].head())
            
            # Save to CSV (append mode)
            script_dir = os.path.dirname(os.path.abspath(__file__))
            csv_path = os.path.join(script_dir, 'clean_observations.csv')
            
            if os.path.exists(csv_path):
                # Append to existing file
                df_clean.to_csv(csv_path, mode='a', header=False, index=False)
                print(f"\nAppended {len(df_clean)} new records to {csv_path}")
            else:
                # Create new file
                df_clean.to_csv(csv_path, index=False)
                print(f"\nCreated new file with {len(df_clean)} records: {csv_path}")
            
            # Upload to database
            print("\nUploading to database...")
            try:
                with DatabaseSession("copper_price.py") as db:
                    db.save(df_clean)
                print("[OK] Successfully uploaded to database")
            except Exception as e:
                print(f"[FAIL] Database upload failed: {e}")
                print("Data has been saved to CSV file")
            
            # Print summary
            print(f"\n" + "=" * 60)
            print(f"Update Summary:")
            print(f"  New records added: {len(df_clean)}")
            print(f"  Latest price: ${df_clean['value'].iloc[-1]:.2f} per metric tonne")
            print(f"  Latest date: {df_clean['as_of_date'].iloc[-1].date()}")
            print("=" * 60)
        else:
            print("Failed to clean data")
    else:
        print("\nNo new data to fetch. Database is up to date.")


if __name__ == "__main__":
    main()
