"""
SHFE Data Cleaning Script
Cleans weekly copper inventory data to standardized long format with forward-fill imputation
"""

import pandas as pd
import numpy as np
import hashlib
from datetime import datetime, timedelta
import os


def calculate_checksum(filepath):
    """Calculate MD5 checksum of a file"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def clean_shfe_data(input_file, freq='W', unit='mt', method='w_fri_last'):
    """
    Clean SHFE weekly copper inventory data
    
    Parameters:
    -----------
    input_file : str
        Path to input CSV file
    freq : str
        Frequency (W for weekly)
    unit : str
        Unit of measurement (mt for metric tonnes)
    method : str
        Data collection method (w_fri_last - last Friday of the week)
    
    Returns:
    --------
    pd.DataFrame
        Cleaned data in long format with forward-filled imputation
    """
    
    # Read the raw data
    df = pd.read_csv(input_file)
    
    # Calculate raw file checksum
    raw_checksum = calculate_checksum(input_file)
    raw_file = os.path.basename(input_file)
    
    # Generate load_run_id (timestamp of this cleaning run)
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Convert date column to datetime
    df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d')
    
    # Select only the needed columns
    df_clean = df[['日期', '总计_本周小计', '总计_本周期货', '总计_小计增减']].copy()
    
    # Rename columns
    df_clean.columns = ['as_of_date', 'shfe_total_mt', 'shfe_futures_mt', 'reported_change']
    
    # Sort by date
    df_clean = df_clean.sort_values('as_of_date').reset_index(drop=True)
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Get date range for forward-fill imputation
    start_date = df_clean['as_of_date'].min()
    end_date = df_clean['as_of_date'].max()
    
    # Create DAILY date range from start to end
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create a full dataframe with all dates
    full_df = pd.DataFrame({'as_of_date': all_dates})
    
    # Merge with actual data and forward-fill
    full_df = full_df.merge(df_clean, on='as_of_date', how='left')
    full_df = full_df.sort_values('as_of_date')
    
    # Forward-fill the values (each Friday's data extends to next Friday)
    full_df['shfe_total_mt'] = full_df['shfe_total_mt'].ffill()
    full_df['shfe_futures_mt'] = full_df['shfe_futures_mt'].ffill()
    # Don't forward-fill reported_change as it's specific to actual data points
    
    # Process each row and convert to long format
    for idx, row in full_df.iterrows():
        as_of_date = row['as_of_date']
        
        # Check if this is an imputed row (not in original data)
        is_imputed = as_of_date not in df_clean['as_of_date'].values
        
        # Calculate quality
        quality = 'ok'
        quality_notes = None
        
        if not is_imputed and not pd.isna(row['reported_change']):
            # For actual data points, check diff_total vs reported_change
            # Find the previous actual data point
            prev_data = df_clean[df_clean['as_of_date'] < as_of_date]
            if not prev_data.empty:
                prev_total = prev_data.iloc[-1]['shfe_total_mt']
                curr_total = row['shfe_total_mt']
                diff_total = curr_total - prev_total
                reported = row['reported_change']
                
                # Check if difference matches reported change
                diff_abs = abs(diff_total - reported)
                threshold = max(abs(curr_total) * 0.001, 100)  # 0.1% or 100, whichever is larger
                
                if diff_abs > threshold:
                    quality = 'warn'
                    quality_notes = f'Mismatch: |diff_total - reported| = {diff_abs:.2f} > {threshold:.2f}'
        
        # Create observations for shfe_total_mt
        if not pd.isna(row['shfe_total_mt']):
            obs_total = {
                'metal': 'COPPER',
                'source': 'SHFE',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': 'shfe_total_mt',
                'value': row['shfe_total_mt'],
                'unit': unit,
                'is_imputed': is_imputed,
                'method': method,
                'quality': quality,
                'quality_notes': quality_notes,
                'load_run_id': load_run_id,
                'raw_file': raw_file,
                'raw_checksum': raw_checksum
            }
            observations.append(obs_total)
        
        # Create observations for shfe_futures_mt
        if not pd.isna(row['shfe_futures_mt']):
            obs_futures = {
                'metal': 'COPPER',
                'source': 'SHFE',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': 'shfe_futures_mt',
                'value': row['shfe_futures_mt'],
                'unit': unit,
                'is_imputed': is_imputed,
                'method': method,
                'quality': quality,
                'quality_notes': quality_notes,
                'load_run_id': load_run_id,
                'raw_file': raw_file,
                'raw_checksum': raw_checksum
            }
            observations.append(obs_futures)
    
    # Convert to DataFrame
    clean_df = pd.DataFrame(observations)
    
    # Sort by date and metric
    clean_df = clean_df.sort_values(['as_of_date', 'metric']).reset_index(drop=True)
    
    return clean_df


def main():
    """Main function to clean SHFE weekly copper inventory data"""
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define input file path
    input_file = os.path.join(script_dir, 'weekly_stock_data.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return None
    
    print(f"Processing SHFE weekly copper inventory data...")
    
    # Clean the data
    clean_data = clean_shfe_data(
        input_file=input_file,
        freq='W',
        unit='mt',
        method='w_fri_last'
    )
    
    print(f"  - Processed {len(clean_data)} observations")
    
    # Save to output file
    output_file = os.path.join(script_dir, 'clean_observations.csv')
    clean_data.to_csv(output_file, index=False)
    
    print(f"\nCleaning complete!")
    print(f"  Total observations: {len(clean_data)}")
    print(f"  Date range: {clean_data['as_of_date'].min()} to {clean_data['as_of_date'].max()}")
    print(f"  Output file: {output_file}")
    
    # Print imputation summary
    imputed_count = clean_data['is_imputed'].sum()
    actual_count = (~clean_data['is_imputed']).sum()
    print(f"\nData breakdown:")
    print(f"  Actual data points: {actual_count}")
    print(f"  Imputed (forward-filled): {imputed_count}")
    
    # Print quality summary
    quality_summary = clean_data['quality'].value_counts()
    print(f"\nQuality summary:")
    for quality, count in quality_summary.items():
        print(f"  {quality}: {count}")
    
    # Show warnings if any
    warnings = clean_data[clean_data['quality'] == 'warn']
    if not warnings.empty:
        unique_dates = warnings['as_of_date'].unique()
        print(f"\n! {len(unique_dates)} dates with warnings")
        unique_notes = warnings['quality_notes'].unique()
        for note in unique_notes[:5]:  # Show first 5 unique warnings
            if note:
                print(f"  - {note}")
    
    return clean_data


if __name__ == "__main__":
    main()
