"""
GLD ETF Data Cleaning Script
Cleans gold holdings data to standardized long format
"""

import pandas as pd
import numpy as np
import hashlib
from datetime import datetime
import os


def calculate_checksum(filepath):
    """Calculate MD5 checksum of a file"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def clean_gld_data(input_file, start_date='2021-01-01', freq='D', unit='oz', method='daily'):
    """
    Clean GLD ETF gold holdings data
    
    Parameters:
    -----------
    input_file : str
        Path to input CSV file
    start_date : str
        Start date for filtering (default: '2021-01-01')
    freq : str
        Frequency (D for daily)
    unit : str
        Unit of measurement (oz for troy ounces)
    method : str
        Data collection method (daily)
    
    Returns:
    --------
    pd.DataFrame
        Cleaned data in long format
    """
    
    # Read the raw data
    df = pd.read_csv(input_file)
    
    # Calculate raw file checksum
    raw_checksum = calculate_checksum(input_file)
    raw_file = os.path.basename(input_file)
    
    # Generate load_run_id (timestamp of this cleaning run)
    load_run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Clean column names (remove leading/trailing spaces)
    df.columns = df.columns.str.strip()
    
    # Select and rename columns
    # The column name in the file is: "Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT"
    df_clean = df[['Date', 'Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT']].copy()
    df_clean.columns = ['Date', 'gld_holdings_oz']
    
    # Convert Date column to datetime
    df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%d-%b-%Y', errors='coerce')
    
    # Remove rows where Date is NaT (HOLIDAY rows)
    df_clean = df_clean.dropna(subset=['Date'])
    
    # Filter to dates >= start_date
    start_date_dt = pd.to_datetime(start_date)
    df_clean = df_clean[df_clean['Date'] >= start_date_dt]
    
    # Convert gld_holdings_oz to numeric, handling HOLIDAY values
    df_clean['gld_holdings_oz'] = pd.to_numeric(df_clean['gld_holdings_oz'], errors='coerce')
    
    # Remove rows with NaN values in gld_holdings_oz
    df_clean = df_clean.dropna(subset=['gld_holdings_oz'])
    
    # Sort by date
    df_clean = df_clean.sort_values('Date').reset_index(drop=True)
    
    # Check for duplicate or non-monotonic dates
    df_clean['is_duplicate'] = df_clean['Date'].duplicated(keep='first')
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Track previous date for monotonicity check
    prev_date = None
    
    # Process each row and convert to long format
    for idx, row in df_clean.iterrows():
        as_of_date = row['Date']
        value = row['gld_holdings_oz']
        is_duplicate = row['is_duplicate']
        
        # Determine quality
        quality = 'ok'
        quality_notes = None
        
        # Check for duplicates
        if is_duplicate:
            quality = 'warn'
            quality_notes = f'Duplicate date: {as_of_date.date()}'
            # Skip duplicate rows
            continue
        
        # Check for non-monotonic dates (shouldn't happen after sorting, but check anyway)
        if prev_date is not None and as_of_date <= prev_date:
            quality = 'warn'
            quality_notes = f'Non-monotonic date: {as_of_date.date()} <= {prev_date.date()}'
        
        prev_date = as_of_date
        
        # Create observation
        obs = {
            'metal': 'GOLD',
            'source': 'GLD',
            'freq': freq,
            'as_of_date': as_of_date,
            'metric': 'gld_holdings_oz',
            'value': value,
            'unit': unit,
            'is_imputed': False,
            'method': method,
            'quality': quality,
            'quality_notes': quality_notes,
            'load_run_id': load_run_id,
            'raw_file': raw_file,
            'raw_checksum': raw_checksum
        }
        
        observations.append(obs)
    
    # Convert to DataFrame
    clean_df = pd.DataFrame(observations)
    
    # Sort by date
    clean_df = clean_df.sort_values('as_of_date').reset_index(drop=True)
    
    return clean_df


def main():
    """Main function to clean GLD ETF gold holdings data"""
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define input file path
    input_file = os.path.join(script_dir, 'GLD_US_archive_EN.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return None
    
    print(f"Processing GLD ETF gold holdings data...")
    
    # Clean the data
    clean_data = clean_gld_data(
        input_file=input_file,
        start_date='2021-01-01',
        freq='D',
        unit='oz',
        method='daily'
    )
    
    print(f"  - Processed {len(clean_data)} observations")
    
    # Save to output file
    output_file = os.path.join(script_dir, 'clean_observations.csv')
    clean_data.to_csv(output_file, index=False)
    
    print(f"\nCleaning complete!")
    print(f"  Total observations: {len(clean_data)}")
    print(f"  Date range: {clean_data['as_of_date'].min()} to {clean_data['as_of_date'].max()}")
    print(f"  Output file: {output_file}")
    
    # Print quality summary
    quality_summary = clean_data['quality'].value_counts()
    print(f"\nQuality summary:")
    for quality, count in quality_summary.items():
        print(f"  {quality}: {count}")
    
    # Show warnings if any
    warnings = clean_data[clean_data['quality'] == 'warn']
    if not warnings.empty:
        print(f"\n! {len(warnings)} warnings found")
        unique_notes = warnings['quality_notes'].unique()
        for note in unique_notes[:10]:  # Show first 10 unique warnings
            if note:
                print(f"  - {note}")
    
    return clean_data


if __name__ == "__main__":
    main()
