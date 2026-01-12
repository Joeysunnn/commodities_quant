"""
LBMA Data Cleaning Script
Cleans gold and silver vault holdings data to standardized long format
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


def clean_lbma_data(input_file, freq='M', unit='oz', method='month_end'):
    """
    Clean LBMA vault holdings data
    
    Parameters:
    -----------
    input_file : str
        Path to input CSV file
    freq : str
        Frequency (M for monthly)
    unit : str
        Unit of measurement (oz for troy ounces in thousands)
    method : str
        Data collection method (month_end)
    
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
    
    # Convert Month_End column to datetime
    df['Month_End'] = pd.to_datetime(df['Month_End'], format='mixed')
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Process each row and convert to long format
    for _, row in df.iterrows():
        as_of_date = row['Month_End']
        
        # Process Gold
        if 'Gold_Troy_Ounces_000s' in df.columns and not pd.isna(row['Gold_Troy_Ounces_000s']):
            gold_value = row['Gold_Troy_Ounces_000s']
            # Convert from thousands to actual ounces
            gold_value = gold_value * 1000
            
            obs_gold = {
                'metal': 'GOLD',
                'source': 'LBMA',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': 'lbma_holdings',
                'value': gold_value,
                'unit': unit,
                'is_imputed': False,
                'method': method,
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': raw_file,
                'raw_checksum': raw_checksum
            }
            observations.append(obs_gold)
        
        # Process Silver
        if 'Silver_Troy_Ounces_000s' in df.columns and not pd.isna(row['Silver_Troy_Ounces_000s']):
            silver_value = row['Silver_Troy_Ounces_000s']
            # Convert from thousands to actual ounces
            silver_value = silver_value * 1000
            
            obs_silver = {
                'metal': 'SILVER',
                'source': 'LBMA',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': 'lbma_holdings',
                'value': silver_value,
                'unit': unit,
                'is_imputed': False,
                'method': method,
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': raw_file,
                'raw_checksum': raw_checksum
            }
            observations.append(obs_silver)
    
    # Convert to DataFrame
    clean_df = pd.DataFrame(observations)
    
    # Sort by date and metal
    clean_df = clean_df.sort_values(['as_of_date', 'metal']).reset_index(drop=True)
    
    return clean_df


def main():
    """Main function to clean LBMA vault holdings data"""
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define input file path
    input_file = os.path.join(script_dir, 'lbma_vault_holdings.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return None
    
    print(f"Processing LBMA vault holdings data...")
    
    # Clean the data
    clean_data = clean_lbma_data(
        input_file=input_file,
        freq='M',
        unit='oz',
        method='month_end'
    )
    
    print(f"  - Processed {len(clean_data)} observations")
    
    # Save to output file
    output_file = os.path.join(script_dir, 'clean_observations.csv')
    clean_data.to_csv(output_file, index=False)
    
    print(f"\nCleaning complete!")
    print(f"  Total observations: {len(clean_data)}")
    print(f"  Date range: {clean_data['as_of_date'].min()} to {clean_data['as_of_date'].max()}")
    print(f"  Metals: {clean_data['metal'].unique()}")
    print(f"  Output file: {output_file}")
    
    # Print quality summary
    quality_summary = clean_data['quality'].value_counts()
    print(f"\nQuality summary:")
    for quality, count in quality_summary.items():
        print(f"  {quality}: {count}")
    
    return clean_data


if __name__ == "__main__":
    main()
