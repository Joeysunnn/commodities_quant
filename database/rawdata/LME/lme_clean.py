"""
LME Data Cleaning Script
Cleans copper inventory data to standardized long format
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


def clean_lme_copper_data(input_file, freq='D', unit='mt', method='daily'):
    """
    Clean LME copper data
    
    Parameters:
    -----------
    input_file : str
        Path to input CSV file
    freq : str
        Frequency (D for daily)
    unit : str
        Unit of measurement (mt for metric tonnes)
    method : str
        Data collection method
    
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
    
    # Convert Date column to datetime
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    
    # Define metric mappings
    metric_columns = {
        'Opening_Stock': 'opening_stock',
        'Delivered_In': 'delivered_in',
        'Delivered_Out': 'delivered_out',
        'Closing_Stock': 'closing_stock',
        'Open_Tonnage': 'open_tonnage',
        'Cancelled_Tonnage': 'cancelled_tonnage'
    }
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Process each row and convert to long format
    for _, row in df.iterrows():
        as_of_date = row['Date']
        
        # Extract values for quality check
        opening = row.get('Opening_Stock')
        delivered_in = row.get('Delivered_In')
        delivered_out = row.get('Delivered_Out')
        closing = row.get('Closing_Stock')
        
        # Calculate balance check
        balance_check = None
        quality = 'ok'
        quality_notes = None
        
        # Check if closing is null/empty -> quality='bad'
        if pd.isna(closing):
            quality = 'bad'
            quality_notes = 'Closing stock is null'
        else:
            # Perform balance check: opening + in - out - closing
            if not pd.isna(opening) and not pd.isna(delivered_in) and not pd.isna(delivered_out):
                balance_check = opening + delivered_in - delivered_out - closing
                
                # If abs(balance_check) > 1e-6, set quality='warn'
                if abs(balance_check) > 1e-6:
                    quality = 'warn'
                    quality_notes = f'Balance check failed: |opening + in - out - closing| = {abs(balance_check):.6f} > 1e-6'
        
        # Create observations for each metric
        for col, metric_name in metric_columns.items():
            if col not in df.columns:
                continue
                
            value = row[col]
            
            # Skip NaN values
            if pd.isna(value):
                continue
            
            # Create observation
            obs = {
                'metal': 'COPPER',
                'source': 'LME',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': metric_name,
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
    
    # Sort by date and metric
    clean_df = clean_df.sort_values(['as_of_date', 'metric']).reset_index(drop=True)
    
    return clean_df


def main():
    """Main function to clean LME copper data"""
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define input file path
    input_file = os.path.join(script_dir, 'copper_data', 'copper_daily_summary.csv')
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return None
    
    print(f"Processing LME Copper data...")
    
    # Clean the data
    clean_data = clean_lme_copper_data(
        input_file=input_file,
        freq='D',
        unit='mt',
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
        unique_dates = warnings['as_of_date'].unique()
        print(f"\n! {len(unique_dates)} dates with warnings")
        unique_notes = warnings['quality_notes'].unique()
        for note in unique_notes[:5]:  # Show first 5 unique warnings
            if note:
                print(f"  - {note}")
    
    # Show bad quality data if any
    bad = clean_data[clean_data['quality'] == 'bad']
    if not bad.empty:
        unique_dates = bad['as_of_date'].unique()
        print(f"\n!! {len(unique_dates)} dates with bad quality")
        unique_notes = bad['quality_notes'].unique()
        for note in unique_notes[:5]:  # Show first 5 unique notes
            if note:
                print(f"  - {note}")
    
    return clean_data


if __name__ == "__main__":
    main()
