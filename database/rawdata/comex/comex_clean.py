"""
COMEX Data Cleaning Script
Cleans gold, silver, and copper inventory data to standardized long format
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


def clean_comex_data(metal, input_file, freq='D', unit='oz', method='daily'):
    """
    Clean COMEX data for a specific metal
    
    Parameters:
    -----------
    metal : str
        Metal name (GOLD, SILVER, or COPPER)
    input_file : str
        Path to input CSV file
    freq : str
        Frequency (D for daily)
    unit : str
        Unit of measurement (oz for gold/silver, mt for copper)
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
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Initialize list to store cleaned observations
    observations = []
    
    # Determine which columns are available
    if metal == 'COPPER':
        # Copper uses different column names and metric tonnes
        metric_columns = {
            'Registered': 'registered_inventory',
            'Eligible': 'eligible_inventory',
            'Total_Copper': 'total_inventory'
        }
    elif metal == 'GOLD':
        # Gold has Pledged column
        metric_columns = {
            'Registered': 'registered_inventory',
            'Pledged': 'pledged_inventory',
            'Eligible': 'eligible_inventory',
            'Combined_Total': 'total_inventory'
        }
    else:  # SILVER
        metric_columns = {
            'Registered': 'registered_inventory',
            'Eligible': 'eligible_inventory',
            'Combined_Total': 'total_inventory'
        }
    
    # Process each row and convert to long format
    for _, row in df.iterrows():
        as_of_date = row['Date']
        
        # Extract values for quality check
        registered = None
        eligible = None
        total = None
        
        for col, metric_name in metric_columns.items():
            if col not in df.columns:
                continue
                
            value = row[col]
            
            # Skip NaN values
            if pd.isna(value):
                continue
            
            # Convert copper from short tons to metric tonnes
            if metal == 'COPPER':
                value = value * 0.90718474
            
            # Store for quality check
            if 'registered' in metric_name:
                registered = value
            elif 'eligible' in metric_name:
                eligible = value
            elif 'total' in metric_name:
                total = value
            
            # Create observation
            obs = {
                'metal': metal,
                'source': 'COMEX',
                'freq': freq,
                'as_of_date': as_of_date,
                'metric': metric_name,
                'value': value,
                'unit': unit,
                'is_imputed': False,
                'method': method,
                'quality': 'ok',
                'quality_notes': None,
                'load_run_id': load_run_id,
                'raw_file': raw_file,
                'raw_checksum': raw_checksum
            }
            
            observations.append(obs)
    
    # Convert to DataFrame
    clean_df = pd.DataFrame(observations)
    
    # Apply quality checks for COMEX data
    # Group by date to check eligible + registered vs total
    if registered is not None and eligible is not None and total is not None:
        dates = clean_df['as_of_date'].unique()
        
        for date in dates:
            date_data = clean_df[clean_df['as_of_date'] == date]
            
            reg_row = date_data[date_data['metric'] == 'registered_inventory']
            elig_row = date_data[date_data['metric'] == 'eligible_inventory']
            total_row = date_data[date_data['metric'] == 'total_inventory']
            
            if not reg_row.empty and not elig_row.empty and not total_row.empty:
                reg_val = reg_row['value'].iloc[0]
                elig_val = elig_row['value'].iloc[0]
                total_val = total_row['value'].iloc[0]
                
                # Calculate difference
                calculated_total = reg_val + elig_val
                difference = abs(total_val - calculated_total)
                
                # Set threshold (0.1% of total or 1000, whichever is larger)
                threshold = max(total_val * 0.001, 1000)
                
                if difference > threshold:
                    # Mark all observations for this date as warning
                    mask = clean_df['as_of_date'] == date
                    clean_df.loc[mask, 'quality'] = 'warn'
                    clean_df.loc[mask, 'quality_notes'] = f'Total mismatch: |total - (reg+elig)| = {difference:.2f} > {threshold:.2f}'
    
    return clean_df


def main():
    """Main function to clean all COMEX data files"""
    
    # Define the metals and their properties
    metals_config = {
        'SILVER': {
            'file': 'comex_silver_history.csv',
            'unit': 'oz',
            'freq': 'D',
            'method': 'daily'
        },
        'GOLD': {
            'file': 'comex_gold_history.csv',
            'unit': 'oz',
            'freq': 'D',
            'method': 'daily'
        },
        'COPPER': {
            'file': 'comex_copper_history.csv',
            'unit': 'mt',
            'freq': 'D',
            'method': 'daily'
        }
    }
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # List to store all cleaned data
    all_clean_data = []
    
    # Process each metal
    for metal, config in metals_config.items():
        input_file = os.path.join(script_dir, config['file'])
        
        if not os.path.exists(input_file):
            print(f"Warning: {input_file} not found, skipping {metal}")
            continue
        
        print(f"Processing {metal}...")
        
        clean_data = clean_comex_data(
            metal=metal,
            input_file=input_file,
            freq=config['freq'],
            unit=config['unit'],
            method=config['method']
        )
        
        all_clean_data.append(clean_data)
        print(f"  - Processed {len(clean_data)} observations")
    
    # Combine all data
    if all_clean_data:
        final_df = pd.concat(all_clean_data, ignore_index=True)
        
        # Sort by date and metal
        final_df = final_df.sort_values(['as_of_date', 'metal', 'metric']).reset_index(drop=True)
        
        # Save to output file
        output_file = os.path.join(script_dir, 'clean_observations.csv')
        final_df.to_csv(output_file, index=False)
        
        print(f"\n✓ Cleaning complete!")
        print(f"  Total observations: {len(final_df)}")
        print(f"  Date range: {final_df['as_of_date'].min()} to {final_df['as_of_date'].max()}")
        print(f"  Output file: {output_file}")
        
        # Print quality summary
        quality_summary = final_df['quality'].value_counts()
        print(f"\nQuality summary:")
        for quality, count in quality_summary.items():
            print(f"  {quality}: {count}")
        
        # Show warnings if any
        warnings = final_df[final_df['quality'] == 'warn']
        if not warnings.empty:
            print(f"\n⚠ {len(warnings)} warnings found")
            unique_notes = warnings['quality_notes'].unique()
            for note in unique_notes[:5]:  # Show first 5 unique warnings
                print(f"  - {note}")
        
        return final_df
    else:
        print("No data files found to process")
        return None


if __name__ == "__main__":
    main()
