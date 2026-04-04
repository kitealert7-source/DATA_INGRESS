import os
import pandas as pd
from datetime import datetime
from pathlib import Path

TMP_DIR = str(Path(__file__).resolve().parents[2] / "tmp" / "XAUUSD_HISTORY")

def validate_exported_file(filepath):
    print(f"\n--- Validating Manual Export: {os.path.basename(filepath)} ---")
    
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return
        
    try:
        # Check basic file properties
        file_stats = os.stat(filepath)
        size_mb = file_stats.st_size / (1024 * 1024)
        print(f"File Size: {size_mb:.2f} MB")
        
        # Determine format (MT5 default export is usually tab or comma separated)
        # We will attempt to read standard CSV or tab-separated
        try:
            df = pd.read_csv(filepath, sep=None, engine='python') # auto-detect separator
        except Exception as e:
             print(f"Error parsing file as auto-delimited CSV: {e}")
             return
            
        print(f"\nDetected Columns: {list(df.columns)}")
        print(f"Total Rows: {len(df)}")
        
        # Find time column
        time_col = None
        for col in df.columns:
            if 'time' in col.lower() or 'date' in col.lower():
                time_col = col
                break
                
        if time_col is None:
             print("CRITICAL: Could not identify a time/date column.")
             print("First few rows for inspection:")
             print(df.head())
             return
             
        # MT5 sometimes exports Date and Time as separate columns
        if 'Date' in df.columns and 'Time' in df.columns:
             df['timestamp_parsed'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        else:
             df['timestamp_parsed'] = pd.to_datetime(df[time_col])
             
        df = df.sort_values('timestamp_parsed')
        
        first_row_ts = df['timestamp_parsed'].min()
        last_row_ts = df['timestamp_parsed'].max()
        
        print("\n=== Validation Report ===")
        print(f"First Row Timestamp : {first_row_ts}")
        print(f"Last Row Timestamp  : {last_row_ts}")
        print(f"Total Row Count     : {len(df)}")
        print("=========================")
        print("\nSTATUS: Waiting for user confirmation before RAW merge.")

    except Exception as e:
        print(f"Validation failed: {e}")

if __name__ == "__main__":
    # Example expected filename format, change based on actual export
    expected_h1_file = os.path.join(TMP_DIR, "XAUUSD_H1_manual.csv")
    validate_exported_file(expected_h1_file)
