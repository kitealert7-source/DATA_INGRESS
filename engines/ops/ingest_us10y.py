"""
US10Y Sovereign Rates PRICE Ingestion Script
Governance: SOP v17-DV1 Compliant
Source: Yahoo Finance (ZN=F - US 10Y Treasury Note Futures)

CRITICAL: This downloads PRICE data, NOT yield data.
"""

import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import hashlib
import json
from pathlib import Path

# Paths
_AG_US10Y = Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT" / "MASTER_DATA" / "US10Y_YAHOO_MASTER"
RAW_DIR      = str(_AG_US10Y / "RAW")
CLEAN_DIR    = str(_AG_US10Y / "CLEAN")
RESEARCH_DIR = str(_AG_US10Y / "RESEARCH")

# Ticker
TICKER = "ZN=F"  # US 10-Year Treasury Note Futures
ASSET = "US10Y"
FEED = "YAHOO"
TIMEFRAME = "1d"

def download_us10y_data():
    """Download full history from Yahoo Finance"""
    print(f"Downloading {TICKER} from Yahoo Finance...")
    
    try:
        # Download full available history
        ticker = yf.Ticker(TICKER)
        df = ticker.history(period="max", interval="1d")
        
        if df.empty:
            raise ValueError(f"No data returned for {TICKER}")
        
        print(f"Downloaded {len(df)} bars from {df.index[0]} to {df.index[-1]}")
        
        # Convert index to timezone-naive UTC
        df.index = df.index.tz_localize(None)
        
        # Keep only OHLC columns
        df = df[['Open', 'High', 'Low', 'Close']].copy()
        df.columns = ['open', 'high', 'low', 'close']
        
        # Reset index to make timestamp a column
        df.reset_index(inplace=True)
        df.rename(columns={'Date': 'time'}, inplace=True)
        
        return df
        
    except Exception as e:
        print(f"ERROR downloading data: {e}")
        raise

def split_by_year_and_save_raw(df):
    """Split data by year and save RAW files"""
    print("\n=== RAW STAGE ===")
    
    df['year'] = pd.to_datetime(df['time']).dt.year
    years = sorted(df['year'].unique())
    
    raw_files = []
    
    for year in years:
        year_df = df[df['year'] == year].copy()
        year_df = year_df.drop(columns=['year'])
        
        # Format timestamp as YYYY-MM-DD
        year_df['time'] = pd.to_datetime(year_df['time']).dt.strftime('%Y-%m-%d')
        
        filename = f"{ASSET}_{FEED}_{TIMEFRAME}_{year}_RAW.csv"
        filepath = os.path.join(RAW_DIR, filename)
        
        # Save with NO index
        year_df.to_csv(filepath, index=False)
        
        raw_files.append((year, filename, len(year_df)))
        print(f"  Saved {filename} ({len(year_df)} rows)")
    
    return raw_files

def generate_clean_stage():
    """Generate CLEAN datasets from RAW"""
    print("\n=== CLEAN STAGE ===")
    
    raw_files = sorted([f for f in os.listdir(RAW_DIR) if f.endswith('_RAW.csv')])
    
    for raw_file in raw_files:
        raw_path = os.path.join(RAW_DIR, raw_file)
        
        # Read RAW
        df = pd.read_csv(raw_path)
        
        # Ensure monotonic timestamps
        df['time'] = pd.to_datetime(df['time'])
        df = df.sort_values('time').reset_index(drop=True)
        
        # Remove duplicates
        initial_count = len(df)
        df = df.drop_duplicates(subset=['time'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        # Remove zero OHLC bars
        zero_mask = (df['open'] == 0) | (df['high'] == 0) | (df['low'] == 0) | (df['close'] == 0)
        zero_bars = zero_mask.sum()
        df = df[~zero_mask].reset_index(drop=True)
        
        # Format timestamp back to string
        df['time'] = df['time'].dt.strftime('%Y-%m-%d')
        
        # Generate clean filename
        clean_file = raw_file.replace('_RAW.csv', '_CLEAN.csv')
        clean_path = os.path.join(CLEAN_DIR, clean_file)
        
        # Save CLEAN
        df.to_csv(clean_path, index=False)
        
        # Calculate SHA256
        with open(clean_path, 'rb') as f:
            clean_sha256 = hashlib.sha256(f.read()).hexdigest()[:16]
        
        print(f"  {clean_file}: {len(df)} rows (removed {duplicates_removed} dupes, {zero_bars} zero bars)")
        print(f"    Range: {df.iloc[0]['time']} to {df.iloc[-1]['time']}")
        print(f"    SHA256: {clean_sha256}")

def generate_research_stage():
    """Generate RESEARCH datasets from CLEAN"""
    print("\n=== RESEARCH STAGE ===")
    
    clean_files = sorted([f for f in os.listdir(CLEAN_DIR) if f.endswith('_CLEAN.csv')])
    
    for clean_file in clean_files:
        clean_path = os.path.join(CLEAN_DIR, clean_file)
        
        # Read CLEAN
        df = pd.read_csv(clean_path)
        
        # RESEARCH = CLEAN for non-tradable data (no execution model)
        research_df = df.copy()
        
        # Generate research filename
        research_file = clean_file.replace('_CLEAN.csv', '_RESEARCH.csv')
        research_path = os.path.join(RESEARCH_DIR, research_file)
        
        # Create metadata header
        metadata = [
            f"# dataset_version: RESEARCH_v1_NOEXECv1_NOSESSIONv1",
            f"# schema_version: SOP_v17_DV1",
            f"# generation_timestamp_utc: {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}",
            f"# source: YAHOO",
            f"# ticker: ZN=F",
            f"# instrument: US_10Y_TREASURY_NOTE_FUTURES",
            f"# data_type: PRICE",
            f"# execution_model_version: NONE",
            f"# tradable: FALSE",
            f"# macro_structural_only: TRUE"
        ]
        
        # Write metadata header + data
        with open(research_path, 'w') as f:
            f.write('\n'.join(metadata) + '\n')
            research_df.to_csv(f, index=False)
        
        # Create lineage file
        lineage_file = research_file.replace('.csv', '_lineage.json')
        lineage_path = os.path.join(RESEARCH_DIR, lineage_file)
        
        lineage = {
            "source_file": clean_file,
            "transformation": "NONE (non-tradable structural data)",
            "execution_model": "NONE",
            "governance_version": "SOP_v17_DV1",
            "generated_utc": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        }
        
        with open(lineage_path, 'w') as f:
            json.dump(lineage, f, indent=2)
        
        print(f"  {research_file}: {len(research_df)} rows")

def main():
    print("=" * 60)
    print("US10Y SOVEREIGN RATES PRICE INGESTION")
    print("Governance: SOP v17-DV1")
    print("Source: Yahoo Finance (ZN=F)")
    print("=" * 60)
    
    # Step 1: Download
    df = download_us10y_data()
    
    # Step 2: RAW stage
    raw_files = split_by_year_and_save_raw(df)
    
    # Step 3: CLEAN stage
    generate_clean_stage()
    
    # Step 4: RESEARCH stage
    generate_research_stage()
    
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"Total years ingested: {len(raw_files)}")
    print(f"Coverage: {raw_files[0][0]} - {raw_files[-1][0]}")
    print("\nNext step: Run dataset_validator_sop17.py --audit-all")

if __name__ == "__main__":
    main()
