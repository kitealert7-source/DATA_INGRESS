"""
Fix timestamp format in XAUUSD_OCTAFX_1m_2025_RAW.csv
Converts DD-MM-YYYY HH:MM to MM-DD-YYYY HH:MM
"""
import pandas as pd
from pathlib import Path

data_root = Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT"
raw_file = data_root / 'MASTER_DATA' / 'XAUUSD_OCTAFX_MASTER' / 'RAW' / 'XAUUSD_OCTAFX_1m_2025_RAW.csv'

print('='*60)
print('XAUUSD_OCTAFX_1m_2025_RAW.csv TIMESTAMP FIX')
print('='*60)
print()

# Read file
df = pd.read_csv(raw_file)
original_count = len(df)
print(f'Original row count: {original_count}')
print()

# Parse with dayfirst=True (DD-MM-YYYY format)
print('Parsing timestamps with dayfirst=True...')
df['time'] = pd.to_datetime(df['time'], dayfirst=True)

# Check for any parsing issues
null_times = df['time'].isna().sum()
print(f'Null timestamps after parsing: {null_times}')
if null_times > 0:
    print('ERROR: Parsing introduced null values. Aborting.')
    exit(1)

# Convert to MM-DD-YYYY HH:MM format
print('Converting to MM-DD-YYYY HH:MM format...')
df['time'] = df['time'].dt.strftime('%m-%d-%Y %H:%M')

# Verify conversion
print()
print('Sample converted timestamps:')
print('  Row 0:', df['time'].iloc[0])
print('  Row', len(df)//2, ':', df['time'].iloc[len(df)//2])
print('  Row', len(df)-1, ':', df['time'].iloc[-1])
print()

# Verify row count preserved
final_count = len(df)
print(f'Final row count: {final_count}')
if original_count != final_count:
    print('ERROR: Row count changed! Aborting.')
    exit(1)

# Save in-place
print()
print(f'Saving to: {raw_file}')
df.to_csv(raw_file, index=False)

print()
print('[SUCCESS] Timestamp format normalized.')
print('='*60)
