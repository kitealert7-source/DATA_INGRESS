import os
import re
import yaml
import pandas as pd
from datetime import datetime
import MetaTrader5 as mt5
from pathlib import Path

_DI = Path(__file__).resolve().parents[2]
BASE_DIR = str(Path(__file__).resolve().parents[3] / "Anti_Gravity_DATA_ROOT" / "MASTER_DATA" / "XAUUSD_OCTAFX_MASTER")
LAYERS = ["RAW", "CLEAN", "RESEARCH"]
TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1mn"]
# Mapping custom timeframe to minutes for expected bar calculation (approx)
TF_MINUTES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
    "1mn": 43200
}

MT5_TIMEFRAMES = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15,
    "30m": mt5.TIMEFRAME_M30,
    "1h": mt5.TIMEFRAME_H1,
    "4h": mt5.TIMEFRAME_H4,
    "1d": mt5.TIMEFRAME_D1,
    "1w": mt5.TIMEFRAME_W1,
    "1mn": mt5.TIMEFRAME_MN1
}

def analyze_file(filepath, tf):
    try:
        df = pd.read_csv(filepath)
        if 'time' not in df.columns:
            return None
        
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df = df.sort_values('time')
        
        row_count = len(df)
        if row_count == 0:
            return None
            
        start_ts = df['time'].min()
        end_ts = df['time'].max()
        
        duplicates = len(df) - len(df.drop_duplicates(subset=['time']))
        
        # Calculate gaps
        df['delta'] = df['time'].diff()
        expected_sec = TF_MINUTES[tf] * 60
        # Ignore weekend gaps for now, just find max continuous gap
        max_gap = df['delta'].max().total_seconds() if len(df) > 1 else 0
        
        # Determine continuous span by checking gaps > 1 week as major gaps
        major_gaps = df[df['delta'].dt.total_seconds() > 7 * 86400]
        calendar_span_years = (end_ts - start_ts).days / 365.25
        
        is_pre_2020 = start_ts.year < 2020
        is_pre_2018 = start_ts.year < 2018
        
        return {
            'row_count': row_count,
            'start_ts': start_ts.isoformat(),
            'end_ts': end_ts.isoformat(),
            'duplicates': duplicates,
            'max_gap_hours': round(max_gap / 3600, 2),
            'calendar_span_years': round(calendar_span_years, 2),
            'major_gap_count': len(major_gaps),
            'is_pre_2020': bool(is_pre_2020),
            'is_pre_2018': bool(is_pre_2018),
            'start_year': start_ts.year
        }
    except Exception as e:
        return {'error': str(e)}

def discover_files():
    data = {}
    for tf in TIMEFRAMES:
        data[tf] = {
            'raw': {'present': False, 'files': []},
            'clean': {'present': False, 'files': []},
            'research': {'present': False, 'files': []}
        }
        
    for layer in LAYERS:
        layer_dir = os.path.join(BASE_DIR, layer)
        if not os.path.exists(layer_dir):
            continue
            
        for f in os.listdir(layer_dir):
            if f.endswith(".csv"):
                # parse format: XAUUSD_OCTAFX_{tf}_{year}_{layer}.csv
                parts = f.split('_')
                if len(parts) >= 5:
                    tf = parts[2]
                    if tf in data:
                        data[tf][layer.lower()]['present'] = True
                        data[tf][layer.lower()]['files'].append(os.path.join(layer_dir, f))
    return data

def main():
    print("Starting XAUUSD Data Audit...")
    files_data = discover_files()
    
    report = {
        'xauusd_data_audit': {}
    }
    
    for tf, layers in files_data.items():
        if not (layers['raw']['present'] or layers['clean']['present'] or layers['research']['present']):
            continue
            
        tf_report = {
            'layers_available': {
                'raw': layers['raw']['present'],
                'clean': layers['clean']['present'],
                'research': layers['research']['present']
            },
            'layer_stats': {}
        }
        
        # Analyze all files for the timeframe, combined across years
        for layer in ['raw', 'clean', 'research']:
            if layers[layer]['present']:
                files = sorted(layers[layer]['files'])
                layer_earliest = "9999-12-31"
                layer_latest = "1970-01-01"
                total_rows = 0
                total_dupes = 0
                max_gap = 0
                pre_2020 = False
                pre_2018 = False
                files_analyzed = 0
                
                for f in files:
                    stats = analyze_file(f, tf)
                    if stats and 'error' not in stats:
                        layer_earliest = min(layer_earliest, stats['start_ts'])
                        layer_latest = max(layer_latest, stats['end_ts'])
                        total_rows += stats['row_count']
                        total_dupes += stats['duplicates']
                        max_gap = max(max_gap, stats['max_gap_hours'])
                        pre_2020 = pre_2020 or stats['is_pre_2020']
                        pre_2018 = pre_2018 or stats['is_pre_2018']
                        files_analyzed += 1
                
                if files_analyzed > 0:
                    try:
                        t1 = datetime.fromisoformat(layer_earliest)
                        t2 = datetime.fromisoformat(layer_latest)
                        calendar_years = (t2 - t1).days / 365.25
                    except:
                        calendar_years = 0

                    tf_report['layer_stats'][layer] = {
                        'total_rows': total_rows,
                        'earliest_timestamp': layer_earliest,
                        'latest_timestamp': layer_latest,
                        'total_duplicates': total_dupes,
                        'max_continuous_gap_hours': max_gap,
                        'calendar_span_years': round(calendar_years, 2),
                        'pre_2020_data_exists': pre_2020,
                        'pre_2018_data_exists': pre_2018,
                        'files_analyzed': files_analyzed
                    }
        
        # Cross-layer consistency check
        consistency = {}
        if 'raw' in tf_report['layer_stats'] and 'clean' in tf_report['layer_stats'] and 'research' in tf_report['layer_stats']:
            raw_start = tf_report['layer_stats']['raw']['earliest_timestamp']
            clean_start = tf_report['layer_stats']['clean']['earliest_timestamp']
            res_start = tf_report['layer_stats']['research']['earliest_timestamp']
            
            consistency['starts_aligned'] = raw_start <= clean_start <= res_start
            consistency['raw_start'] = raw_start
            consistency['clean_start'] = clean_start
            consistency['research_start'] = res_start
            
            # check for missing rows (clean vs research usually the same or res<clean)
            consistency['raw_rows'] = tf_report['layer_stats']['raw']['total_rows']
            consistency['clean_rows'] = tf_report['layer_stats']['clean']['total_rows']
            consistency['research_rows'] = tf_report['layer_stats']['research']['total_rows']
            
        tf_report['consistency'] = consistency
        report['xauusd_data_audit'][tf] = tf_report
    
    # Check MT5
    broker_estimate = {}
    print("Checking MT5 Broker history...")
    if mt5.initialize():
        symbol = "XAUUSD"
        now = datetime.now()
        # Find earliest available date per timeframe by requesting huge number of bars
        for tf, mt5_tf in MT5_TIMEFRAMES.items():
            if tf in report['xauusd_data_audit'] or tf in ['1m', '5m', '15m', '1h', '1d']:
                bars = mt5.copy_rates_from(symbol, mt5_tf, now, 100000000)
                if bars is not None and len(bars) > 0:
                    earliest = pd.to_datetime(bars[0]['time'], unit='s').isoformat()
                    broker_estimate[tf] = {
                        'max_bars_retrievable': len(bars),
                        'earliest_server_history': earliest
                    }
                else:
                    broker_estimate[tf] = 'Unavailable or API error'
        mt5.shutdown()
    else:
        broker_estimate = 'MT5 Initialization failed. Terminal closed?'

    report['broker_max_history_estimate'] = broker_estimate
    
    overall = {
        'longest_timeframe_available': "1d" if "1d" in report['xauusd_data_audit'] else None,
        'shortest_timeframe_available': "1m" if "1m" in report['xauusd_data_audit'] else min([k for k in report['xauusd_data_audit'].keys()] + ['None']),
        'rebuild_required': False # Just a placeholder assumption
    }
    report['overall_status'] = overall

    with open(str(_DI / "reports" / "xauusd_audit_report.yaml"), 'w') as f:
        yaml.dump(report, f, sort_keys=False, default_flow_style=False)
        
    print("Audit Complete. Saved to reports/xauusd_audit_report.yaml")

if __name__ == "__main__":
    main()
