
import os
import re
import argparse
import shutil
import hashlib
import json
import datetime

# CONFIG
import sys
sys.path.append(os.getcwd())
from scripts.utils.path_config import GET_DATA_ROOT

BASE_DIR = os.path.join(GET_DATA_ROOT(), "MASTER_DATA")
GOVERNANCE_DIR = os.path.join(os.getcwd(), "GOVERNANCE", "MIGRATIONS")

# REGEX FOR OLD FORMAT (Approximate)
# XAUUSD_2m_2025_MT5_RAW.csv
LEGACY_PATTERN = re.compile(r"^(?P<asset>[A-Z0-9]+)_(?P<tf>\d+[mh])_(?P<year>\d{4})_(?P<source>[A-Z0-9]+)_(?P<type>RAW|CLEAN|RESEARCH)(?:_.*)?\.csv$")

# REGEX FOR NEW FORMAT (Strict) in case of re-run
NEW_PATTERN = re.compile(r"^(?P<asset>[A-Z0-9]+)_(?P<feed>[A-Z]+)_(?P<tf>\d+[mh])_(?P<year>\d{4})_(?P<type>RAW|CLEAN|RESEARCH)(?:_.*)?\.csv$")


def get_file_hash(filepath):
    sha = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while True:
                block = f.read(65536)
                if not block: break
                sha.update(block)
        return sha.hexdigest()
    except Exception as e:
        return f"ERROR-{e}"

def get_feed_from_source(source, master_folder_name):
    """
    Deterministic mapping per Addendum A3.
    """
    # Explicit Mapping Table
    if source == "MT5": return "OCTAFX"
    if source == "DELTA": return "DELTA"
    
    # Fallback to Master Context if unambiguous and critical
    if "OCTAFX" in master_folder_name and source in ["MT5", "OCTAFX"]:
        return "OCTAFX"
    
    # Critial Fail for unknown sources
    raise ValueError(f"CRITICAL: Unknown or Ambiguous SOURCE '{source}' for folder '{master_folder_name}'. Cannot map to FEED.")

def plan_migration_for_file(filepath):
    dirname = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    master_folder = os.path.basename(os.path.dirname(os.path.dirname(filepath)))
    
    name_root, ext = os.path.splitext(filename)
    
    # Handle sidecar suffix
    suffix = ""
    if "_manifest" in name_root:
        real_root = name_root.replace("_manifest", "")
        suffix = "_manifest"
    elif "_lineage" in name_root:
        real_root = name_root.replace("_lineage", "")
        suffix = "_lineage"
    else:
        real_root = name_root

    # Check if already canonical
    if NEW_PATTERN.match(real_root + ".csv"):
        return None  # Skip, already valid

    virtual_csv = real_root + ".csv"
    match = LEGACY_PATTERN.match(virtual_csv)
    
    if not match:
        return None # Not a legacy file we recognize

    asset = match.group("asset")
    tf = match.group("tf")
    year = match.group("year")
    source = match.group("source")
    dtype = match.group("type")
    
    try:
        feed = get_feed_from_source(source, master_folder)
    except ValueError as e:
        print(f"  [ERROR] {e}")
        return None

    # Construct New Name: [ASSET]_[FEED]_[TIMEFRAME]_[YEAR]_[TYPE]
    new_root = f"{asset}_{feed}_{tf}_{year}_{dtype}"
    new_filename = new_root + suffix + ext
    
    if new_filename == filename:
        return None

    return {
        "old_name": filename,
        "new_name": new_filename,
        "path": filepath,
        "dir": dirname,
        "source": source,
        "feed": feed,
        "asset": asset,
        "timeframe": tf,
        "year": year,
        "type": dtype,
        "sha256": get_file_hash(filepath) # Pre-rename hash
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="Perform actual renaming")
    args = parser.parse_args()
    
    if args.execute:
        print("!!! DESTRUCTIVE ACTION: EXECUTING RENAMES !!!")
    else:
        print("--- MIGRATION PLANNING (DRY RUN) ---")

    migration_list = []
    
    # Scan
    for root, dirs, files in os.walk(BASE_DIR):
        # SKIP ARCHIVE
        if "ARCHIVE" in root:
            continue
            
        for f in files:
            if f.endswith(".csv") or f.endswith(".json"):
                 path = os.path.join(root, f)
                 item = plan_migration_for_file(path)
                 if item:
                     migration_list.append(item)

    # Check for duplicates
    target_names = [x['new_name'] for x in migration_list]
    if len(target_names) != len(set(target_names)):
        print("CRITICAL EXCEPTION: Duplicate target filenames detected! Aborting.")
        # Find duplicates for debug
        seen = set()
        for x in target_names:
            if x in seen:
                print(f"  Duplicate Target: {x}")
            seen.add(x)
        return

    # Generate Plan Object
    plan_data = {
        "timestamp_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "total_files": len(migration_list),
        "migrations": migration_list
    }

    # Save Plan
    plan_file = "migration_plan.json"
    with open(plan_file, 'w') as f:
        json.dump(plan_data, f, indent=2)
    
    print(f"Plan generated: {plan_file} ({len(migration_list)} files)")
    
    if args.execute:
        # EXECUTE
        success_count = 0
        for item in migration_list:
            old_path = item['path']
            new_path = os.path.join(item['dir'], item['new_name'])
            
            try:
                os.rename(old_path, new_path)
                print(f"  [RENAME] {item['old_name']} -> {item['new_name']}")
                success_count += 1
            except Exception as e:
                print(f"  [FAIL] {item['old_name']}: {e}")
        
        # Archive Plan
        if not os.path.exists(GOVERNANCE_DIR):
            os.makedirs(GOVERNANCE_DIR)
        
        archive_name = f"TIMEFRAME_FILENAME_MIGRATION_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        archive_path = os.path.join(GOVERNANCE_DIR, archive_name)
        shutil.copy(plan_file, archive_path)
        print(f"Migration Plan Archived: {archive_path}")
        print(f"Success: {success_count}/{len(migration_list)}")
    
    else:
        # Dry Run Output
        for item in migration_list[:5]:
             print(f"  [PLAN] {item['old_name']} -> {item['new_name']}")
        if len(migration_list) > 5:
            print(f"  ... and {len(migration_list)-5} more.")

if __name__ == "__main__":
    main()
