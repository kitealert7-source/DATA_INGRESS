"""
Dataset Version Governor (DVG) - SOP v17-DV1 Compliant
=======================================================
Implements structural versioning for CLEAN → RESEARCH pipeline.

Format: RESEARCH_v<r>_EXECv<e>_SESSIONv<s>

Usage:
    from dataset_version_governor_v17 import DatasetVersionGovernor
    
    dvg = DatasetVersionGovernor()
    version_meta = dvg.compute_version(
        prev_research_manifest_path="path/to/lineage.json",
        new_clean_manifest_path="path/to/clean_manifest.json",
        exec_model_version="octafx_exec_v1.0",
        session_filter_version="SESSIONv1"
    )
"""

import os

ENGINE_VERSION = "SOP17_INCREMENTAL_STABLE_v1"
import json
import hashlib
import re
import datetime
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List


class DatasetVersionGovernor:
    """
    SOP v17-DV1 Compliant Dataset Version Governor.
    
    Responsibilities:
    1. Load previous RESEARCH manifest (if exists)
    2. Load current CLEAN manifest
    3. Apply versioning rules to detect deltas
    4. Produce new dataset_version
    5. Persist into lineage.json
    6. Ensure deterministic and idempotent behavior
    """
    
    # Lock file settings (per SOP §14)
    LOCK_FILE_NAME = ".dvg.lock"
    LOCK_TIMEOUT_SECONDS = 300  # 5 minutes
    
    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = base_dir or os.getcwd()
    
    # =========================================================================
    # PUBLIC API
    # =========================================================================
    
    def compute_version(
        self,
        prev_research_manifest_path: Optional[str],
        new_clean_manifest_path: str,
        exec_model_version: str,
        session_filter_version: str,
        allow_initial_create: bool = True
    ) -> Dict[str, Any]:
        """
        Compute the new dataset_version based on structural changes.
        
        Args:
            prev_research_manifest_path: Path to previous lineage.json (None if first run)
            new_clean_manifest_path: Path to current clean_manifest.json
            exec_model_version: e.g., "octafx_exec_v1.0"
            session_filter_version: e.g., "SESSIONv1"
            allow_initial_create: If True, allows creation of v1 when no previous exists
            
        Returns:
            dict with dataset_version, deltas, previous_version, notes
        """
        # Load manifests
        prev_manifest = None
        if prev_research_manifest_path and os.path.exists(prev_research_manifest_path):
            prev_manifest = self._load_json(prev_research_manifest_path)
        
        if not os.path.exists(new_clean_manifest_path):
            raise FileNotFoundError(f"CLEAN manifest not found: {new_clean_manifest_path}")
        
        new_clean_manifest = self._load_json(new_clean_manifest_path)
        
        # Initialize deltas
        deltas = {
            "clean_sha256_changed": False,
            "exec_changed": False,
            "session_changed": False,
            "fields_changed": [],
            "schema_changed": False
        }
        
        notes = []
        
        # Parse previous version components
        prev_r, prev_e, prev_s = 0, 0, 0
        prev_version_str = None
        migrated_from = None
        
        if prev_manifest:
            prev_version_str = prev_manifest.get("dataset_version", "")
            
            # Check for legacy date-based version
            if self._is_legacy_version(prev_version_str):
                migration_result = self.migrate_legacy_version(prev_version_str)
                migrated_from = prev_version_str
                prev_r, prev_e, prev_s = 1, 1, 1  # Baseline to v1
                notes.append(f"Migrated from legacy version: {prev_version_str}")
            else:
                prev_r, prev_e, prev_s = self._parse_version(prev_version_str)
        
        # Determine new version components
        new_r, new_e, new_s = prev_r, prev_e, prev_s
        
        # First-time creation
        if prev_manifest is None:
            if not allow_initial_create:
                raise ValueError("No previous manifest and allow_initial_create=False")
            new_r, new_e, new_s = 1, 1, 1
            notes.append("Initial dataset creation.")
        else:
            # Check CLEAN SHA256 change (structural)
            prev_clean_sha = prev_manifest.get("clean_manifest", {}).get("clean_sha256", "")
            new_clean_sha = new_clean_manifest.get("clean_sha256", "")
            
            if prev_clean_sha != new_clean_sha:
                deltas["clean_sha256_changed"] = True
                new_r += 1
                notes.append("CLEAN SHA changed (new bars or structural modification).")
            
            # Check schema change
            prev_schema = prev_manifest.get("clean_manifest", {}).get("schema_version", "1.0.0")
            new_schema = new_clean_manifest.get("schema_version", "1.0.0")
            if prev_schema != new_schema:
                deltas["schema_changed"] = True
                new_r += 1
                notes.append(f"Schema changed: {prev_schema} → {new_schema}")
            
            # Check column changes
            prev_cols = set(prev_manifest.get("clean_manifest", {}).get("columns", []))
            new_cols = set(new_clean_manifest.get("columns", []))
            added_cols = new_cols - prev_cols
            removed_cols = prev_cols - new_cols
            
            if added_cols or removed_cols:
                deltas["fields_changed"] = list(added_cols | removed_cols)
                if not deltas["clean_sha256_changed"]:  # Avoid double increment
                    new_r += 1
                notes.append(f"Column changes: +{list(added_cols)}, -{list(removed_cols)}")
            
            # Check execution model change
            prev_exec = prev_manifest.get("execution_model_version", "")
            if prev_exec != exec_model_version:
                deltas["exec_changed"] = True
                new_e += 1
                notes.append(f"Execution model changed: {prev_exec} → {exec_model_version}")
            
            # Check session filter change
            prev_session = prev_manifest.get("session_filter_version", "")
            if prev_session != session_filter_version:
                deltas["session_changed"] = True
                new_s += 1
                notes.append(f"Session filter changed: {prev_session} → {session_filter_version}")
        
        # Build version string
        new_version = f"RESEARCH_v{new_r}_EXECv{new_e}_SESSIONv{new_s}"
        
        result = {
            "dataset_version": new_version,
            "deltas": deltas,
            "previous_version": prev_version_str,
            "notes": " ".join(notes) if notes else "No changes detected.",
            "r": new_r,
            "e": new_e,
            "s": new_s
        }
        
        if migrated_from:
            result["migrated_from_legacy"] = migrated_from
        
        return result
    
    def compute_sha256(self, file_path: str) -> str:
        """
        Compute SHA256 hash of file content.
        
        For CSV files, skips header comments (lines starting with #).
        """
        sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            # For CSVs, we hash only data content (skip # comments)
            if file_path.endswith('.csv'):
                for line in f:
                    if not line.startswith(b'#'):
                        sha256.update(line)
            else:
                for chunk in iter(lambda: f.read(8192), b''):
                    sha256.update(chunk)
        
        return sha256.hexdigest()
    
    def generate_clean_manifest(self, clean_csv_path: str) -> Dict[str, Any]:
        """
        Generate manifest for a CLEAN CSV file.
        
        Returns:
            dict with clean_sha256, bar_count, first_bar_utc, last_bar_utc, columns, schema_version
        """
        if not os.path.exists(clean_csv_path):
            raise FileNotFoundError(f"CLEAN file not found: {clean_csv_path}")
            
        fast_path = False
        meta_path = clean_csv_path + ".meta.json"
        clean_dir = os.path.dirname(clean_csv_path)
        existing_manifest_path = os.path.join(clean_dir, "clean_manifest.json")
        
        if os.path.exists(meta_path) and os.path.exists(existing_manifest_path):
            try:
                stat = os.stat(clean_csv_path)
                with open(meta_path, 'r') as f:
                    meta = json.load(f)
                if meta.get("file_size") == stat.st_size and meta.get("mtime") == stat.st_mtime:
                    fast_path = True
            except:
                pass

        sha256 = self.compute_sha256(clean_csv_path)
        
        if fast_path:
            with open(existing_manifest_path, 'r') as f:
                manifest = json.load(f)
            manifest["clean_sha256"] = sha256
            manifest["generated_utc"] = datetime.datetime.utcnow().isoformat() + "Z"
            if "bar_count" not in manifest:
                manifest["bar_count"] = meta.get("total_lines", 0) - 1 # approximate if missing, though it shouldn't be
            return manifest
        
        # Read CSV to extract metadata
        df = pd.read_csv(clean_csv_path, comment='#')
        
        # Extract bar info
        bar_count = len(df)
        columns = list(df.columns)
        
        first_bar = None
        last_bar = None
        
        if 'time' in df.columns and bar_count > 0:
            df['time'] = pd.to_datetime(df['time'])
            first_bar = df['time'].min().isoformat() + "Z"
            last_bar = df['time'].max().isoformat() + "Z"
        
        manifest = {
            "clean_sha256": sha256,
            "bar_count": bar_count,
            "first_bar_utc": first_bar,
            "last_bar_utc": last_bar,
            "columns": columns,
            "schema_version": "1.0.0",
            "generated_utc": datetime.datetime.utcnow().isoformat() + "Z"
        }
        
        return manifest
    
    def save_clean_manifest(self, clean_csv_path: str, manifest: Optional[Dict] = None) -> str:
        """
        Generate and save clean_manifest.json in the same directory as the CLEAN CSV.
        
        Returns:
            Path to saved manifest
        """
        if manifest is None:
            manifest = self.generate_clean_manifest(clean_csv_path)
        
        clean_dir = os.path.dirname(clean_csv_path)
        manifest_path = os.path.join(clean_dir, "clean_manifest.json")
        
        self._save_json(manifest_path, manifest)
        return manifest_path
    
    def generate_lineage(
        self,
        version_meta: Dict[str, Any],
        clean_manifest: Dict[str, Any],
        research_csv_path: str,
        exec_model_version: str,
        session_filter_version: str
    ) -> Dict[str, Any]:
        """
        Generate full lineage.json for a RESEARCH dataset.
        """
        research_sha256 = self.compute_sha256(research_csv_path)
        
        lineage = {
            "dataset_version": version_meta["dataset_version"],
            "clean_manifest": clean_manifest,
            "research_sha256": research_sha256,
            "execution_model_version": exec_model_version,
            "session_filter_version": session_filter_version,
            "generation_timestamp_utc": datetime.datetime.utcnow().isoformat() + "Z",
            "deltas": version_meta["deltas"],
            "previous_version": version_meta.get("previous_version"),
            "notes": version_meta.get("notes", "")
        }
        
        if "migrated_from_legacy" in version_meta:
            lineage["migrated_from_legacy"] = version_meta["migrated_from_legacy"]
        
        return lineage
    
    def save_lineage(self, research_dir: str, lineage: Dict[str, Any]) -> str:
        """
        Save lineage.json to the RESEARCH directory.
        Uses atomic write (via temp file) per SOP §14.2.
        
        Returns:
            Path to saved lineage
        """
        lineage_path = os.path.join(research_dir, "lineage.json")
        self._save_json_atomic(lineage_path, lineage)
        return lineage_path
    
    # =========================================================================
    # LOCKING (SOP §14)
    # =========================================================================
    
    def acquire_lock(self, research_dir: str) -> bool:
        """
        Acquire exclusive write lock for a RESEARCH directory.
        
        Returns:
            True if lock acquired, False if already locked by another process
        """
        lock_path = os.path.join(research_dir, self.LOCK_FILE_NAME)
        
        # Check for existing lock
        if os.path.exists(lock_path):
            lock_data = self._load_json(lock_path)
            started = datetime.datetime.fromisoformat(lock_data.get("started_utc", "").rstrip("Z"))
            age_seconds = (datetime.datetime.utcnow() - started).total_seconds()
            
            if age_seconds > self.LOCK_TIMEOUT_SECONDS:
                # Stale lock, clean up
                os.remove(lock_path)
                print(f"DVG: Cleaned up stale lock (age: {age_seconds:.0f}s)")
            else:
                # Active lock by another process
                return False
        
        # Create lock
        lock_data = {
            "pid": os.getpid(),
            "started_utc": datetime.datetime.utcnow().isoformat() + "Z",
            "operation": "compute_version"
        }
        self._save_json(lock_path, lock_data)
        return True
    
    def release_lock(self, research_dir: str) -> None:
        """
        Release write lock for a RESEARCH directory.
        """
        lock_path = os.path.join(research_dir, self.LOCK_FILE_NAME)
        if os.path.exists(lock_path):
            os.remove(lock_path)
    
    # =========================================================================
    # LEGACY MIGRATION (SOP §11)
    # =========================================================================
    
    def migrate_legacy_version(self, legacy_str: str) -> Dict[str, Any]:
        """
        Map legacy date-based version to structural version.
        
        Legacy format examples:
        - RESEARCH_v17_octafx_exec_v1.0_20251212
        - BTCUSD_5m_2025_MT5_RESEARCH
        
        Returns:
            Migration info dict
        """
        return {
            "legacy_version": legacy_str,
            "mapped_to": "RESEARCH_v1_EXECv1_SESSIONv1",
            "migration_timestamp_utc": datetime.datetime.utcnow().isoformat() + "Z",
            "notes": "Baselined from legacy date-based version."
        }
    
    def log_migration(self, research_dir: str, migration_info: Dict[str, Any]) -> None:
        """
        Append migration record to migration.log per SOP §11.
        """
        log_path = os.path.join(research_dir, "migration.log")
        
        entry = (
            f"[{migration_info['migration_timestamp_utc']}] "
            f"MIGRATED: {migration_info['legacy_version']} → {migration_info['mapped_to']}\n"
        )
        
        with open(log_path, 'a', encoding='utf-8') as f:
            f.write(entry)
    
    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================
    
    def _is_legacy_version(self, version_str: str) -> bool:
        """Check if version string uses legacy date-based format."""
        if not version_str:
            return False
        
        # New format: RESEARCH_v<r>_EXECv<e>_SESSIONv<s>
        new_pattern = r"^RESEARCH_v\d+_EXECv\d+_SESSIONv\d+$"
        if re.match(new_pattern, version_str):
            return False
        
        # Anything else is legacy
        return True
    
    def _parse_version(self, version_str: str) -> tuple:
        """
        Parse RESEARCH_v<r>_EXECv<e>_SESSIONv<s> into (r, e, s).
        """
        pattern = r"^RESEARCH_v(\d+)_EXECv(\d+)_SESSIONv(\d+)$"
        match = re.match(pattern, version_str)
        
        if match:
            return int(match.group(1)), int(match.group(2)), int(match.group(3))
        
        # Default for unparseable
        return 1, 1, 1
    
    def _load_json(self, path: str) -> Dict[str, Any]:
        """Load JSON file."""
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _save_json(self, path: str, data: Dict[str, Any]) -> None:
        """Save JSON file."""
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    
    def _save_json_atomic(self, path: str, data: Dict[str, Any]) -> None:
        """
        Save JSON atomically via temp file (per SOP §14.2).
        Write to .tmp, then rename.
        """
        tmp_path = path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        # Atomic rename (on Windows, need to remove target first)
        if os.path.exists(path):
            os.remove(path)
        os.rename(tmp_path, path)


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI interface for testing DVG."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Dataset Version Governor (DVG) - SOP v17-DV1")
    parser.add_argument("--test", type=str, help="Test DVG on a MASTER folder (e.g., MASTER_DATA/BTC_OCTAFX_MASTER)")
    parser.add_argument("--generate-manifests", action="store_true", help="Generate clean manifests for all CLEAN CSVs")
    parser.add_argument("--base-dir", type=str, default=None, help="Base directory (default: cwd)")
    
    args = parser.parse_args()
    
    dvg = DatasetVersionGovernor(base_dir=args.base_dir)
    
    if args.generate_manifests:
        # Find all CLEAN CSVs and generate manifests
        import glob
        base = args.base_dir or os.getcwd()
        pattern = os.path.join(base, "MASTER_DATA", "*", "CLEAN", "*.csv")
        clean_files = glob.glob(pattern)
        
        print(f"Found {len(clean_files)} CLEAN files")
        for clean_path in clean_files:
            print(f"  Generating manifest for {os.path.basename(clean_path)}...")
            manifest = dvg.generate_clean_manifest(clean_path)
            manifest_path = dvg.save_clean_manifest(clean_path, manifest)
            print(f"    Saved: {manifest_path}")
            print(f"    SHA256: {manifest['clean_sha256'][:16]}...")
            print(f"    Bars: {manifest['bar_count']}")
        
        print("Done.")
    
    elif args.test:
        # Test DVG on a specific master folder
        master_path = args.test
        clean_dir = os.path.join(master_path, "CLEAN")
        research_dir = os.path.join(master_path, "RESEARCH")
        
        # Find first CLEAN CSV
        import glob
        clean_files = glob.glob(os.path.join(clean_dir, "*.csv"))
        if not clean_files:
            print(f"No CLEAN CSVs found in {clean_dir}")
            return
        
        clean_path = clean_files[0]
        print(f"Testing DVG on: {clean_path}")
        
        # Generate CLEAN manifest
        clean_manifest = dvg.generate_clean_manifest(clean_path)
        manifest_path = dvg.save_clean_manifest(clean_path, clean_manifest)
        print(f"CLEAN manifest: {manifest_path}")
        
        # Check for existing lineage
        lineage_path = os.path.join(research_dir, "lineage.json")
        prev_lineage = lineage_path if os.path.exists(lineage_path) else None
        
        # Compute version
        version_meta = dvg.compute_version(
            prev_research_manifest_path=prev_lineage,
            new_clean_manifest_path=manifest_path,
            exec_model_version="octafx_exec_v1.0",
            session_filter_version="SESSIONv1"
        )
        
        print(f"\nVersion Result:")
        print(f"  dataset_version: {version_meta['dataset_version']}")
        print(f"  previous_version: {version_meta.get('previous_version')}")
        print(f"  notes: {version_meta['notes']}")
        print(f"  deltas: {version_meta['deltas']}")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
