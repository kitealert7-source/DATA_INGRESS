
import os
import shutil

RESULTS_DIR = os.path.join(os.getcwd(), "RESULTS")

def cleanup():
    # Walk RESULTS/BTC/DELTA/... and ETH/DELTA/...
    # Delete any timestamp folder NOT containing "0916"
    
    deleted_count = 0
    
    for root, dirs, files in os.walk(RESULTS_DIR):
        if "summary.json" in files:
            # Check if Delta
            path_parts = root.split(os.sep)
            if "DELTA" in path_parts:
                run_id = path_parts[-1] # timestamp
                if "20251210_0916" not in run_id:
                    print(f"Deleting invalid Delta run: {root}")
                    try:
                        shutil.rmtree(root)
                        deleted_count += 1
                    except Exception as e:
                        print(f"Error deleting {root}: {e}")
                else:
                    print(f"Keeping valid Delta run: {root}")
    
    print(f"Cleanup complete. Deleted {deleted_count} runs.")

if __name__ == "__main__":
    cleanup()
