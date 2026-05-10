#!/usr/bin/env python3
import subprocess
import csv
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

# --- Configuration ---
# Target Docker container name
CONTAINER = "hadoop-master"
# Source CSV file path
CSV_PATH = "../data/cybersecurity_threat_detection_logs.csv"
# Target HDFS base directory
HDFS_BASE = "/data/cybersecurity/logs"
# Shared directory between Host and Container (Mounted via Docker Volume)
TMP_DIR = "/tmp/partitions"
# Utilize all available CPU cores for true parallelism
MAX_WORKERS = os.cpu_count() //2

if MAX_WORKERS < 1:
    MAX_WORKERS = 1

def upload_to_hdfs(partition_key):
    """
    Handles the directory creation and file streaming to HDFS.
    Uses 'cat | docker exec -i' to bypass 'docker cp' overhead.
    """
    try:
        hdfs_path = f"{HDFS_BASE}/{partition_key}"
        local_filename = partition_key.replace("/", "_") + ".csv"
        local_file = os.path.join(TMP_DIR, local_filename)

        # 1. Create the partition directory in HDFS if it doesn't exist
        subprocess.run(
            f"docker exec {CONTAINER} hdfs dfs -mkdir -p {hdfs_path}",
            shell=True, check=True, capture_output=True
        )

        # 2. Stream local file content directly into HDFS 'put' command
        # This avoids redundant data copying steps
        upload_cmd = f"cat {local_file} | docker exec -i {CONTAINER} hdfs dfs -put -f - {hdfs_path}/logs.csv"
        subprocess.run(upload_cmd, shell=True, check=True, capture_output=True)

        return True
    except Exception as e:
        print(f"    [ERROR] Failed to process {partition_key}: {e}")
        return False

def main():
    print(f"[1/4] Starting Memory-Optimized Partitioning...")
    # Ensure the shared volume directory exists
    os.makedirs(TMP_DIR, exist_ok=True)
    partitions = defaultdict(list)
    
    # Open the large CSV and group rows by date in memory
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        for row in reader:
            try:
                # Extract year, month, and day for Hive-style partitioning
                date_part = row["timestamp"].split("T")[0]
                year, month, day = date_part.split("-")
                key = f"year={year}/month={month}/day={day}"
                partitions[key].append(row)
            except KeyError:
                # Skip rows with invalid or missing timestamp
                continue

    print(f"    Detected {len(partitions)} unique partitions. Writing local CSVs...")

    # Write each partition to a separate CSV file in the shared TMP_DIR
    for key, rows in partitions.items():
        local_file = os.path.join(TMP_DIR, key.replace("/", "_") + ".csv")
        with open(local_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)

    print(f"[2/4] Executing Parallel Upload using {MAX_WORKERS} processes...")
    
    # Use Multiprocessing to handle HDFS uploads across all CPU cores
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(upload_to_hdfs, partitions.keys()))

    print(f"[3/4] Cleanup Phase (Optional)...")
    # You can uncomment the line below to delete temporary files after upload
    # for f in os.listdir(TMP_DIR): os.remove(os.path.join(TMP_DIR, f))

    success_count = sum(results)
    print(f"\n✅ Processing Finished!")
    print(f"   Successfully uploaded: {success_count}/{len(results)} partitions.")

if __name__ == "__main__":
    main()
