#!/usr/bin/env python3
import subprocess
import csv
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
CONTAINER = "hadoop-master"
CSV_PATH = "../data/cybersecurity_threat_detection_logs.csv"
HDFS_BASE = "/data/cybersecurity/logs"
TMP_DIR = "/tmp/partitions"
MAX_THREADS = 4  # Adjust based on your CPU/Docker limits

def process_partition(partition_key):
    """Function to handle HDFS upload for a single partition (Thread Task)"""
    try:
        hdfs_path = f"{HDFS_BASE}/{partition_key}"
        local_filename = partition_key.replace("/", "_") + ".csv"
        local_file = os.path.join(TMP_DIR, local_filename)
        container_tmp = f"/tmp/{local_filename}"

        # 1. Create HDFS directory
        subprocess.run(["docker", "exec", CONTAINER,
                        "hdfs", "dfs", "-mkdir", "-p", hdfs_path], 
                        check=True, capture_output=True)

        # 2. Copy local CSV to Docker container
        subprocess.run(["docker", "cp", local_file,
                        f"{CONTAINER}:{container_tmp}"], 
                        check=True, capture_output=True)

        # 3. Put file into HDFS
        subprocess.run(["docker", "exec", CONTAINER,
                        "hdfs", "dfs", "-put", "-f", container_tmp,
                        f"{hdfs_path}/logs.csv"], 
                        check=True, capture_output=True)

        print(f"    [OK] Thread finished: {hdfs_path}/logs.csv")
        return True
    except Exception as e:
        print(f"    [ERROR] Failed to upload {partition_key}: {e}")
        return False

# --- Main Execution ---
print("[1/4] Reading dataset and partitioning in memory...")
os.makedirs(TMP_DIR, exist_ok=True)
partitions = defaultdict(list)
header = None

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames
    for row in reader:
        try:
            ts = row["timestamp"]
            date_part = ts.split("T")[0]
            year, month, day = date_part.split("-")
            key = f"year={year}/month={month}/day={day}"
            partitions[key].append(row)
        except Exception:
            continue

print(f"    {len(partitions)} distinct days detected.")

print("[2/4] Generating local partitioned CSV files...")
for partition_key, rows in partitions.items():
    local_file = os.path.join(TMP_DIR, partition_key.replace("/", "_") + ".csv")
    with open(local_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

print(f"[3/4] Parallel Upload to HDFS (Threads: {MAX_THREADS})...")
# Using ThreadPoolExecutor to run process_partition in parallel
with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
    results = list(executor.map(process_partition, partitions.keys()))

print("[4/4] Verification...")
subprocess.run(["docker", "exec", CONTAINER,
                "hdfs", "dfs", "-ls", "-R", "/data/cybersecurity/logs/"])

print(f"\nPartitioning and Parallel Upload completed! Successful: {sum(results)}/{len(results)}")
