#!/usr/bin/env python3
import subprocess
import csv
import os
from collections import defaultdict

CONTAINER = "hadoop-master"
CSV_PATH = "../data/cybersecurity_threat_detection_logs.csv"
HDFS_BASE = "/data/cybersecurity/logs"
TMP_DIR = "/tmp/partitions"

print("[1/4] Lecture du dataset...")
os.makedirs(TMP_DIR, exist_ok=True)

partitions = defaultdict(list)
header = None

with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames
    for row in reader:
        try:
            # format: 2024-05-01T00:00:00
            ts = row["timestamp"]
            date_part = ts.split("T")[0]        # "2024-05-01"
            year, month, day = date_part.split("-")
            key = f"year={year}/month={month}/day={day}"
            partitions[key].append(row)
        except Exception:
            continue

print(f"    {len(partitions)} jours detectes dans le dataset")

print("[2/4] Creation des fichiers CSV par jour...")
for partition_key, rows in partitions.items():
    local_file = os.path.join(TMP_DIR, partition_key.replace("/", "_") + ".csv")
    with open(local_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)
    print(f"    {partition_key} -> {len(rows)} lignes")

print("[3/4] Upload dans HDFS...")
for partition_key in partitions.keys():
    hdfs_path = f"{HDFS_BASE}/{partition_key}"
    local_file = os.path.join(TMP_DIR, partition_key.replace("/", "_") + ".csv")
    container_tmp = f"/tmp/{partition_key.replace('/', '_')}.csv"

    # Creer dossier HDFS
    subprocess.run(["docker", "exec", CONTAINER,
                    "hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)

    # Copier fichier dans le container
    subprocess.run(["docker", "cp", local_file,
                    f"{CONTAINER}:{container_tmp}"], check=True)

    # Upload dans HDFS
    subprocess.run(["docker", "exec", CONTAINER,
                    "hdfs", "dfs", "-put", "-f", container_tmp,
                    f"{hdfs_path}/logs.csv"], check=True)

    print(f"    OK -> {hdfs_path}/logs.csv")

print("[4/4] Verification...")
subprocess.run(["docker", "exec", CONTAINER,
                "hdfs", "dfs", "-ls", "-R", "/data/cybersecurity/logs/"])

print("\nPartitionnement termine avec succes!")
