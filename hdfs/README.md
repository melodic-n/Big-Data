# HDFS Data Ingestion — Cybersecurity Threat Detection

**Task owner:** HDFS + Data Upload  
**Project:** Mini-Project Big Data SITCN2 — Lambda Architecture

---

## What this does

This task handles the **data ingestion layer** of the Lambda architecture:

1. Downloads the cybersecurity dataset (Kaggle)
2. Creates a **partitioned HDFS directory structure** (`/data/cybersecurity/logs/year=.../month=.../day=.../`)
3. Uploads the CSV into HDFS (raw zone + partitioned zone)
4. Prepares the `/data/cybersecurity/batch/` output directories for the Spark team

---

## Prerequisites

- Docker installed and running
- `docker-compose.yml` from the cluster task already launched:
  ```bash
  docker-compose up -d
  ```
- Dataset downloaded from Kaggle:  
  [cybersecurity-threat-detection-logs](https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs)

---

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/melodic-n/Big-Data.git
cd Big-Data

# 2. Start the cluster (if not already running)
docker-compose up -d

# 3. Place the dataset OR the script will ask you for the path
#    Default search paths: ./data/, ~/Téléchargements/, ~/Downloads/
cp /path/to/cybersecurity_threat_detection_logs.csv ./data/

# 4. Run the HDFS setup
chmod +x setup.sh
./setup.sh

# 5. Verify the result
chmod +x verify_hdfs.sh
./verify_hdfs.sh
```

---

## HDFS Structure Created

```
/data/cybersecurity/
├── raw/
│   └── cybersecurity_threat_detection_logs.csv   ← full dataset
├── logs/
│   ├── year=2023/month=10/day=15/
│   │   └── cybersecurity_threat_detection_logs.csv
│   ├── year=2023/month=10/day=16/   (ready for partitioned data)
│   ├── year=2023/month=10/day=17/
│   ├── year=2023/month=11/day=01/
│   └── year=2023/month=11/day=02/
└── batch/
    ├── port_scans/         ← output for Spark batch (port scan detection)
    ├── attack_patterns/    ← output for Spark batch (SQLi/XSS patterns)
    └── threat_timeline/    ← output for Spark batch (temporal analysis)
```

---

## How teammates use this data (Spark Batch Layer)

```java
// Read raw CSV
Dataset<Row> logs = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://hadoop-master:9000/data/cybersecurity/raw/cybersecurity_threat_detection_logs.csv");

// Or read partitioned structure
Dataset<Row> logs = spark.read()
    .option("header", "true")
    .csv("hdfs://hadoop-master:9000/data/cybersecurity/logs/*/*/*");

// Write results back (example)
logs.write().parquet("hdfs://hadoop-master:9000/data/cybersecurity/batch/port_scans");
```

---

## Deliverables

- [x] `setup.sh` — automated HDFS ingestion script
- [x] `verify_hdfs.sh` — verification + screenshot helper
- [ ] Screenshot of `hdfs dfs -ls -R /data/cybersecurity/` (run `verify_hdfs.sh`)
- [ ] Screenshot of HDFS Web UI at `http://localhost:9870`

---

## Dataset Fields Reference

| Field | Description | Example |
|---|---|---|
| timestamp | Event date/time | 2023-10-15 14:23:45 |
| source_ip | Source IP | 192.168.1.45 |
| dest_ip | Destination IP | 10.0.0.100 |
| protocol | Protocol | HTTP, TCP, SSH |
| action | Action taken | allowed, blocked |
| threat_label | Threat class | benign, suspicious, malicious |
| log_type | Log source | firewall, ids, application |
| bytes_transferred | Data volume (bytes) | 1542 |
| user_agent | Client agent | Mozilla/5.0, sqlmap/1.7 |
| request_path | Accessed path | /admin/login.php |
