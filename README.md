# 🚨 Real-Time Cybersecurity Threat Detection (Lambda Architecture)

## 📌 Project Overview

This project implements a **real-time cybersecurity threat detection system** using a **Lambda Architecture** approach.

The system analyzes network logs to:

* Detect **historical attack patterns** (Batch Layer)
* Identify **real-time threats** (Speed Layer)
* Provide **visual insights via a dashboard**

---

## 🧠 Architecture

The system follows the **Lambda Architecture**:

```
                +-------------------+
                |   Data Source     |
                | (CSV / Producer)  |
                +---------+---------+
                          |
             +------------+------------+
             |                         |
      +------+-------+         +-------+------+
      |  Batch Layer |         | Speed Layer  |
      | (Hadoop +    |         | (Kafka +     |
      |  Spark)      |         |  Streaming)  |
      +------+-------+         +-------+------+
             |                         |
             +------------+------------+
                          |
                  +-------+-------+
                  | Serving Layer |
                  | (API + UI)    |
                  +---------------+
```

---

## ⚙️ Technologies Used

* **Hadoop (HDFS)** → Distributed storage
* **Apache Spark** → Batch processing
* **Apache Kafka** → Streaming pipeline *(Sprint 2)*
* **Spark Streaming** → Real-time detection *(Sprint 2)*
* **Docker & Docker Compose** → Environment setup
* **Python** → Data processing & scripts
* **Cassandra / HBase** → Storage *(Planned)*
* **Grafana / Web UI** → Dashboard *(Sprint 3)*

---

## 🚀 Sprint 1 — Batch Layer (Completed Phase)

### 🎯 Objective

Build a **stable batch processing system** capable of analyzing historical cybersecurity logs.

---

## 🧱 What Was Built

### 1️⃣ Distributed Environment (Docker)

* Hadoop cluster (HDFS)
* Spark cluster
* All services orchestrated using Docker Compose

---

### 2️⃣ Data Storage (HDFS)

* Dataset stored in:

```
/data/logs/logs.csv
```

* HDFS used as the **main storage layer** for batch processing

---

### 3️⃣ Batch Processing (Spark)

Implemented analysis on historical data:

* 🔝 Top malicious IPs
* 📊 Data volume by threat type
* 🔍 Basic threat filtering (malicious / suspicious)

---

### 4️⃣ Report Phase A - Latex

start writing the report : 
  -Front page 
  -Introduction 
  - Phase one documentation
    
---

## 🔄 Team Workflow (How We Collaborate)

Each team member works **locally but with the same environment**:

```
Person 1 → builds environment → pushes code
Person 2 → pulls → runs environment
Person 3 → pulls → runs environment
Person 4 → pulls → runs environment
```

👉 We share **configuration, not running systems**

---

## 🧩 Data Consistency Strategy

Since HDFS is local to each Docker instance, data is **recreated automatically**.

We implemented a shared setup script:

### 📄 `setup.sh`

```bash
#!/bin/bash

echo "Starting containers..."
docker-compose up -d

echo "Waiting for Hadoop..."
sleep 10

echo "Creating HDFS directories..."
docker exec -it hadoop-master hdfs dfs -mkdir -p /data/logs

echo "Uploading dataset..."
docker exec -it hadoop-master hdfs dfs -put -f /data/logs.csv /data/logs/

echo "Done!"
```

### ✅ Purpose

* Ensures dataset is always available
* Avoids data loss between teammates
* Standardizes environment setup

---

## 📂 Project Structure

```
project/
├── docker/
│   └── docker-compose.yml
├── batch/
│   └── batch_job.py
├── streaming/        # Sprint 2
├── api/              # Sprint 3
├── dashboard/        # Sprint 3
├── data/
│   └── logs.csv
└── report/
```

---

## ▶️ How to Run (Sprint 1)

### 1. Clone the repository

```
git clone <repo-url>
cd project
```

---

### 2. Run setup

```
bash setup.sh
```

---

### 3. Run batch processing

```
spark-submit batch/batch_job.py
```

---

## ✅ Sprint 1 Deliverables

* ✔️ Fully working Docker environment
* ✔️ Dataset stored in HDFS
* ✔️ Spark batch processing implemented
* ✔️ At least 2 analyses completed
* ✔️ Advanced detection implemented
* ✔️ Reproducible setup (`setup.sh`)
* ✔️ Initial report with screenshots

---

## ⚡ Sprint 2 — Streaming Layer (Next Phase)

* Setup Kafka
* Create Producer (CSV → Kafka)
* Implement Spark Streaming:

  * Brute-force detection
  * Attack signatures

---

## 🌐 Sprint 3 — Integration & Deployment

* Build API (Flask)
* Create dashboard
* Deploy using Docker (local or cloud)
* Final testing & demo

---

## 🔍 Detection Examples

* 🚨 Brute-force attack
  → 5+ failed logins in 1 minute

* 💉 SQL Injection
  → Patterns like `' OR 1=1--`

* 📊 Data anomaly
  → High data transfer spikes

---

## 🛡️ Risk Management

* Use GitHub with frequent commits
* Work with feature branches
* Recreate environment using `setup.sh`
* Test components independently
* Integrate early (avoid last-minute issues)
* Keep dataset and configs backed up

---

## 📊 Expected Results

* Detection of multiple attack types
* Clear batch analysis results
* Scalable architecture ready for real-time extension
* Modular and maintainable code

---

## 📎 Dataset

Cybersecurity logs dataset (Kaggle):
https://www.kaggle.com/datasets/aryan208/cybersecurity-threat-detection-logs

---

## 📎 Bonus mansawhch (ML or Detecting Chain
)

