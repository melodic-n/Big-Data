# ⚡ SOC Batch Processing Layer

This layer is responsible for the large-scale analysis of cybersecurity logs stored in HDFS. It performs threat detection, IP reputation scoring, and timeline aggregation, then stores the results in **Apache HBase**.

## 🚀 Key Features

* **Parallel Writing:** Optimized using PySpark's `foreachPartition` to write data directly from executors to HBase Thrift, avoiding Driver bottlenecks.
* **Incremental Processing:** Implements a **Watermarking** mechanism to track the last processed date and only analyze new log partitions.
* **Multi-threaded Ingestion:** Uses a custom parallel writer with `happybase` to maximize write throughput to HBase.
* **Automated Orchestration:** Includes a `setup.sh` script that manages service health (YARN, Thrift) and table creation.

## 🛠️ Tech Stack

* **Engine:** Apache Spark (PySpark)
* **Storage:** Hadoop HDFS (Input) & Apache HBase (Output)
* **Communication:** HBase Thrift Server & Happybase
* **Resource Management:** Apache Hadoop YARN

## 📂 Structure

```text
batch-layer/
├── batch_hbase.py      # The Spark processing logic (Analysis + Parallel Write)
├── setup.sh    # Orchestrator: Checks YARN/HBase, Setup Tables, Submits Job
