from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum, desc, to_timestamp, to_date, 
    current_timestamp, lit, regexp_extract, concat_ws, date_trunc, first, max as spark_max
)
from pyspark.sql.types import *
import subprocess
import sys

# -----------------------------------------------------------------------------
# STEP 0: Dependencies Installation
# -----------------------------------------------------------------------------
# Installing happybase on all executors to ensure Thrift connection works
subprocess.check_call([sys.executable, "-m", "pip", "install", "happybase", "-q"])
import happybase

# HBase Thrift Server configuration
HBASE_HOST = "hbase"

# -----------------------------------------------------------------------------
# STEP 1: Optimized Multi-threaded Write Function
# -----------------------------------------------------------------------------
def write_partition_to_hbase(partition_iterator, table_name, cf, col_map, batch_size=1000):
    """
    Function executed in parallel on Spark executors.
    Each executor opens its own connection and writes its partition data.
    """
    import happybase # Required import inside the worker context
    
    try:
        # Each partition worker opens one connection to the Thrift server
        connection = happybase.Connection(HBASE_HOST, port=9090)
        table = connection.table(table_name)
        
        # Using batch mode for better network performance
        with table.batch(batch_size=batch_size) as batch:
            cf_bytes = cf.encode()
            for row in partition_iterator:
                row_key = str(row["row_key"]).encode()
                data = {}
                
                # Mapping Spark columns to HBase qualifiers
                for spark_col, hbase_qual in col_map.items():
                    val = row[spark_col]
                    if val is not None:
                        data[cf_bytes + b":" + hbase_qual.encode()] = str(val).encode()
                
                if data:
                    batch.put(row_key, data)
        
        connection.close()
    except Exception as e:
        print(f"[ERROR] Worker failed writing to {table_name}: {e}")

# -----------------------------------------------------------------------------
# STEP 2: Spark Session Initialization
# -----------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SOC_Batch_HBase_Parallel") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
def install_happybase(path):
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "happybase"])
spark.sparkContext.parallelize(range(100), 10).foreachPartition(lambda x: install_happybase(x))

spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------------------------------------------------
# STEP 3: Paths & Watermarking
# -----------------------------------------------------------------------------
UNIFIED_INPUT  = "hdfs://hadoop-master:9000/data/cybersecurity/logs/"
WATERMARK_PATH = "hdfs://hadoop-master:9000/data/cybersecurity/batch/_last_run_watermark"

def read_watermark(spark, path):
    try:
        return spark.read.text(path).collect()[0][0]
    except:
        return None

def write_watermark(spark, path, value):
    spark.createDataFrame([(value,)], ["value"]).coalesce(1).write.mode("overwrite").text(path)

# -----------------------------------------------------------------------------
# STEP 4: Data Loading & Preprocessing
# -----------------------------------------------------------------------------
print(f"[INFO] Reading logs from: {UNIFIED_INPUT}")
logs_raw = spark.read.option("header", "true").csv(UNIFIED_INPUT)

# Create partition_date from timestamp for watermark filtering
logs_raw = logs_raw.withColumn("partition_date", to_date(col("timestamp")))

# Filtering only new data based on watermark
last_partition = read_watermark(spark, WATERMARK_PATH)
if last_partition:
    logs = logs_raw.filter(col("partition_date") > to_date(lit(last_partition), "yyyy-MM-dd"))
else:
    logs = logs_raw

if logs.storageLevel == "NONE" and logs.count() == 0:
    print("[INFO] No new data to process.")
    spark.stop()
    sys.exit(0)

# Enrich data and extract destination port
logs = logs.withColumn("timestamp", to_timestamp(col("timestamp"))) \
           .withColumn("detection_date", to_date(col("timestamp"))) \
           .withColumn("dest_port", regexp_extract(col("dest_ip"), ":(\\d+)", 1))
logs.cache()

# -----------------------------------------------------------------------------
# STEP 5: Security Analysis & Threat Detection
# -----------------------------------------------------------------------------
patterns = {
    "SQL_Injection": r"(?i)(' OR 1=1|UNION SELECT|sqlmap|SELECT.*FROM|--)",
    "XSS": r"(?i)(<script>|alert\(|onerror)",
    "Command_Injection": r"(?i)(bin/sh|bin/bash|powershell|curl.*\|)"
}

df_sec = logs
for name, pattern in patterns.items():
    df_sec = df_sec.withColumn(name, when(col("request_path").rlike(pattern), 1).otherwise(0))

df_sec = df_sec.withColumn("attack_type", 
    when(col("SQL_Injection") == 1, "SQL_Injection")
    .when(col("XSS") == 1, "XSS")
    .when(col("Command_Injection") == 1, "Command_Injection")
    .otherwise("Normal")
).withColumn("batch_processed_at", current_timestamp())

# 1. IP Reputation Summary
ip_reputation = logs.groupBy("source_ip").agg(
    count("*").alias("total_requests"),
    count(when(col("threat_label") != "benign", True)).alias("threat_count")
).withColumn("threat_score", (col("threat_count") / col("total_requests"))) \
 .withColumn("risk_level", when(col("threat_score") > 0.7, "HIGH").otherwise("LOW")) \
 .withColumn("row_key", col("source_ip"))

# 2. Attack Patterns Summary
attack_patterns_df = df_sec.filter(col("attack_type") != "Normal") \
    .groupBy("attack_type", "detection_date") \
    .agg(count("*").alias("attack_count"), spark_max("batch_processed_at").alias("last_seen")) \
    .withColumn("row_key", concat_ws("#", col("attack_type"), col("detection_date").cast("string")))

# 3. Threat Timeline Summary
threat_timeline = logs.withColumn("hour", date_trunc("hour", col("timestamp"))) \
    .groupBy("hour", "threat_label") \
    .agg(count("*").alias("event_count"), sum("bytes_transferred").alias("total_bytes")) \
    .withColumn("row_key", concat_ws("#", col("threat_label"), col("hour").cast("string")))

# -----------------------------------------------------------------------------
# STEP 6: Multi-threaded Parallel Write to HBase
# -----------------------------------------------------------------------------
print("[HBASE] Starting parallel write via foreachPartition...")

# Writing IP Reputation table
ip_reputation.foreachPartition(lambda it: write_partition_to_hbase(
    it, "ip_reputation", "info", {
        "total_requests": "total_requests", "threat_count": "threat_count",
        "threat_score": "threat_score", "risk_level": "risk_level"
    }
))

# Writing Attack Patterns table
attack_patterns_df.foreachPartition(lambda it: write_partition_to_hbase(
    it, "attack_patterns", "stats", {
        "attack_count": "attack_count", "last_seen": "last_seen"
    }
))

# Writing Threat Timeline table
threat_timeline.foreachPartition(lambda it: write_partition_to_hbase(
    it, "threat_timeline", "data", {
        "event_count": "event_count", "total_bytes": "total_bytes"
    }
))

# -----------------------------------------------------------------------------
# STEP 7: Watermark Update & Cleanup
# -----------------------------------------------------------------------------
max_date = logs.agg({"partition_date": "max"}).collect()[0][0]
write_watermark(spark, WATERMARK_PATH, str(max_date))

logs.unpersist()
print("✅ Batch Processing to HBase completed successfully using Multi-threading.")
spark.stop()
