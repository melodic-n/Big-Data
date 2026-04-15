#!/usr/bin/env python3
"""
=============================================================================
Spark_streaming.py  —  Speed Layer (PROD READY)
Cybersecurity Threat Detection  |  SITCN2 Big Data Mini-Project
=============================================================================
"""

import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, FloatType
)

# ============================================================================
# CONFIGURATION DYNAMIQUE (Docker DNS)
# ============================================================================
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "172.20.0.8:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC",     "cybersecurity-logs")
CASSANDRA_HOST  = os.environ.get("CASSANDRA_HOST",  "172.20.0.5")
CASSANDRA_PORT  = int(os.environ.get("CASSANDRA_PORT", "9042"))
CASSANDRA_KS    = "cybersecurity"

BRUTE_FORCE_THRESHOLD = 5
DATA_EXFIL_THRESHOLD  = 10_000_000  # 10 MB

ATTACK_SIGNATURES = [
    "sqlmap", "nikto", "OR 1=1", "UNION SELECT", "DROP TABLE", 
    "\\.\\./", "cmd=", "eval\\(", "alert\\(", "onload=", "etc/passwd"
]
# ============================================================================
# SPARK SESSION
# ============================================================================
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CyberSecurity-SpeedLayer")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
        # تخفيف الضغط على الـ RAM
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

# ============================================================================
# SCHEMA D'ENTRÉE (JSON Kafka)
# ============================================================================
LOG_SCHEMA = StructType([
    StructField("timestamp",         StringType(), True),
    StructField("source_ip",         StringType(), True),
    StructField("dest_ip",           StringType(), True),
    StructField("protocol",          StringType(), True),
    StructField("action",            StringType(), True),
    StructField("threat_label",      StringType(), True),
    StructField("log_type",          StringType(), True),
    StructField("bytes_transferred", StringType(), True), # Read as String then cast
    StructField("user_agent",        StringType(), True),
    StructField("request_path",      StringType(), True),
])

# ============================================================================
# CASSANDRA WRITER (Micro-batch) - FIXED: Removed isEmpty()
# ============================================================================
def cassandra_stream_writer(table: str):
    def _write(batch_df, batch_id):
        # ممتنوعش تستعمل isEmpty() هنا حيت كتدير Crash لـ Py4J
        try:
            (batch_df.write
             .format("org.apache.spark.sql.cassandra")
             .mode("append")
             .options(table=table, keyspace=CASSANDRA_KS)
             .save())
            # الطباعة فقط إلا كانت الداتا كاينة (اختياري)
            # print(f"[Batch {batch_id}] Done -> {table}")
        except Exception as exc:
            print(f"[Batch {batch_id}] ERR {table}: {exc}", file=sys.stderr)
    return _write

# ============================================================================
# LECTURE KAFKA
# ============================================================================
def read_kafka_stream(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest") 
        .option("failOnDataLoss", "false")
        .load()
    )
    
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", LOG_SCHEMA).alias("d"))
        .select("d.*")
        # CORRECTION: Format 'T' pour ISO Date (2024-07-18T00:00:00)
        .withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("bytes_xfer", F.col("bytes_transferred").cast(LongType()))
        .drop("timestamp", "bytes_transferred")
    )
    return parsed

# ============================================================================
# DÉTECTIONS 
# ============================================================================

def detect_brute_force(parsed_df):
    failed = parsed_df.filter(
        (F.col("action") == "blocked") | 
        (F.lower(F.col("request_path")).contains("login"))
    )
    windowed = (
        failed
        .groupBy(F.window("event_time", "1 minute").alias("win"), F.col("source_ip"))
        .agg(F.count("*").alias("failed_count"))
        .filter(F.col("failed_count") >= BRUTE_FORCE_THRESHOLD)
    )
    return (
        windowed
        .withColumn("window_start", F.col("win.start"))
        .withColumn("threat_score", F.least(F.lit(1.0), (F.col("failed_count") / 20.0).cast(FloatType())))
        .withColumn("detected_at", F.current_timestamp())
        .select("source_ip", "window_start", "failed_count", "threat_score", "detected_at")
    )

def detect_signatures(parsed_df):
    sig_regex = "|".join(ATTACK_SIGNATURES)
    flagged = parsed_df.filter(
        F.col("request_path").rlike(sig_regex) | F.col("user_agent").rlike(sig_regex)
    )
    return (
        flagged
        .withColumn("attack_type",
            F.when(F.col("user_agent").rlike("sqlmap|nikto"), F.lit("scanner_tool"))
             .when(F.col("request_path").rlike("UNION|SELECT|DROP|OR 1=1"), F.lit("sqli"))
             .when(F.col("request_path").rlike("<script|alert|onload"), F.lit("xss"))
             .otherwise(F.lit("signature_match"))
        )
        .withColumn("threat_score", F.lit(0.9).cast(FloatType()))
        .withColumn("window_start", F.current_timestamp()) # Simplified for speed
        .withColumn("window_end", F.current_timestamp())
        .select("source_ip", "event_time", "dest_ip", "protocol", "action", "threat_label", 
                "log_type", "bytes_xfer", "user_agent", "request_path", "attack_type", 
                "threat_score", "window_start", "window_end")
    )

def detect_data_exfil(parsed_df):
    windowed = (
        parsed_df
        .groupBy(F.window("event_time", "10 seconds").alias("win"), F.col("source_ip"))
        .agg(F.sum("bytes_xfer").alias("total_bytes"))
        .filter(F.col("total_bytes") > DATA_EXFIL_THRESHOLD)
    )
    return (
        windowed
        .withColumn("window_start", F.col("win.start"))
        .withColumn("threat_score", F.least(F.lit(1.0), (F.col("total_bytes") / 100_000_000.0).cast(FloatType())))
        .withColumn("detected_at", F.current_timestamp())
        .select("source_ip", "window_start", "total_bytes", "threat_score", "detected_at")
    )

def compute_ip_scores(parsed_df):
    scored = (
        parsed_df
        .withColumn("weight", F.when(F.col("threat_label") == "malicious", 1.0).when(F.col("threat_label") == "suspicious", 0.5).otherwise(0.1))
        .groupBy(F.window("event_time", "5 minutes").alias("win"), F.col("source_ip"))
        .agg(F.avg("weight").alias("threat_score"), F.count("*").alias("attack_count"), F.max("event_time").alias("last_seen"))
    )
    return (
        scored
        .withColumn("threat_score", F.col("threat_score").cast(FloatType()))
        .withColumn("attack_count", F.col("attack_count").cast("int"))
        .withColumn("is_blocked", F.when(F.col("threat_score") > 0.7, True).otherwise(False))
        .withColumn("last_seen", F.col("last_seen"))
        .select("source_ip", "threat_score", "attack_count", "last_seen", "is_blocked")
    )

# ============================================================================
# MAIN
# ============================================================================
def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR") # Clean logs

    parsed = read_kafka_stream(spark)
    
    # Exécution des pipelines
    queries = []
    
    mapping = {
        "brute_force_alerts": detect_brute_force(parsed),
        "realtime_threats": detect_signatures(parsed),
        "data_exfil_alerts": detect_data_exfil(parsed),
        "ip_threat_scores": compute_ip_scores(parsed)
    }

    # IMPORTANT: Nettoyer le checkpoint avant de lancer
    CP = "/tmp/spark_checkpoint_cyber"

    for table, df in mapping.items():
        q = (df.writeStream
             .outputMode("update") 
             .foreachBatch(cassandra_stream_writer(table))
             .option("checkpointLocation", f"{CP}/{table}")
             .start())
        queries.append(q)

    print(f"\n[OK] Pipeline Speed Layer démarré. Analyse en temps réel sur {KAFKA_BOOTSTRAP}...\n")
    
    try:
        spark.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Streaming Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
