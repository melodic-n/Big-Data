#!/usr/bin/env python3
# =============================================================================
# Spark Streaming — Cybersecurity SOC  (Dual Sink: Cassandra + InfluxDB)
# =============================================================================

import os
import requests
import logging
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ===================== LOGGING =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("SOC-Speed-Layer")

# ===================== CONFIG =====================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.20.0.10:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",      "cybersecurity-logs")

CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",   "172.20.0.20")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KS    = "cybersecurity"

CHECKPOINT      = "/tmp/spark_checkpoints/speed_layer"

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://172.20.0.30:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "super-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "cyber-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "speed-bucket")

BRUTE_FORCE_THRESHOLD = 5
DATA_EXFIL_THRESHOLD  = 10_000_000

# ===================== SIGNATURES (OPTIMIZED) =====================
SIGNATURES = {
    "sqli": re.compile(r"(?i)(union\s+select|or\s+1=1|drop\s+table|insert\s+into|select\s+\*|--|#|xp_cmdshell|benchmark\(|sleep\()"),
    "xss": re.compile(r"(?i)(<script|alert\(|onerror=|onload=|javascript:|<img[^>]+src=|document\.cookie|eval\()"),
    "path_traversal": re.compile(r"(?i)(\.\./|\.\.\\|%2e%2e%2f|etc/passwd|etc/shadow|/proc/self)"),
    "rce": re.compile(r"(?i)(cmd=|exec\(|system\(|passthru\(|shell_exec\(|popen\(|proc_open\(|wget\s|curl\s|bash\s+-i|/bin/sh)"),
    "lfi": re.compile(r"(?i)(include\s*\(|require\s*\(|file=|/etc/|/var/log|/proc/)"),
    "scanner_tool": re.compile(r"(?i)(sqlmap|nikto|nmap|masscan|dirbuster|gobuster|wfuzz|hydra|metasploit|nessus)"),
    "xxe": re.compile(r"(?i)(<!entity|<!doctype|system\s+['\"]|/etc/passwd|file://)"),
    "ssrf": re.compile(r"(?i)(localhost|127\.0\.0\.1|169\.254\.|metadata\.google|169\.254\.169\.254)"),
}

# ===================== SPARK =====================
def spark_session():
    return (
        SparkSession.builder
        .appName("SOC-Speed-Layer")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )

# ===================== SCHEMA =====================
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("source_ip", StringType()),
    StructField("dest_ip", StringType()),
    StructField("protocol", StringType()),
    StructField("action", StringType()),
    StructField("threat_label", StringType()),
    StructField("log_type", StringType()),
    StructField("bytes_transferred", StringType()),
    StructField("user_agent", StringType()),
    StructField("request_path", StringType()),
])

# ===================== STREAM =====================
def read_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("bytes_xfer", F.col("bytes_transferred").cast("long"))
        .drop("timestamp", "bytes_transferred")
        .filter(F.col("event_time").isNotNull())
    )

# ===================== INFLUX =====================
def _write_to_influx(lines):
    if not lines:
        return

    url = f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns"
    headers = {"Authorization": f"Token {INFLUX_TOKEN}"}
    body = "\n".join(lines)

    try:
        r = requests.post(url, data=body, headers=headers, timeout=30)
        if r.status_code != 204:
            logger.error(f"InfluxDB error: {r.text}")
    except Exception as e:
        logger.error(f"InfluxDB exception: {e}")

# ===================== WRITERS =====================
def influx_writer(tag):
    def inner(batch_df, batch_id):
        logger.info(f"[InfluxDB] Writing {tag} batch {batch_id}")
    return inner

# ===================== DETECTIONS =====================
def brute_force(df):
    return (
        df.withWatermark("event_time", "2 minutes")
        .filter(
            (F.col("action") == "blocked") |
            (F.lower(F.col("request_path")).contains("login"))
        )
        .groupBy(F.window("event_time", "1 minute"), "source_ip")
        .count()
        .filter(F.col("count") >= BRUTE_FORCE_THRESHOLD)
        .select(
            "source_ip",
            F.col("window.start").alias("window_start"),
            F.col("count").alias("failed_count")
        )
    )

def signatures(df):
    all_regex = "|".join([r.pattern for r in SIGNATURES.values()])

    return (
        df.withColumn("event_time", F.col("event_time"))
        .filter(
            F.col("request_path").rlike(all_regex) |
            F.col("user_agent").rlike(all_regex)
        )
        .withColumn("attack_type", F.lit("detected"))
        .withColumn("threat_score", F.lit(0.95))
        .select("source_ip", "event_time", "request_path", "user_agent", "attack_type", "threat_score")
    )

def exfiltration(df):
    return (
        df.withWatermark("event_time", "1 minutes")
        .groupBy(F.window("event_time", "10 seconds"), "source_ip")
        .agg(F.sum("bytes_xfer").alias("total_bytes"))
        .filter(F.col("total_bytes") > DATA_EXFIL_THRESHOLD)
        .select(
            "source_ip",
            F.col("window.start").alias("window_start"),
            "total_bytes"
        )
    )

def ip_scores(df):
    return (
        df.groupBy("source_ip")
        .agg(
            F.count("*").alias("attack_count"),
            F.max("event_time").alias("last_seen")
        )
    )

# ===================== MAIN =====================
def main():
    spark = spark_session()
    df = read_stream(spark)

    pipelines = [
        brute_force(df),
        signatures(df),
        exfiltration(df),
        ip_scores(df)
    ]

    for p in pipelines:
        p.writeStream \
         .format("console") \
         .outputMode("append") \
         .start()

    logger.info("SOC Speed Layer running...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
