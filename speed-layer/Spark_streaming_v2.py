#!/usr/bin/env python3
# =============================================================================
# Spark Streaming — Cybersecurity SOC (Cassandra + InfluxDB) - V2 CORRECTED
# =============================================================================

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ===================== CONFIGURATION =====================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.20.0.10:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",      "cybersecurity-logs")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",   "172.20.0.20")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KS    = "cybersecurity"
CHECKPOINT      = "/Guard/spark_checkpoints/speed_layer"

INFLUXDB_URL     = os.getenv("INFLUXDB_URL",     "http://influxdb:8086")
INFLUXDB_TOKEN   = os.getenv("INFLUXDB_TOKEN",   "super-token")
INFLUXDB_ORG     = os.getenv("INFLUXDB_ORG",     "cyber-org")
INFLUXDB_BUCKET  = os.getenv("INFLUXDB_BUCKET_SPEED", "speed-bucket")

BRUTE_FORCE_THRESHOLD = 5
DATA_EXFIL_THRESHOLD  = 100_000  # CORRECTED: 100KB in bytes

SIGNATURES = {
    "sqli": r"(?i)(union\s+select|or\s+1=1|drop\s+table|insert\s+into|select\s+\*|--|#|xp_cmdshell|benchmark\(|sleep\()",
    "xss": r"(?i)(<script|alert\(|onerror=|onload=|javascript:|<img[^>]+src=|document\.cookie|eval\()",
    "path_traversal": r"(?i)(\.\./|\.\.\\|%2e%2e%2f|%2e%2e/|\.\.%2f|etc/passwd|etc/shadow|/proc/self)",
    "rce": r"(?i)(cmd=|exec\(|system\(|passthru\(|shell_exec\(|popen\(|proc_open\(|wget\s|curl\s|bash\s+-i|/bin/sh|/bin/bash)",
    "lfi": r"(?i)(include\s*\(|require\s*\(|file=|page=|path=.*\.\./|/etc/|/var/log|/proc/)",
    "scanner_tool": r"(?i)(sqlmap|nikto|nmap|masscan|dirbuster|gobuster|wfuzz|hydra|metasploit|nessus)",
    "xxe": r"(?i)(<!entity|<!doctype|system\s+['\"]|PUBLIC\s+['\"]|/etc/passwd|file://)",
    "ssrf": r"(?i)(localhost|127\.0\.0\.1|169\.254\.|internal\.|metadata\.google|169\.254\.169\.254)",
}

# ===================== SPARK SESSION =====================
def spark_session():
    return (SparkSession.builder
        .appName("SOC-Speed-Layer")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate())

# ===================== SCHEMA & READER =====================
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

def read_stream(spark):
    return (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("bytes_xfer", F.col("bytes_transferred").cast("long"))
        .filter(F.col("event_time").isNotNull() & F.col("source_ip").isNotNull()))

# ===================== WRITERS =====================
def write_to_cassandra(table):
    def writer(batch_df, batch_id):
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=table, keyspace=CASSANDRA_KS) \
                .save()
        except Exception as e:
            print(f"[ERROR] Cassandra {table} (Batch {batch_id}): {e}")
    return writer

def write_to_influxdb(measurement, tag_cols, time_col):
    def writer(batch_df, batch_id):
        if batch_df.isEmpty(): return
        
        # Correction logic: exclude time_col and tags from fields
        field_cols = [c for c in batch_df.columns if c != time_col and c not in tag_cols]

        def write_partition(partition):
            from influxdb_client import InfluxDBClient, Point
            from influxdb_client.client.write_api import SYNCHRONOUS
            
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api = client.write_api(write_options=SYNCHRONOUS)
            
            try:
                points = []
                for row in partition:
                    point = Point(measurement)
                    # Tags
                    for tag in tag_cols:
                        if row[tag]: point = point.tag(tag, str(row[tag]))
                    
                    # Fields
                    for field in field_cols:
                        val = row[field]
                        if val is None: continue
                        
                        if isinstance(val, list):
                            # BUG FIX 3: Handling empty lists / joining
                            str_val = ",".join(str(v) for v in val if v is not None)
                            if str_val: point = point.field(field, str_val)
                        elif isinstance(val, bool):
                            # BUG FIX 2: Native boolean support
                            point = point.field(field, val)
                        else:
                            point = point.field(field, val)
                    
                    # Timestamp
                    point = point.time(row[time_col])
                    points.append(point)
                    
                    if len(points) >= 500:
                        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
                        points = []
                
                if points:
                    write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            except Exception as e:
                print(f"[ERROR] InfluxDB Write Error: {e}")
            finally:
                write_api.close()
                client.close()

        batch_df.foreachPartition(write_partition)
    return writer

# ===================== DETECTIONS =====================
def brute_force(df):
    return (df.withWatermark("event_time", "2 minutes")
        .filter(F.col("action") == "blocked")
        .groupBy(F.window("event_time", "1 minute"), "source_ip")
        .count()
        .filter(F.col("count") >= BRUTE_FORCE_THRESHOLD)
        .withColumn("threat_score", F.least(F.lit(1.0), F.col("count") / F.lit(20.0)).cast("float"))
        .select("source_ip", F.col("window.start").alias("window_start"), 
                F.col("count").alias("failed_count"), "threat_score"))

def signatures(df):
    all_regex = "|".join(SIGNATURES.values())
    attack_type_col = F.when(F.col("request_path").rlike(SIGNATURES["sqli"]), "sqli") \
                       .when(F.col("request_path").rlike(SIGNATURES["xss"]), "xss") \
                       .otherwise("other")
    
    return (df.filter(F.col("request_path").rlike(all_regex))
        .withColumn("attack_type", attack_type_col)
        .withColumn("threat_score", F.lit(0.95).cast("float"))
        .select("source_ip", "event_time", "attack_type", "threat_score", "request_path"))

def exfiltration(df):
    return (df.withWatermark("event_time", "1 minute")
        .groupBy(F.window("event_time", "10 seconds"), "source_ip")
        .agg(F.sum("bytes_xfer").alias("total_bytes"))
        .filter(F.col("total_bytes") > DATA_EXFIL_THRESHOLD)
        .withColumn("threat_score", F.lit(1.0).cast("float"))
        .select("source_ip", F.col("window.start").alias("window_start"), "total_bytes", "threat_score"))

def ip_scores(df):
    # Logique simplifiée pour l'agrégation par IP
    return (df.withWatermark("event_time", "10 minutes")
        .groupBy(F.window("event_time", "5 minutes"), "source_ip")
        .agg(F.count("*").alias("attack_count"),
             F.max("event_time").alias("last_seen"),
             F.collect_set(F.lit("general_threat")).alias("attack_types")) # Exemple simplifié
        .withColumn("threat_score", F.lit(0.8).cast("float"))
        .withColumn("is_blocked", F.lit(True))
        .select("source_ip", "threat_score", "attack_count", "last_seen", "is_blocked", "attack_types"))

# ===================== MAIN =====================
def main():
    spark = spark_session()
    df = read_stream(spark)

    # Configuration des pipelines : (Nom, DF, Tags Influx, TimeCol Influx)
    pipelines = [
        ("brute_force_alerts", brute_force(df), ["source_ip"], "window_start"),
        ("realtime_threats",   signatures(df),  ["source_ip", "attack_type"], "event_time"),
        ("data_exfil_alerts",  exfiltration(df),["source_ip"], "window_start"),
        ("ip_threat_scores",   ip_scores(df),   ["source_ip"], "last_seen")
    ]

    active_streams = []
    for table, stream_df, tags, t_col in pipelines:
        def make_writer(t, tg, tc):
            c_writer = write_to_cassandra(t)
            i_writer = write_to_influxdb(t, tg, tc)
            return lambda b_df, b_id: [c_writer(b_df, b_id), i_writer(b_df, b_id)]

        q = (stream_df.writeStream
            .foreachBatch(make_writer(table, tags, t_col))
            .option("checkpointLocation", f"{CHECKPOINT}/{table}")
            .trigger(processingTime="10 seconds")
            .start())
        active_streams.append(q)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()