#!/usr/bin/env python3
# =============================================================================
# Spark Streaming — Cybersecurity SOC  (Dual Sink: Cassandra + InfluxDB)
# =============================================================================

import os
import requests                           # <-- added for InfluxDB HTTP writes
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ===================== CONFIG =====================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "172.20.0.10:29092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",      "cybersecurity-logs")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST",   "172.20.0.20")
CASSANDRA_PORT  = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KS    = "cybersecurity"
CHECKPOINT      = "/Guard/spark_checkpoints/speed_layer"

# InfluxDB config – fixed URL (no more unreachable localhost)
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://172.20.0.30:8086")  # <-- changed
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "super-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "cyber-org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "speed-bucket")

BRUTE_FORCE_THRESHOLD = 5
DATA_EXFIL_THRESHOLD  = 10_000_000

SIGNATURES = {
    "sqli":           r"(?i)(union\s+select|or\s+1=1|drop\s+table|insert\s+into|select\s+\*|--|#|xp_cmdshell|benchmark\(|sleep\()",
    "xss":            r"(?i)(<script|alert\(|onerror=|onload=|javascript:|<img[^>]+src=|document\.cookie|eval\()",
    "path_traversal": r"(?i)(\.\./|\.\.\\|%2e%2e%2f|%2e%2e/|\.\.%2f|etc/passwd|etc/shadow|/proc/self)",
    "rce":            r"(?i)(cmd=|exec\(|system\(|passthru\(|shell_exec\(|popen\(|proc_open\(|wget\s|curl\s|bash\s+-i|/bin/sh|/bin/bash)",
    "lfi":            r"(?i)(include\s*\(|require\s*\(|file=|page=|path=.*\.\./|/etc/|/var/log|/proc/)",
    "scanner_tool":   r"(?i)(sqlmap|nikto|nmap|masscan|dirbuster|gobuster|wfuzz|hydra|metasploit|nessus)",
    "xxe":            r"(?i)(<!entity|<!doctype|system\s+['\"]|PUBLIC\s+['\"]|/etc/passwd|file://)",
    "ssrf":           r"(?i)(localhost|127\.0\.0\.1|169\.254\.|internal\.|metadata\.google|169\.254\.169\.254)",
}

# ===================== SPARK =====================
def spark_session():
    return (
        SparkSession.builder
        .appName("SOC-Speed-Layer")
        .config("spark.cassandra.connection.host",      CASSANDRA_HOST)
        .config("spark.cassandra.connection.port",      str(CASSANDRA_PORT))
        .config("spark.driver.extraJavaOptions",
                "-Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses=true")
        .config("spark.executor.extraJavaOptions",
                "-Djava.net.preferIPv4Stack=true -Djava.net.preferIPv4Addresses=true")
        .config("spark.sql.shuffle.partitions",             "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )

# ===================== SCHEMA =====================
schema = StructType([
    StructField("timestamp",         StringType()),
    StructField("source_ip",         StringType()),
    StructField("dest_ip",           StringType()),
    StructField("protocol",          StringType()),
    StructField("action",            StringType()),
    StructField("threat_label",      StringType()),
    StructField("log_type",          StringType()),
    StructField("bytes_transferred", StringType()),
    StructField("user_agent",        StringType()),
    StructField("request_path",      StringType()),
])

# ===================== STREAM =====================
def read_stream(spark):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",     KAFKA_BOOTSTRAP)
        .option("subscribe",                   KAFKA_TOPIC)
        .option("startingOffsets",             "latest")
        .option("failOnDataLoss",              "false")
        .option("kafka.request.timeout.ms",    "60000")
        .option("kafka.session.timeout.ms",    "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .option("kafka.max.poll.records",      "500")
        .load()
    )
    df = (
        df.selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json("json", schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
        .withColumn("bytes_xfer", F.col("bytes_transferred").cast("long"))
        .drop("timestamp", "bytes_transferred")
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("source_ip").isNotNull())
    )
    return df

# ===================== INFLUXDB WRITER (no external library) =====================
def _escape_tag(v: str) -> str:
    """Escape spaces, commas, and equals in InfluxDB tag values."""
    return str(v).replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")

def _escape_field_str(v: str) -> str:
    """Wrap string field values in double-quotes, escaping inner quotes."""
    return '"' + str(v).replace('"', '\\"') + '"'

def _write_to_influx(lines: list):
    """
    Send a list of line‑protocol strings to InfluxDB via its HTTP API.
    Uses only the 'requests' library (no influxdb_client dependency).
    """
    if not lines:
        return
    url = f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={INFLUX_BUCKET}&precision=ns"
    headers = {
        "Authorization": f"Token {INFLUX_TOKEN}",
        "Content-Type":  "text/plain; charset=utf-8"
    }
    body = "\n".join(lines).encode("utf-8")
    try:
        resp = requests.post(url, data=body, headers=headers, timeout=10)
        if resp.status_code != 204:
            print(f"[InfluxDB NON-204] {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[InfluxDB ERROR] {e}")

def influx_brute_force_writer(batch_df, batch_id):
    rows = batch_df.select(
        "source_ip", "window_start", "failed_count", "threat_score"
    ).collect()
    lines = []
    for row in rows:
        if row["window_start"] is None:
            continue
        ts_ns = int(row["window_start"].timestamp() * 1e9)
        line = (
            f"brute_force_alerts,"
            f"source_ip={_escape_tag(row['source_ip'])} "
            f"failed_count={row['failed_count']}i,"
            f"threat_score={row['threat_score']} "
            f"{ts_ns}"
        )
        lines.append(line)
    try:
        _write_to_influx(lines)
        print(f"[InfluxDB Batch {batch_id}] brute_force_alerts → {len(lines)} points")
    except Exception as e:
        print(f"[InfluxDB ERROR] brute_force_alerts batch {batch_id}: {e}")

def influx_signatures_writer(batch_df, batch_id):
    rows = batch_df.select(
        "source_ip", "event_time", "attack_type",
        "protocol", "log_type", "threat_score", "bytes_xfer"
    ).collect()
    lines = []
    for row in rows:
        if row["event_time"] is None:
            continue
        ts_ns = int(row["event_time"].timestamp() * 1e9)
        attack = _escape_tag(row["attack_type"] or "unknown")
        proto  = _escape_tag(row["protocol"]    or "unknown")
        ltype  = _escape_tag(row["log_type"]    or "unknown")
        line = (
            f"realtime_threats,"
            f"source_ip={_escape_tag(row['source_ip'])},"
            f"attack_type={attack},"
            f"protocol={proto},"
            f"log_type={ltype} "
            f"threat_score={row['threat_score']},"
            f"bytes_xfer={row['bytes_xfer'] or 0}i "
            f"{ts_ns}"
        )
        lines.append(line)
    try:
        _write_to_influx(lines)
        print(f"[InfluxDB Batch {batch_id}] realtime_threats → {len(lines)} points")
    except Exception as e:
        print(f"[InfluxDB ERROR] realtime_threats batch {batch_id}: {e}")

def influx_exfil_writer(batch_df, batch_id):
    rows = batch_df.select(
        "source_ip", "window_start", "total_bytes", "threat_score"
    ).collect()
    lines = []
    for row in rows:
        if row["window_start"] is None:
            continue
        ts_ns = int(row["window_start"].timestamp() * 1e9)
        line = (
            f"data_exfil_alerts,"
            f"source_ip={_escape_tag(row['source_ip'])} "
            f"total_bytes={row['total_bytes']}i,"
            f"threat_score={row['threat_score']} "
            f"{ts_ns}"
        )
        lines.append(line)
    try:
        _write_to_influx(lines)
        print(f"[InfluxDB Batch {batch_id}] data_exfil_alerts → {len(lines)} points")
    except Exception as e:
        print(f"[InfluxDB ERROR] data_exfil_alerts batch {batch_id}: {e}")

def influx_ip_scores_writer(batch_df, batch_id):
    rows = batch_df.select(
        "source_ip", "last_seen", "threat_score",
        "attack_count", "attack_types", "is_blocked"
    ).collect()
    lines = []
    for row in rows:
        if row["last_seen"] is None:
            continue
        ts_ns      = int(row["last_seen"].timestamp() * 1e9)
        blocked    = _escape_tag(str(row["is_blocked"]).lower())
        atypes_str = _escape_field_str(",".join(row["attack_types"] or []))
        line = (
            f"ip_threat_scores,"
            f"source_ip={_escape_tag(row['source_ip'])},"
            f"is_blocked={blocked} "
            f"threat_score={row['threat_score']},"
            f"attack_count={row['attack_count']}i,"
            f"attack_types={atypes_str} "
            f"{ts_ns}"
        )
        lines.append(line)
    try:
        _write_to_influx(lines)
        print(f"[InfluxDB Batch {batch_id}] ip_threat_scores → {len(lines)} points")
    except Exception as e:
        print(f"[InfluxDB ERROR] ip_threat_scores batch {batch_id}: {e}")

# ===================== CASSANDRA WRITER =====================
def write_to_cassandra(table):
    def writer(batch_df, batch_id):
        try:
            batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=table, keyspace=CASSANDRA_KS) \
                .save()
            print(f"[Cassandra Batch {batch_id}] Written to {table}")
        except Exception as e:
            print(f"[Cassandra ERROR] Batch {batch_id} → {table}: {e}")
    return writer

# ===================== DUAL-SINK WRAPPER =====================
def dual_sink(cassandra_table, influx_writer):
    cass_writer = write_to_cassandra(cassandra_table)
    def writer(batch_df, batch_id):
        batch_df.cache()
        try:
            cass_writer(batch_df, batch_id)
            influx_writer(batch_df, batch_id)
        finally:
            batch_df.unpersist()
    return writer

# ===================== DETECTIONS =====================
def brute_force(df):
    return (
        df.withWatermark("event_time", "2 minutes")
        .filter(
            (F.col("action") == "blocked") |
            (F.lower(F.col("request_path")).contains("login")) |
            (F.lower(F.col("request_path")).contains("auth")) |
            (F.lower(F.col("request_path")).contains("admin"))
        )
        .groupBy(F.window("event_time", "1 minute"), F.col("source_ip"))
        .count()
        .filter(F.col("count") >= BRUTE_FORCE_THRESHOLD)
        .withColumn("threat_score", F.least(F.lit(1.0), F.col("count") / F.lit(20.0)))
        .withColumn("detected_at",  F.current_timestamp())
        .select(
            "source_ip",
            F.col("window.start").alias("window_start"),
            F.col("count").alias("failed_count"),
            F.col("threat_score").cast("float"),
            "detected_at"
        )
    )

def signatures(df):
    attack_type_col = F.when(
        F.col("request_path").rlike(SIGNATURES["scanner_tool"]) |
        F.col("user_agent").rlike(SIGNATURES["scanner_tool"]), "scanner_tool"
    ).when(
        F.col("request_path").rlike(SIGNATURES["sqli"]) |
        F.col("user_agent").rlike(SIGNATURES["sqli"]), "sqli"
    ).when(
        F.col("request_path").rlike(SIGNATURES["xss"]) |
        F.col("user_agent").rlike(SIGNATURES["xss"]), "xss"
    ).when(
        F.col("request_path").rlike(SIGNATURES["path_traversal"]) |
        F.col("user_agent").rlike(SIGNATURES["path_traversal"]), "path_traversal"
    ).when(
        F.col("request_path").rlike(SIGNATURES["rce"]) |
        F.col("user_agent").rlike(SIGNATURES["rce"]), "rce"
    ).when(
        F.col("request_path").rlike(SIGNATURES["lfi"]) |
        F.col("user_agent").rlike(SIGNATURES["lfi"]), "lfi"
    ).when(
        F.col("request_path").rlike(SIGNATURES["xxe"]) |
        F.col("user_agent").rlike(SIGNATURES["xxe"]), "xxe"
    ).when(
        F.col("request_path").rlike(SIGNATURES["ssrf"]) |
        F.col("user_agent").rlike(SIGNATURES["ssrf"]), "ssrf"
    ).otherwise("other_signature")

    all_regex = "|".join(SIGNATURES.values())
    return (
        df.withWatermark("event_time", "2 minutes")
        .filter(
            F.col("request_path").isNotNull() &
            (F.col("request_path").rlike(all_regex) | F.col("user_agent").rlike(all_regex))
        )
        .withColumn("attack_type",  attack_type_col)
        .withColumn("threat_score", F.lit(0.95).cast("float"))
        .withColumn("window_start", F.col("event_time"))
        .withColumn("window_end",   F.col("event_time"))
        .select(
            "source_ip", "event_time", "dest_ip", "protocol", "action",
            "threat_label", "log_type",
            F.coalesce(F.col("bytes_xfer"),    F.lit(0)).alias("bytes_xfer"),
            F.coalesce(F.col("user_agent"),    F.lit("unknown")).alias("user_agent"),
            F.coalesce(F.col("request_path"),  F.lit("/")).alias("request_path"),
            "attack_type", "threat_score", "window_start", "window_end"
        )
    )

def exfiltration(df):
    return (
        df.withWatermark("event_time", "1 minutes")
        .filter(F.col("bytes_xfer").isNotNull() & (F.col("bytes_xfer") > 0))
        .groupBy(F.window("event_time", "10 seconds"), F.col("source_ip"))
        .agg(F.sum("bytes_xfer").alias("total_bytes"))
        .filter(F.col("total_bytes") > DATA_EXFIL_THRESHOLD)
        .withColumn("threat_score", F.lit(1.0).cast("float"))
        .withColumn("detected_at",  F.current_timestamp())
        .select(
            "source_ip",
            F.col("window.start").alias("window_start"),
            F.col("total_bytes").cast("long"),
            "threat_score",
            "detected_at"
        )
    )

def ip_scores(df):
    attack_type_col = F.when(
        F.col("request_path").rlike(SIGNATURES["scanner_tool"]) |
        F.col("user_agent").rlike(SIGNATURES["scanner_tool"]), "scanner_tool"
    ).when(
        F.col("request_path").rlike(SIGNATURES["sqli"]) |
        F.col("user_agent").rlike(SIGNATURES["sqli"]), "sqli"
    ).when(
        F.col("request_path").rlike(SIGNATURES["xss"]) |
        F.col("user_agent").rlike(SIGNATURES["xss"]), "xss"
    ).when(
        F.col("request_path").rlike(SIGNATURES["path_traversal"]) |
        F.col("user_agent").rlike(SIGNATURES["path_traversal"]), "path_traversal"
    ).when(
        F.col("request_path").rlike(SIGNATURES["rce"]) |
        F.col("user_agent").rlike(SIGNATURES["rce"]), "rce"
    ).when(
        F.col("request_path").rlike(SIGNATURES["lfi"]) |
        F.col("user_agent").rlike(SIGNATURES["lfi"]), "lfi"
    ).when(
        F.col("request_path").rlike(SIGNATURES["xxe"]) |
        F.col("user_agent").rlike(SIGNATURES["xxe"]), "xxe"
    ).when(
        F.col("request_path").rlike(SIGNATURES["ssrf"]) |
        F.col("user_agent").rlike(SIGNATURES["ssrf"]), "ssrf"
    )
    return (
        df.withWatermark("event_time", "10 minutes")
        .withColumn("attack_type", attack_type_col)
        .filter(F.col("attack_type").isNotNull())
        .withColumn("weight",
            F.when(F.col("threat_label") == "malicious",  F.lit(1.0))
             .when(F.col("threat_label") == "suspicious", F.lit(0.5))
             .when(F.col("action") == "blocked",          F.lit(0.7))
             .otherwise(F.lit(0.1))
        )
        .groupBy(F.window("event_time", "5 minutes"), "source_ip")
        .agg(
            F.avg("weight").alias("threat_score"),
            F.count("*").alias("attack_count"),
            F.max("event_time").alias("last_seen"),
            F.collect_set("attack_type").alias("attack_types")
        )
        .withColumn("attack_types",
            F.expr("filter(attack_types, x -> x is not null)")
        )
        .withColumn("threat_score",
            F.coalesce(F.col("threat_score"), F.lit(0.1)).cast("float")
        )
        .withColumn("attack_count", F.col("attack_count").cast("int"))
        .withColumn("is_blocked", F.col("threat_score") > 0.7)
        .select(
            "source_ip", "threat_score", "attack_count",
            "last_seen", "is_blocked", "attack_types"
        )
    )

# ===================== MAIN =====================
def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df = read_stream(spark)

    pipelines = [
        ("brute_force_alerts", brute_force(df),  "append", influx_brute_force_writer),
        ("realtime_threats",   signatures(df),   "append", influx_signatures_writer),
        ("data_exfil_alerts",  exfiltration(df), "append", influx_exfil_writer),
        ("ip_threat_scores",   ip_scores(df),    "append", influx_ip_scores_writer),
    ]

    active = []
    for table, stream_df, mode, influx_fn in pipelines:
        q = (
            stream_df.writeStream
            .outputMode(mode)
            .foreachBatch(dual_sink(table, influx_fn))
            .option("checkpointLocation", f"{CHECKPOINT}/{table}")
            .trigger(processingTime="10 seconds")
            .start()
        )
        active.append(q)
        print(f"[OK] Stream started → {table} (Cassandra + InfluxDB)")

    print("\n[RUNNING] Speed Layer active. Ctrl+C to stop.\n")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[STOP] Shutting down...")
    finally:
        for q in active:
            q.stop()
        spark.stop()

if __name__ == "__main__":
    main()
