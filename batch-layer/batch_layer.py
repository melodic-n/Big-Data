from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum as spark_sum, avg,
    to_timestamp, to_date, current_timestamp,
    regexp_extract, concat_ws, date_trunc,
    max as spark_max, lit, countDistinct, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, DoubleType
)
import sys
import time
import re
import socket

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
HBASE_HOST     = "hbase"
HBASE_PORT     = 9090
INFLUXDB_URL   = "http://influxdb:8086"
INFLUXDB_TOKEN = "super-token"
INFLUXDB_ORG   = "cyber-org"
INFLUXDB_BUCKET= "cyber-bucket"

WRITE_TO_HBASE    = True
WRITE_TO_INFLUXDB = True

THREAT_LABELS = ["suspicious", "malicious"]

# ─────────────────────────────────────────────────────────────────────────────
# VÉRIFICATION DE CONNECTIVITÉ
# ─────────────────────────────────────────────────────────────────────────────
def check_host_port(host, port, timeout=2):
    try:
        socket.create_connection((host, port), timeout=timeout)
        return True
    except (socket.timeout, socket.error):
        return False

def diagnose_connectivity():
    print("\n[DIAG] Service connectivity check:")
    hbase_ok  = check_host_port(HBASE_HOST, HBASE_PORT)
    influx_ok = check_host_port("influxdb", 8086)
    print(f"       HBase ({HBASE_HOST}:{HBASE_PORT}): {'✓ OK' if hbase_ok else '✗ OFFLINE'}")
    print(f"       InfluxDB (influxdb:8086):        {'✓ OK' if influx_ok else '✗ OFFLINE'}")
    if not hbase_ok:
        print("\n[WARN] HBase is unreachable. Possible causes:")
        print("       1. HBase container is not running")
        print("       2. Network bridge is misconfigured (check docker-compose)")
        print(f"       3. HBASE_HOST IP {HBASE_HOST} is incorrect")
        print("       4. HBase hasn't fully started (try waiting 30-60 seconds)")
    if not influx_ok:
        print("\n[WARN] InfluxDB is unreachable. Possible causes:")
        print("       1. InfluxDB container is not running")
        print("       2. Service hasn't fully started")
    return hbase_ok, influx_ok

# ─────────────────────────────────────────────────────────────────────────────
# SCHÉMA CSV
# ─────────────────────────────────────────────────────────────────────────────
CSV_SCHEMA = StructType([
    StructField("timestamp",         TimestampType(), True),
    StructField("source_ip",         StringType(),    True),
    StructField("dest_ip",           StringType(),    True),
    StructField("protocol",          StringType(),    True),
    StructField("action",            StringType(),    True),
    StructField("threat_label",      StringType(),    True),
    StructField("log_type",          StringType(),    True),
    StructField("bytes_transferred", LongType(),      True),
    StructField("user_agent",        StringType(),    True),
    StructField("request_path",      StringType(),    True),
    StructField("response_code",     LongType(),      True),
])

# ─────────────────────────────────────────────────────────────────────────────
# CONNEXIONS  ← imports are INSIDE the functions, exactly like the first script
# ─────────────────────────────────────────────────────────────────────────────
def connect_hbase():
    import happybase                          # ← import here, not at top
    if not WRITE_TO_HBASE:
        print("[INFO] HBase writes disabled")
        return None
    for i in range(5):
        try:
            conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=5000)
            conn.open()
            print("[OK] HBase connected successfully")
            return conn
        except Exception as e:
            print(f"[WARN] HBase connection retry {i+1}/5... ({type(e).__name__})")
            time.sleep(2)
    print(f"[ERROR] Failed to connect to HBase after 5 retries")
    return None

def connect_influx():
    from influxdb_client import InfluxDBClient  # ← import here, not at top
    if not WRITE_TO_INFLUXDB:
        print("[INFO] InfluxDB writes disabled")
        return None
    for i in range(5):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG, timeout=10000)
            client.ping()
            print("[OK] InfluxDB connected successfully")
            return client
        except Exception as e:
            print(f"[WARN] InfluxDB retry {i+1}/5... ({type(e).__name__})")
            time.sleep(2)
    print(f"[ERROR] Failed to connect to InfluxDB after 5 retries")
    return None

# ─────────────────────────────────────────────────────────────────────────────
# ÉCRITURE PARTITION  ← NO bare imports at top of this function
# ─────────────────────────────────────────────────────────────────────────────
def write_partition(iterator, table_name, cf, col_map, measurement, tag_cols, field_cols, time_col):
    from influxdb_client.client.write_api import SYNCHRONOUS   # ← safe, only used if connect_influx succeeds
    from datetime import datetime

    hbase_conn    = None
    influx_client = None
    success_count = 0

    try:
        if WRITE_TO_HBASE:
            hbase_conn = connect_hbase()        # happybase imported inside connect_hbase()
            batch = hbase_conn.table(table_name).batch(batch_size=1000) if hbase_conn else None
        else:
            batch = None

        if WRITE_TO_INFLUXDB:
            influx_client = connect_influx()    # influxdb_client imported inside connect_influx()
            write_api = influx_client.write_api(write_options=SYNCHRONOUS) if influx_client else None
        else:
            write_api = None

        if not batch and not write_api:
            print("[WARN] No write targets available")

        points   = []
        cf_bytes = cf.encode()

        for row in iterator:
            try:
                # ── HBase ─────────────────────────────────────────────────
                if batch:
                    row_key = str(row["row_key"]).encode()
                    data = {}
                    for spark_col, hbase_qual in col_map.items():
                        val = row[spark_col]
                        if val is not None:
                            data[cf_bytes + b":" + hbase_qual.encode()] = str(val).encode()
                    if data:
                        batch.put(row_key, data)

                # ── InfluxDB ──────────────────────────────────────────────
                if write_api:
                    from influxdb_client import Point
                    point = Point(measurement)
                    for tag in tag_cols:
                        val = row[tag]
                        if val is not None:
                            point.tag(tag, str(val))
                    for field in field_cols:
                        val = row[field]
                        if val is not None:
                            if isinstance(val, (int, float, bool)):
                                point.field(field, val)
                            else:
                                point.field(field, str(val))
                    ts = row[time_col]
                    if ts:
                        point.time(ts)
                    else:
                        point.time(datetime.utcnow())
                    points.append(point)
                    if len(points) >= 3000:
                        write_api.write(bucket=INFLUXDB_BUCKET, record=points)
                        success_count += len(points)
                        points = []
            except Exception as e:
                print(f"[WARN] Row processing failed: {e}")

        if batch:
            try:
                batch.send()
                print("[OK] HBase batch flushed")
            except Exception as e:
                print(f"[WARN] HBase batch flush failed: {e}")

        if points:
            try:
                write_api.write(bucket=INFLUXDB_BUCKET, record=points)
                success_count += len(points)
                print(f"[OK] InfluxDB final batch written ({len(points)} points)")
            except Exception as e:
                print(f"[WARN] InfluxDB final batch failed: {e}")

    except Exception as e:
        print(f"[ERROR] Partition processing failed: {e}")
    finally:
        if hbase_conn:
            hbase_conn.close()
        if influx_client:
            influx_client.close()

# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("SOC_Batch_STABLE").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

if len(sys.argv) < 2:
    print("Usage: spark-submit script.py <hdfs_path>")
    sys.exit(1)

INPUT_PATH = sys.argv[1]
print(f"\n[INFO] Reading: {INPUT_PATH}")

hbase_available, influx_available = diagnose_connectivity()
if not hbase_available:
    WRITE_TO_HBASE = False
    print("[WARN] Disabling HBase writes")
if not influx_available:
    WRITE_TO_INFLUXDB = False
    print("[WARN] Disabling InfluxDB writes")

# ─────────────────────────────────────────────────────────────────────────────
# LECTURE DES DONNÉES
# ─────────────────────────────────────────────────────────────────────────────
logs_raw = spark.read.option("header", "true").schema(CSV_SCHEMA).csv(INPUT_PATH)

logs = logs_raw.withColumn(
    "timestamp",
    when(col("timestamp").isNotNull(), to_timestamp(col("timestamp"))).otherwise(current_timestamp())
).withColumn(
    "detection_date", to_date(col("timestamp"))
).withColumn(
    "dest_port",
    when(col("dest_ip").isNotNull(), regexp_extract(col("dest_ip"), r":(\d+)", 1)).otherwise("")
).withColumn(
    "bytes_transferred",
    when(col("bytes_transferred").isNotNull(), col("bytes_transferred")).otherwise(0)
)

record_count = logs.count()
if record_count == 0:
    print("[INFO] No data found in partition.")
    spark.stop()
    sys.exit(0)

print(f"[INFO] Loaded {record_count:,} records from partition")
logs = logs.repartition(4)

# ─────────────────────────────────────────────────────────────────────────────
# EXTRACTION DATE DE PARTITION
# ─────────────────────────────────────────────────────────────────────────────
match = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', INPUT_PATH)
if match:
    year, month, day = match.groups()
    partition_date = f"{year}-{month}-{day}"
else:
    from datetime import date
    partition_date = date.today().isoformat()

print(f"[INFO] Partition date: {partition_date}")

# ─────────────────────────────────────────────────────────────────────────────
# DÉTECTION DE PATTERNS D'ATTAQUE
# ─────────────────────────────────────────────────────────────────────────────
ATTACK_PATTERNS = {
    "SQL_Injection":  r"(?i)(' OR 1=1|UNION SELECT|sqlmap|--\s|;--|DROP TABLE|INSERT INTO|' OR '1'='1)",
    "XSS":            r"(?i)(<script>|alert\(|onerror=|javascript:|onload=)",
    "Path_Traversal": r"(?i)(\.\./|\.\.\\|%2e%2e%2f|%252e%252e%252f)",
    "RCE":            r"(?i)(;ls |;cat |;wget |;curl |`id`|\$\(id\))",
    "Nikto_Scan":     r"(?i)(nikto|nmap|masscan)",
}

df_sec = logs
for name, pattern in ATTACK_PATTERNS.items():
    df_sec = df_sec.withColumn(name, when(col("request_path").rlike(pattern), 1).otherwise(0))

df_sec = df_sec.withColumn(
    "attack_type",
    when(col("SQL_Injection")  == 1, "SQL_Injection")
    .when(col("XSS")           == 1, "XSS")
    .when(col("Path_Traversal")== 1, "Path_Traversal")
    .when(col("RCE")           == 1, "RCE")
    .when(col("Nikto_Scan")    == 1, "Nikto_Scan")
    .otherwise("Normal")
)

# ─────────────────────────────────────────────────────────────────────────────
# AGRÉGATIONS
# ─────────────────────────────────────────────────────────────────────────────

# 1. IP REPUTATION — score de réputation par IP
ip_rep = logs.groupBy("source_ip").agg(
    count("*").alias("total_requests"),
    count(when(col("threat_label").isin(THREAT_LABELS), True)).alias("threat_count"),
    count(when(col("action") == "blocked", True)).alias("blocked_count"),
    spark_sum("bytes_transferred").alias("total_bytes")
).withColumn(
    "threat_score",
    when(col("total_requests") > 0, col("threat_count") / col("total_requests")).otherwise(0.0)
).withColumn(
    "row_key", col("source_ip")
).withColumn(
    "detection_date", to_timestamp(lit(partition_date + " 00:00:00"))
)

# 2. ATTACK PATTERNS — patterns d'attaques détectés
attack_df = df_sec.filter(col("attack_type") != "Normal") \
    .groupBy("attack_type", "detection_date") \
    .agg(
        count("*").alias("attack_count"),
        countDistinct("source_ip").alias("unique_ips")
    ) \
    .withColumn("row_key", concat_ws("#", col("attack_type"), col("detection_date"))) \
    .withColumn("detection_ts", to_timestamp(lit(partition_date + " 00:00:00")))

# 3. THREAT TIMELINE — évolution temporelle des menaces
timeline = logs \
    .withColumn("hour", date_trunc("hour", col("timestamp"))) \
    .groupBy("hour", "threat_label") \
    .agg(
        count("*").alias("event_count"),
        spark_sum("bytes_transferred").alias("bytes_sum")
    ) \
    .withColumn("row_key", concat_ws("#", col("threat_label"), col("hour")))

# 4. PORT SCANS — détection de scans réseau
port_scans = logs.filter(col("protocol") == "TCP") \
    .groupBy("source_ip") \
    .agg(
        countDistinct("dest_ip").alias("distinct_destinations"),
        count("*").alias("total_connections"),
        count(when(col("threat_label").isin(THREAT_LABELS), True)).alias("threat_connections")
    ) \
    .filter(col("distinct_destinations") > 5) \
    .withColumn("scan_score", col("distinct_destinations") / col("total_connections")) \
    .withColumn("row_key", col("source_ip")) \
    .withColumn("detection_date", to_timestamp(lit(partition_date + " 00:00:00")))

# 5. TOP IPs — top 100 IPs malveillantes
top_ips = logs.filter(col("threat_label").isin(THREAT_LABELS)) \
    .groupBy("source_ip") \
    .agg(
        count("*").alias("threat_count"),
        countDistinct("dest_ip").alias("targets_count"),
        spark_sum("bytes_transferred").alias("total_bytes"),
        count(when(col("action") == "blocked", True)).alias("blocked_count")
    ) \
    .orderBy(desc("threat_count")) \
    .limit(100) \
    .withColumn("row_key", col("source_ip")) \
    .withColumn("detection_date", to_timestamp(lit(partition_date + " 00:00:00")))

# 6. VOLUME ANALYSIS — volume de données par type de menace
volume = logs.groupBy("threat_label") \
    .agg(
        spark_sum("bytes_transferred").alias("total_bytes"),
        avg("bytes_transferred").alias("avg_bytes"),
        spark_max("bytes_transferred").alias("max_bytes"),
        count("*").alias("event_count")
    ) \
    .withColumn("row_key", col("threat_label")) \
    .withColumn("detection_date", to_timestamp(lit(partition_date + " 00:00:00")))

# ─────────────────────────────────────────────────────────────────────────────
# ÉCRITURES
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[INFO] Writing results (HBase: {WRITE_TO_HBASE}, InfluxDB: {WRITE_TO_INFLUXDB})...")

# 1. ip_reputation
try:
    ip_rep.foreachPartition(lambda it: write_partition(
        it, "ip_reputation", "info",
        {
            "total_requests": "total_requests",
            "threat_count"  : "threat_count",
            "blocked_count" : "blocked_count",
            "total_bytes"   : "total_bytes",
            "threat_score"  : "threat_score",
        },
        "ip_reputation", ["source_ip"],
        ["total_requests", "threat_count", "blocked_count", "total_bytes", "threat_score"],
        "detection_date"
    ))
    print("[OK] IP reputation processing completed")
except Exception as e:
    print(f"[WARN] IP reputation write failed: {e}")

# 2. attack_patterns
try:
    attack_df.foreachPartition(lambda it: write_partition(
        it, "attack_patterns", "stats",
        {
            "attack_count": "attack_count",
            "unique_ips"  : "unique_ips",
        },
        "attack_patterns", ["attack_type"],
        ["attack_count", "unique_ips"],
        "detection_ts"
    ))
    print("[OK] Attack patterns processing completed")
except Exception as e:
    print(f"[WARN] Attack patterns write failed: {e}")

# 3. threat_timeline
try:
    timeline.foreachPartition(lambda it: write_partition(
        it, "threat_timeline", "data",
        {
            "event_count": "event_count",
            "bytes_sum"  : "bytes_sum",
        },
        "threat_timeline", ["threat_label"],
        ["event_count", "bytes_sum"],
        "hour"
    ))
    print("[OK] Timeline processing completed")
except Exception as e:
    print(f"[WARN] Timeline write failed: {e}")

# 4. port_scans
try:
    port_scans.foreachPartition(lambda it: write_partition(
        it, "port_scans", "info",
        {
            "distinct_destinations": "distinct_destinations",
            "total_connections"    : "total_connections",
            "threat_connections"   : "threat_connections",
            "scan_score"           : "scan_score",
        },
        "port_scans", ["source_ip"],
        ["distinct_destinations", "total_connections", "threat_connections", "scan_score"],
        "detection_date"
    ))
    print("[OK] Port scans processing completed")
except Exception as e:
    print(f"[WARN] Port scans write failed: {e}")

# 5. top_ips
try:
    top_ips.foreachPartition(lambda it: write_partition(
        it, "top_ips", "info",
        {
            "threat_count" : "threat_count",
            "targets_count": "targets_count",
            "total_bytes"  : "total_bytes",
            "blocked_count": "blocked_count",
        },
        "top_ips", ["source_ip"],
        ["threat_count", "targets_count", "total_bytes", "blocked_count"],
        "detection_date"
    ))
    print("[OK] Top IPs processing completed")
except Exception as e:
    print(f"[WARN] Top IPs write failed: {e}")

# 6. volume_analysis
try:
    volume.foreachPartition(lambda it: write_partition(
        it, "volume_analysis", "info",
        {
            "total_bytes": "total_bytes",
            "avg_bytes"  : "avg_bytes",
            "max_bytes"  : "max_bytes",
            "event_count": "event_count",
        },
        "volume_analysis", ["threat_label"],
        ["total_bytes", "avg_bytes", "max_bytes", "event_count"],
        "detection_date"
    ))
    print("[OK] Volume analysis processing completed")
except Exception as e:
    print(f"[WARN] Volume analysis write failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n✅ BATCH PROCESSING COMPLETE\n")
