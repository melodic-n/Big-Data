from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, sum, to_timestamp, to_date,
    current_timestamp, regexp_extract, concat_ws,
    date_trunc, max as spark_max, lit
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
HBASE_HOST = "hbase"   # Nom du conteneur HBase
HBASE_PORT = 9090

INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "super-token"
INFLUXDB_ORG = "cyber-org"
INFLUXDB_BUCKET = "cyber-bucket"

WRITE_TO_HBASE = True
WRITE_TO_INFLUXDB = True

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
    hbase_ok = check_host_port(HBASE_HOST, HBASE_PORT)
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
    StructField("timestamp", TimestampType(), True),
    StructField("source_ip", StringType(), True),
    StructField("dest_ip", StringType(), True),
    StructField("request_path", StringType(), True),
    StructField("threat_label", StringType(), True),
    StructField("response_code", LongType(), True),
])

# ─────────────────────────────────────────────────────────────────────────────
# CONNEXIONS
# ─────────────────────────────────────────────────────────────────────────────
def connect_hbase():
    import happybase
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
    from influxdb_client import InfluxDBClient
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
# ÉCRITURE PARTITION
# ─────────────────────────────────────────────────────────────────────────────
def write_partition(iterator, table_name, cf, col_map, measurement, tag_cols, field_cols, time_col):
    import happybase
    from influxdb_client import Point
    from influxdb_client.client.write_api import SYNCHRONOUS
    from datetime import datetime

    hbase_conn = None
    influx_client = None
    success_count = 0

    try:
        if WRITE_TO_HBASE:
            hbase_conn = connect_hbase()
            batch = hbase_conn.table(table_name).batch(batch_size=1000) if hbase_conn else None
        else:
            batch = None

        if WRITE_TO_INFLUXDB:
            influx_client = connect_influx()
            write_api = influx_client.write_api(write_options=SYNCHRONOUS) if influx_client else None
        else:
            write_api = None

        if not batch and not write_api:
            print("[WARN] No write targets available")

        points = []
        cf_bytes = cf.encode()

        for row in iterator:
            try:
                # HBase
                if batch:
                    row_key = str(row["row_key"]).encode()
                    data = {}
                    for spark_col, hbase_qual in col_map.items():
                        val = row[spark_col]
                        if val is not None:
                            data[cf_bytes + b":" + hbase_qual.encode()] = str(val).encode()
                    if data:
                        batch.put(row_key, data)

                # InfluxDB
                if write_api:
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

# Lecture des données
logs_raw = spark.read.option("header", "true").schema(CSV_SCHEMA).csv(INPUT_PATH)
logs = logs_raw.withColumn(
    "timestamp",
    when(col("timestamp").isNotNull(), to_timestamp(col("timestamp"))).otherwise(current_timestamp())
).withColumn(
    "detection_date", to_date(col("timestamp"))
).withColumn(
    "dest_port",
    when(col("dest_ip").isNotNull(), regexp_extract(col("dest_ip"), r":(\d+)", 1)).otherwise("")
)

record_count = logs.count()
if record_count == 0:
    print("[INFO] No data found in partition.")
    spark.stop()
    sys.exit(0)

print(f"[INFO] Loaded {record_count:,} records from partition")
logs = logs.repartition(4)

# ─────────────────────────────────────────────────────────────────────────────
# ANALYSE DE SÉCURITÉ
# ─────────────────────────────────────────────────────────────────────────────
patterns = {
    "SQL_Injection": r"(?i)(' OR 1=1|UNION SELECT|sqlmap)",
    "XSS": r"(?i)(<script>|alert\()",
}
df_sec = logs
for name, pattern in patterns.items():
    df_sec = df_sec.withColumn(name, when(col("request_path").rlike(pattern), 1).otherwise(0))
df_sec = df_sec.withColumn(
    "attack_type",
    when(col("SQL_Injection") == 1, "SQL_Injection")
    .when(col("XSS") == 1, "XSS")
    .otherwise("Normal")
)

# ─────────────────────────────────────────────────────────────────────────────
# AGRÉGATIONS
# ─────────────────────────────────────────────────────────────────────────────
# Extraction de la date de partition
match = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', INPUT_PATH)
if match:
    year, month, day = match.groups()
    partition_date = f"{year}-{month}-{day}"
else:
    from datetime import date
    partition_date = date.today().isoformat()

ip_rep = logs.groupBy("source_ip").agg(
    count("*").alias("total_requests"),
    count(when(col("threat_label") != "benign", True)).alias("threat_count")
).withColumn(
    "threat_score", col("threat_count") / col("total_requests")
).withColumn(
    "row_key", col("source_ip")
).withColumn(
    "detection_date", to_timestamp(lit(partition_date + " 00:00:00"))   # ✅ Conversion en timestamp
)

attack_df = df_sec.filter(col("attack_type") != "Normal") \
    .groupBy("attack_type", "detection_date") \
    .agg(count("*").alias("attack_count")) \
    .withColumn("row_key", concat_ws("#", col("attack_type"), col("detection_date")))

timeline = logs.withColumn("hour", date_trunc("hour", col("timestamp"))) \
    .groupBy("hour", "threat_label") \
    .agg(count("*").alias("event_count")) \
    .withColumn("row_key", concat_ws("#", col("threat_label"), col("hour")))

# ─────────────────────────────────────────────────────────────────────────────
# ÉCRITURES
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[INFO] Writing results (HBase: {WRITE_TO_HBASE}, InfluxDB: {WRITE_TO_INFLUXDB})...")

try:
    ip_rep.foreachPartition(lambda it: write_partition(
        it, "ip_reputation", "info",
        {"total_requests": "total_requests", "threat_count": "threat_count"},
        "ip_reputation", ["source_ip"],
        ["total_requests", "threat_count"],
        "detection_date"
    ))
    print("[OK] IP reputation processing completed")
except Exception as e:
    print(f"[WARN] IP reputation write failed: {e}")

try:
    attack_df.foreachPartition(lambda it: write_partition(
        it, "attack_patterns", "stats",
        {"attack_count": "attack_count"},
        "attack_patterns", ["attack_type"],
        ["attack_count"],
        "detection_date"
    ))
    print("[OK] Attack patterns processing completed")
except Exception as e:
    print(f"[WARN] Attack patterns write failed: {e}")

try:
    timeline.foreachPartition(lambda it: write_partition(
        it, "threat_timeline", "data",
        {"event_count": "event_count"},
        "threat_timeline", ["threat_label"],
        ["event_count"],
        "hour"
    ))
    print("[OK] Timeline processing completed")
except Exception as e:
    print(f"[WARN] Timeline write failed: {e}")

print("\n✅ BATCH PROCESSING COMPLETE\n")
spark.stop()