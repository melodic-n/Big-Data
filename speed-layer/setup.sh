#!/bin/bash
# =============================================================================
# speed_layer_setup.sh — Bulletproof Speed Layer Orchestrator
# This script automates the full lifecycle of the Cybersecurity SOC Speed Layer.
# =============================================================================
set -e

# --- Configuration ---
KAFKA_CONTAINER="kafka"
CASSANDRA_CONTAINER="cassandra"
HADOOP_MASTER="hadoop-master"
CASS_KS="cybersecurity"
TOPIC_NAME="cybersecurity-logs"
CSV_LOCAL_PATH="../data/cybersecurity_threat_detection_logs_streaming.csv"
INFLUXDB_TOKEN="super-token"

# Verified Hadoop Binary Path
HADOOP_SBIN="/usr/local/hadoop/sbin"

# Colors for professional logging
GREEN='\033[0;32m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}   $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
log_warn() { echo -e "${YELLOW}[WARN]${NC}  $1"; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}      CYBERSECURITY SPEED LAYER — FULL AUTOMATION           ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Network Discovery
# Resolve the internal Docker IP of the Cassandra container for Spark connectivity.
log_step "Resolving Cassandra IP..."
CASS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CASSANDRA_CONTAINER")
log_success "Cassandra found at $CASS_IP"

# 2. Cassandra Schema Initialization
# Drops and recreates the keyspace to ensure a clean state for real-time processing.
log_step "Initializing Cassandra Schema (Clean State)..."
docker exec -i "$CASSANDRA_CONTAINER" cqlsh <<EOF
DROP KEYSPACE IF EXISTS $CASS_KS;
CREATE KEYSPACE IF NOT EXISTS $CASS_KS WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE $CASS_KS;

CREATE TABLE realtime_threats (
    source_ip TEXT, event_time TIMESTAMP, dest_ip TEXT, protocol TEXT, action TEXT, 
    threat_label TEXT, log_type TEXT, bytes_xfer BIGINT, user_agent TEXT, request_path TEXT,
    attack_type TEXT, threat_score FLOAT, window_start TIMESTAMP, window_end TIMESTAMP,
    PRIMARY KEY (source_ip, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC) AND default_time_to_live = 86400;

CREATE TABLE brute_force_alerts (source_ip TEXT, window_start TIMESTAMP, failed_count INT, threat_score FLOAT, detected_at TIMESTAMP, PRIMARY KEY (source_ip, window_start));
CREATE TABLE ip_threat_scores (source_ip TEXT PRIMARY KEY, threat_score FLOAT, attack_count INT, last_seen TIMESTAMP, is_blocked BOOLEAN, attack_types set<TEXT>);
CREATE TABLE data_exfil_alerts (source_ip text, window_start timestamp, total_bytes bigint, threat_score float, detected_at timestamp, PRIMARY KEY (source_ip, window_start)) WITH CLUSTERING ORDER BY (window_start DESC);
EOF
log_success "Cassandra schema reset successfully."

# 3. InfluxDB Setup
# Resets the specific bucket used for real-time visualization in Grafana.
log_step "Configuring InfluxDB Bucket..."
docker exec influxdb influx bucket delete --name speed-bucket --org cyber-org --token $INFLUXDB_TOKEN || true
docker exec influxdb influx bucket create \
    --name speed-bucket --org cyber-org --token $INFLUXDB_TOKEN \
    --description "Bucket for Real-time Speed Layer"
log_success "InfluxDB bucket reset and ready."

log_step "Force killing all previous Spark jobs..."
for container in "$HADOOP_MASTER" "hadoop-worker1" "hadoop-worker2"; do
    docker exec $container pkill -9 java || true
done

# 4. Hadoop/Spark Auto-Healing Verification
# Checks if required daemons are running; if not, attempts to start them using verified paths.
log_step "Verifying Hadoop Daemons with Auto-Healing..."
for daemon in "NameNode" "ResourceManager"; do
    if docker exec "$HADOOP_MASTER" jps | grep -q "$daemon"; then
        log_success "$daemon is active."
    else
        log_warn "$daemon is missing! Attempting to start via $HADOOP_SBIN..."
        if [ "$daemon" == "NameNode" ]; then
            docker exec "$HADOOP_MASTER" "$HADOOP_SBIN/start-dfs.sh"
        else
            docker exec "$HADOOP_MASTER" "$HADOOP_SBIN/start-yarn.sh"
        fi
        sleep 5 # Grace period for Java process startup
        
        # Post-restart verification
        if docker exec "$HADOOP_MASTER" jps | grep -q "$daemon"; then
            log_success "$daemon recovered successfully."
        else
            log_error "Failed to start $daemon. Check Hadoop logs in $HADOOP_MASTER."
        fi
    fi
done

log_step "Ensuring HDFS is out of Safe Mode..."
docker exec "$HADOOP_MASTER" hdfs dfsadmin -safemode leave

# 5. Spark Environment Reset
# Kills stale Spark sessions and wipes checkpoints to force "earliest" offset processing.
log_step "Wiping Spark Checkpoints and killing old jobs..."
docker exec "$HADOOP_MASTER" pkill -f spark-submit || true
docker exec "$HADOOP_MASTER" rm -rf /tmp/spark_checkpoints || true
docker exec hadoop-master hdfs dfs -rm -r /tmp/spark_checkpoints || true
sleep 2

# 6. Spark Streaming Deployment
# Deploys code to the Hadoop Master and submits the streaming job.
log_step "Deploying Spark Streaming Job..."
docker exec -u root "$HADOOP_MASTER" pip3 install requests --quiet
docker cp spark_streaming.py "$HADOOP_MASTER":/tmp/spark_streaming.py

# Submit Spark Job with Kafka and Cassandra connectors
docker exec -d "$HADOOP_MASTER" bash -c "spark-submit \
    --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    --conf 'spark.cassandra.connection.host=$CASS_IP' \
    /tmp/spark_streaming.py > /tmp/spark_speed_layer.log 2>&1"

log_step "Waiting for Spark initialization (15s)..."
sleep 15
if docker exec "$HADOOP_MASTER" grep -q "Exception" /tmp/spark_speed_layer.log; then
    log_warn "Potential error in Spark logs. Review with: docker exec $HADOOP_MASTER tail -f /tmp/spark_speed_layer.log"
else
    log_success "Spark Job is running in background."
fi

# 7. Data Ingestion (Kafka Producer)
# Sets up the Kafka topic and triggers the Python producer to stream CSV data.
log_step "Starting Kafka Producer..."
docker exec -it "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC_NAME" --if-not-exists --partitions 1 --replication-factor 1
docker cp producer.py "$KAFKA_CONTAINER":/tmp/producer.py
docker cp "$CSV_LOCAL_PATH" "$KAFKA_CONTAINER":/tmp/streaming_data.csv
docker exec -it "$KAFKA_CONTAINER" pip3 install kafka-python-ng --quiet

log_success "Streaming data now..."
docker exec -it "$KAFKA_CONTAINER" python3 /tmp/producer.py

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}      SPEED LAYER SETUP COMPLETE - MONITORING ACTIVE         ${NC}"
echo -e "${GREEN}============================================================${NC}"
