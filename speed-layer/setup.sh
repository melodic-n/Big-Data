#!/bin/bash
set -e

# --- Configuration ---
KAFKA_CONTAINER="kafka"
CASSANDRA_CONTAINER="cassandra"
HADOOP_MASTER="hadoop-master"
CASS_KS="cybersecurity"
TOPIC_NAME="cybersecurity-logs"
CSV_LOCAL_PATH="../data/cybersecurity_threat_detection_logs_streaming.csv"
INFLUXDB_TOKEN="super-token"
# Colors
GREEN='\033[0;32m'; BLUE='\033[0;34m'; CYAN='\033[0;36m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}   $1"; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}      CYBERSECURITY SPEED LAYER — ALL-IN-DOCKER             ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Resolve Cassandra IP
CASS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CASSANDRA_CONTAINER")

# 2. Setup Cassandra Schema
log_step "Initializing Cassandra Schema..."
docker exec -i "$CASSANDRA_CONTAINER" cqlsh <<EOF
DROP KEYSPACE IF EXISTS $CASS_KS;
CREATE KEYSPACE IF NOT EXISTS $CASS_KS WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE $CASS_KS;

CREATE TABLE IF NOT EXISTS realtime_threats (
    source_ip TEXT, event_time TIMESTAMP, dest_ip TEXT, protocol TEXT, action TEXT, 
    threat_label TEXT, log_type TEXT, bytes_xfer BIGINT, user_agent TEXT, request_path TEXT,
    attack_type TEXT, threat_score FLOAT, window_start TIMESTAMP, window_end TIMESTAMP,
    PRIMARY KEY (source_ip, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC) AND default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS brute_force_alerts (source_ip TEXT, window_start TIMESTAMP, failed_count INT, threat_score FLOAT, detected_at TIMESTAMP, PRIMARY KEY (source_ip, window_start));
CREATE TABLE IF NOT EXISTS ip_threat_scores (source_ip TEXT PRIMARY KEY, threat_score FLOAT, attack_count INT, last_seen TIMESTAMP, is_blocked BOOLEAN, attack_types set<TEXT>);
CREATE TABLE IF NOT EXISTS data_exfil_alerts (source_ip text, window_start timestamp, total_bytes bigint, threat_score float, detected_at timestamp, PRIMARY KEY (source_ip, window_start)) WITH CLUSTERING ORDER BY (window_start DESC);
EOF
log_success "Cassandra ready."

# --- InfluxDB Setup ---
log_step "Configuring InfluxDB Bucket..."

docker exec influxdb influx bucket delete --name speed-bucket --org cyber-org --token super-token || true
docker exec influxdb influx bucket create \
    --name speed-bucket \
    --org cyber-org \
    --token $INFLUXDB_TOKEN \
    --description "Bucket for Real-time Speed Layer" \
    || echo -e "${YELLOW}[INFO]${NC} Bucket might already exist, skipping..."

log_success "InfluxDB bucket ready."

# =============================================================================
# 2.5 Verification of Hadoop/Spark Nodes (JPS Check)
# =============================================================================
log_step "Verifying Hadoop/Spark Daemons via JPS..."

# 1. Check NameNode and ResourceManager on Master
MASTER_JPS=$(docker exec "$HADOOP_MASTER" jps)
for daemon in "NameNode" "ResourceManager"; do
    if echo "$MASTER_JPS" | grep -q "$daemon"; then
        log_success "$daemon is running on Master."
    else
        log_error "$daemon is NOT running on Master. Check Hadoop logs."
    fi
done

# 2. Check DataNodes on Workers (if they exist in your setup)
# Assuming you have workers named hadoop-worker1, hadoop-worker2
WORKERS=("hadoop-worker1" "hadoop-worker2")
for W in "${WORKERS[@]}"; do
    # Vérifier si le container existe avant de lancer jps
    if [ "$(docker ps -q -f name=$W)" ]; then
        WORKER_JPS=$(docker exec "$W" jps)
        if echo "$WORKER_JPS" | grep -q "DataNode"; then
            log_success "DataNode is running on $W."
        else
            log_warn "DataNode is NOT running on $W. Spark might have limited storage access."
        fi
    fi
done

# 3. Launch Spark Streaming
log_step "Deploying Spark Streaming to Hadoop Master..."
docker exec -u root "$HADOOP_MASTER" pip3 install requests --quiet
docker cp spark_streaming.py "$HADOOP_MASTER":/tmp/spark_streaming.py
docker exec -d "$HADOOP_MASTER" bash -c "spark-submit \
    --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
    --conf 'spark.cassandra.connection.host=$CASS_IP' \
    /tmp/spark_streaming.py > /tmp/spark_speed_layer.log 2>&1"
log_success "Spark Job running in background."

# 4. Prepare & Run Producer inside Kafka Container
log_step "Preparing Producer inside Kafka Container..."
docker exec -it "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC_NAME" --if-not-exists --partitions 1 --replication-factor 1

# Copy files to Kafka
docker cp producer.py "$KAFKA_CONTAINER":/tmp/producer.py
docker cp "$CSV_LOCAL_PATH" "$KAFKA_CONTAINER":/tmp/streaming_data.csv

# Install dependencies inside Kafka container
log_step "Launching Producer..."
docker exec -it "$KAFKA_CONTAINER" bash -c "pip3 install kafka-python-ng --quiet"
docker exec -it "$KAFKA_CONTAINER" python3 /tmp/producer.py
echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}    SPEED LAYER IS ACTIVE (ALL LOGS STREAMED)               ${NC}"
echo -e "${GREEN}============================================================${NC}"
