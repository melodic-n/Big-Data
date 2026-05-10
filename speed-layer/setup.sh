#!/bin/bash

# =============================================================================
# CYBERSECURITY SPEED LAYER — FULL AUTOMATION
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION
# =============================================================================

KAFKA_CONTAINER="kafka"
CASSANDRA_CONTAINER="cassandra"
HADOOP_MASTER="hadoop-master"

CASS_KS="cybersecurity"
TOPIC_NAME="cybersecurity-logs"

CSV_LOCAL_PATH="../data/cybersecurity_threat_detection_logs_streaming.csv"

INFLUXDB_TOKEN="super-token"

HADOOP_SBIN="/usr/local/hadoop/sbin"

CHECKPOINT_BASE="/tmp/spark_checkpoints"
SPEED_LAYER_PATH="$CHECKPOINT_BASE/speed_layer"

# =============================================================================
# COLORS
# =============================================================================

GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_step() {
    echo -e "${CYAN}[STEP]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# =============================================================================
# HEADER
# =============================================================================

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}      CYBERSECURITY SPEED LAYER — FULL AUTOMATION           ${NC}"
echo -e "${BLUE}============================================================${NC}"

# =============================================================================
# CHECK REQUIRED CONTAINERS
# =============================================================================

log_step "Checking required containers..."

for c in "$KAFKA_CONTAINER" "$CASSANDRA_CONTAINER" "$HADOOP_MASTER"
do
    if ! docker ps --format '{{.Names}}' | grep -q "^$c$"; then
        log_error "Container '$c' is NOT running."
    fi
done

log_success "All required containers are running."

# =============================================================================
# WAIT FOR CASSANDRA
# =============================================================================

log_step "Waiting for Cassandra..."

MAX_RETRIES=30
COUNT=0

while ! docker exec "$CASSANDRA_CONTAINER" cqlsh -e "DESCRIBE KEYSPACES" > /dev/null 2>&1
do
    if [[ $COUNT -ge $MAX_RETRIES ]]; then
        log_error "Cassandra did not start in time."
    fi

    log_warn "Cassandra not ready yet..."
    sleep 5
    ((COUNT++))
done

log_success "Cassandra is ready."

# =============================================================================
# RESOLVE CASSANDRA INTERNAL IP
# =============================================================================

log_step "Resolving Cassandra Docker IP..."

CASS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CASSANDRA_CONTAINER")

log_success "Cassandra found at $CASS_IP"

# =============================================================================
# CREATE / RESET CASSANDRA SCHEMA
# =============================================================================

log_step "Creating Cassandra schema..."

docker exec -i "$CASSANDRA_CONTAINER" cqlsh <<EOF

DROP KEYSPACE IF EXISTS $CASS_KS;

CREATE KEYSPACE IF NOT EXISTS $CASS_KS
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE $CASS_KS;

DROP TABLE IF EXISTS realtime_threats;
DROP TABLE IF EXISTS brute_force_alerts;
DROP TABLE IF EXISTS data_exfil_alerts;
DROP TABLE IF EXISTS ip_threat_scores;
DROP TABLE IF EXISTS ml_predictions;

CREATE TABLE realtime_threats (
    source_ip TEXT,
    event_time TIMESTAMP,
    dest_ip TEXT,
    protocol TEXT,
    action TEXT,
    threat_label TEXT,
    log_type TEXT,
    bytes_xfer BIGINT,
    user_agent TEXT,
    request_path TEXT,
    attack_type TEXT,
    threat_score FLOAT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    PRIMARY KEY (source_ip, event_time)
)
WITH CLUSTERING ORDER BY (event_time DESC)
AND default_time_to_live = 86400;

CREATE TABLE brute_force_alerts (
    source_ip TEXT,
    window_start TIMESTAMP,
    failed_count INT,
    threat_score FLOAT,
    detected_at TIMESTAMP,
    PRIMARY KEY (source_ip, window_start)
);

CREATE TABLE data_exfil_alerts (
    source_ip TEXT,
    window_start TIMESTAMP,
    total_bytes BIGINT,
    threat_score FLOAT,
    detected_at TIMESTAMP,
    PRIMARY KEY (source_ip, window_start)
)
WITH CLUSTERING ORDER BY (window_start DESC);

CREATE TABLE ip_threat_scores (
    source_ip TEXT PRIMARY KEY,
    threat_score FLOAT,
    attack_count INT,
    last_seen TIMESTAMP,
    is_blocked BOOLEAN,
    attack_types SET<TEXT>
);

CREATE TABLE ml_predictions (
    source_ip text,
    event_time timestamp,
    protocol text,
    action text,
    threat_label text,

    predicted_label text,
    prediction double,

    benign_probability double,
    suspicious_probability double,
    malicious_probability double,

    bytes_xfer bigint,

    PRIMARY KEY (source_ip, event_time)
)
WITH CLUSTERING ORDER BY (event_time DESC);

EOF

log_success "Cassandra schema ready."

# =============================================================================
# CONFIGURE INFLUXDB
# =============================================================================

log_step "Configuring InfluxDB bucket..."

docker exec influxdb influx bucket delete \
    --name speed-bucket \
    --org cyber-org \
    --token $INFLUXDB_TOKEN || true

docker exec influxdb influx bucket create \
    --name speed-bucket \
    --org cyber-org \
    --token $INFLUXDB_TOKEN \
    --description "Bucket for Real-time Speed Layer"

log_success "InfluxDB bucket ready."

# =============================================================================
# CLEAN OLD SPARK JOBS
# =============================================================================

log_step "Cleaning previous Spark jobs..."

docker exec "$HADOOP_MASTER" pkill -f Spark_streaming.py || true
docker exec "$HADOOP_MASTER" pkill -f org.apache.spark.deploy || true

log_success "Old Spark jobs cleaned."

# =============================================================================
# VERIFY HADOOP DAEMONS
# =============================================================================

log_step "Verifying Hadoop daemons..."

for daemon in "NameNode" "ResourceManager"
do
    if docker exec "$HADOOP_MASTER" jps | grep -q "$daemon"; then

        log_success "$daemon is active."

    else

        log_warn "$daemon missing. Attempting restart..."

        if [ "$daemon" == "NameNode" ]; then
            docker exec "$HADOOP_MASTER" "$HADOOP_SBIN/start-dfs.sh"
        else
            docker exec "$HADOOP_MASTER" "$HADOOP_SBIN/start-yarn.sh"
        fi

        sleep 10

        if docker exec "$HADOOP_MASTER" jps | grep -q "$daemon"; then
            log_success "$daemon recovered successfully."
        else
            log_error "Failed to start $daemon"
        fi
    fi
done

# =============================================================================
# LEAVE SAFE MODE
# =============================================================================

log_step "Leaving HDFS safe mode..."

docker exec "$HADOOP_MASTER" hdfs dfsadmin -safemode leave || true

# =============================================================================
# VERIFY DATANODES
# =============================================================================

log_step "Checking HDFS DataNodes..."

DATANODES=$(docker exec "$HADOOP_MASTER" hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}')

if [ -z "$DATANODES" ] || [ "$DATANODES" = "0" ]; then
    log_error "No live DataNodes detected."
else
    log_success "$DATANODES DataNode(s) active."
fi

# =============================================================================
# CLEAN SPARK CHECKPOINTS
# =============================================================================

log_step "Cleaning Spark checkpoints..."

mkdir -p "$SPEED_LAYER_PATH"

rm -rf "$SPEED_LAYER_PATH"/*

docker exec "$HADOOP_MASTER" rm -rf /tmp/spark_checkpoints || true
docker exec "$HADOOP_MASTER" hdfs dfs -rm -r /tmp/spark_checkpoints || true

log_success "Checkpoint cleanup complete."

# =============================================================================
# CREATE KAFKA TOPIC
# =============================================================================

log_step "Creating Kafka topic..."

docker exec "$KAFKA_CONTAINER" kafka-topics \
    --create \
    --if-not-exists \
    --topic "$TOPIC_NAME" \
    --bootstrap-server kafka:9092 \
    --partitions 3 \
    --replication-factor 1

log_success "Kafka topic ready."

# =============================================================================
# VERIFY KAFKA
# =============================================================================

log_step "Verifying Kafka broker..."

docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server kafka:9092 \
    --list

log_success "Kafka broker reachable."

# =============================================================================
# COPY SPARK STREAMING JOB
# =============================================================================

log_step "Copying Spark Streaming job..."

docker cp spark_streaming.py \
"$HADOOP_MASTER":/tmp/Spark_streaming.py

log_success "Spark_streaming.py copied."

# =============================================================================
# INSTALL PYTHON DEPENDENCIES
# =============================================================================

log_step "Installing dependencies..."

docker exec -u root "$HADOOP_MASTER" bash -c "
python3 -m pip install requests  kafka-python cassandra-driver influxdb-client --quiet
"

docker exec -u root "$KAFKA_CONTAINER" bash -c "
apt-get update -y >/dev/null 2>&1 || true
apt-get install -y python3-pip >/dev/null 2>&1 || true
python3 -m pip install kafka-python-ng==2.2.3 --quiet
"

log_success "Dependencies installed."

# =============================================================================
# START SPARK STREAMING
# =============================================================================

log_step "Launching Spark Streaming..."

docker exec -d "$HADOOP_MASTER" bash -c "
spark-submit \
--master local[*] \
--driver-memory 2g \
--executor-memory 2g \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
--conf spark.cassandra.connection.host=$CASS_IP \
/tmp/Spark_streaming.py \
> /tmp/spark_speed_layer.log 2>&1
"

log_step "Waiting for Spark initialization..."

sleep 20

if docker exec "$HADOOP_MASTER" grep -q "Exception" /tmp/spark_speed_layer.log; then
    log_warn "Potential Spark error detected."
else
    log_success "Spark Streaming launched successfully."
fi

# =============================================================================
# COPY PRODUCER FILES
# =============================================================================

log_step "Copying producer files..."

docker cp producer.py \
"$KAFKA_CONTAINER":/tmp/producer.py

docker cp "$CSV_LOCAL_PATH" \
"$KAFKA_CONTAINER":/tmp/streaming_data.csv

log_success "Producer files copied."

# =============================================================================
# START PRODUCER
# =============================================================================

log_step "Starting Kafka producer..."

docker exec -d "$KAFKA_CONTAINER" bash -c "
python3 /tmp/producer.py \
> /tmp/producer.log 2>&1
"

sleep 10

if docker exec "$KAFKA_CONTAINER" ps aux | grep -v grep | grep -q producer.py; then
    log_success "Kafka producer started successfully."
else
    log_error "Kafka producer failed to start."
fi
# =============================================================================
# FINAL
# =============================================================================

echo
log_success "SYSTEM FULLY STARTED"
echo

echo "==================== USEFUL COMMANDS ===================="

echo
echo "Check Spark logs:"
echo "docker exec -it hadoop-master tail -f /tmp/spark_speed_layer.log"

echo
echo "Check Producer logs:"
echo "docker exec -it kafka tail -f /tmp/producer.log"

echo
echo "Check Kafka topics:"
echo "docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list"

echo
echo "Consume Kafka messages:"
echo "docker exec -it kafka kafka-console-consumer \\"
echo "--bootstrap-server kafka:9092 \\"
echo "--topic cybersecurity-logs"

echo
echo "Check Cassandra:"
echo "docker exec -it cassandra cqlsh"

echo
echo "Inside Cassandra:"
echo "USE cybersecurity;"
echo "SELECT * FROM realtime_threats LIMIT 5;"
echo "SELECT * FROM ml_predictions LIMIT 5;"

echo
echo "Check Hadoop logs:"
echo "docker logs -f hadoop-master"

echo
echo "========================================================="
echo "      CYBERSECURITY SPEED LAYER IS RUNNING"
echo "========================================================="
echo
