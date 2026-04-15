#!/bin/bash
# =============================================================================
# setup_speed.sh — Speed Layer Full Orchestrator (Bulletproof)
# Kafka + Cassandra + Spark Streaming
# =============================================================================

set -e

# --- Colors ---
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# --- Config ---
KAFKA_CONTAINER="kafka"
CASSANDRA_CONTAINER="cassandra"
TOPIC_NAME="cybersecurity-logs"
CASS_KS="cybersecurity"
SPARK_LOG="/tmp/spark_speed_layer.log"
SPARK_PID_FILE="/tmp/spark_speed.pid"

echo -e "${BLUE}${BOLD}============================================================${NC}"
echo -e "${BLUE}${BOLD}   CYBERSECURITY SPEED LAYER — ALL-IN-ONE ORCHESTRATOR      ${NC}"
echo -e "${BLUE}${BOLD}============================================================${NC}"

# 1. Health Check
log_info "Checking Docker containers..."
for c in "$KAFKA_CONTAINER" "$CASSANDRA_CONTAINER"; do
    if ! docker ps --format '{{.Names}}' | grep -q "^$c$"; then
        log_error "Container '$c' is not running. Check 'docker-compose up -d'."
    fi
done
log_success "All containers are running."

# 2. Wait for Cassandra (Critical)
log_info "Waiting for Cassandra to be ready (this can take up to 2 mins)..."
MAX_RETRIES=24; COUNT=0
while ! docker exec "$CASSANDRA_CONTAINER" cqlsh -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
    [[ $COUNT -eq $MAX_RETRIES ]] && log_error "Cassandra timeout."
    log_warn "Still waiting for Cassandra... ($((COUNT*5))s)"
    sleep 5; ((COUNT++))
done
log_success "Cassandra is UP and listening."

# 3. Create Cassandra Schema
log_info "Injecting CQL Schema..."
docker exec -i "$CASSANDRA_CONTAINER" cqlsh <<EOF
CREATE KEYSPACE IF NOT EXISTS $CASS_KS WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE $CASS_KS;

CREATE TABLE IF NOT EXISTS realtime_threats (
    source_ip TEXT, event_time TIMESTAMP, dest_ip TEXT, protocol TEXT, action TEXT, 
    threat_label TEXT, log_type TEXT, bytes_xfer BIGINT, user_agent TEXT, request_path TEXT,
    attack_type TEXT, threat_score FLOAT, window_start TIMESTAMP, window_end TIMESTAMP,
    PRIMARY KEY (source_ip, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC) AND default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS brute_force_alerts (
    source_ip TEXT, window_start TIMESTAMP, failed_count INT, 
    threat_score FLOAT, detected_at TIMESTAMP,
    PRIMARY KEY (source_ip, window_start)
);

CREATE TABLE IF NOT EXISTS ip_threat_scores (
    source_ip TEXT PRIMARY KEY, threat_score FLOAT, attack_count INT, 
    last_seen TIMESTAMP, is_blocked BOOLEAN
);
EOF
log_success "Cassandra tables created."

# 4. Kafka Topic Setup
log_info "Setting up Kafka topic..."
docker exec "$KAFKA_CONTAINER" kafka-topics --create --topic "$TOPIC_NAME" \
    --bootstrap-server localhost:9092 --partitions 3 --if-not-exists || true
log_success "Topic '$TOPIC_NAME' is ready."

# 5. Resolve Cassandra Internal IP
CASS_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$CASSANDRA_CONTAINER")
log_info "Cassandra Internal IP: $CASS_IP"

# 6. Launch Spark Streaming (Background)
log_info "Launching Spark Streaming job..."
if [[ -f "$SPARK_PID_FILE" ]]; then
    kill -9 $(cat "$SPARK_PID_FILE") 2>/dev/null || true
fi

# We use nohup to keep Spark running in background
nohup spark-submit \
    --master "local[2]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf "spark.driver.extraJavaOptions=-Djava.net.preferIPv4Stack=true" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
    --conf "spark.cassandra.connection.host=$CASS_IP" \
    Spark_streaming.py > "$SPARK_LOG" 2>&1 &

echo $! > "$SPARK_PID_FILE"
log_success "Spark job started (PID: $(cat $SPARK_PID_FILE)). Logs: tail -f $SPARK_LOG"

# --- Step 10: Start Producer INSIDE Container ---
log_info "Preparing Producer inside Kafka container..."

# 1. Copy files to Container
docker cp producer.py "$KAFKA_CONTAINER":/tmp/producer.py
docker cp ../data/cybersecurity_threat_detection_logs_streaming.csv "$KAFKA_CONTAINER":/tmp/data.csv

# 2. Install pip if missing and then install kafka-python-ng
log_info "Installing dependencies inside container..."
docker exec -u root "$KAFKA_CONTAINER" bash -c "
    if ! command -v pip &> /dev/null && ! command -v pip3 &> /dev/null; then
        echo 'Installing pip...'
        apt-get update && apt-get install -y python3-pip
    fi
    python3 -m pip install kafka-python-ng --quiet
"
log_success "Dependencies ready."
# 3. Run Producer
log_info "Running Producer..."
docker exec -it "$KAFKA_CONTAINER" python3 /tmp/producer.py
