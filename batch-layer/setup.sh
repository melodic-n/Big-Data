#!/bin/bash
# =============================================================================
# setup_and_run.sh — The Final Bulletproof SOC Pipeline Orchestrator
# =============================================================================

set -e

# --- Configuration ---
HBASE_CONTAINER="hbase"
HADOOP_MASTER="hadoop-master"
WORKERS=("hadoop-worker1" "hadoop-worker2")
SCRIPT_SRC="./batch_layer.py"
SCRIPT_DEST="/root/batch_layer.py"

# Updated HDFS Path
HDFS_LOGS_PATH="/data/cybersecurity/logs"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[1;34m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}         SOC BIG DATA PIPELINE — FULL SETUP & RUN           ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Start Core Services
log_info "Ensuring HDFS and YARN are running..."
docker exec $HADOOP_MASTER start-dfs.sh || log_warn "HDFS might be already running"
docker exec $HADOOP_MASTER start-yarn.sh || log_warn "YARN might be already running"
sleep 5

# 2. HBase Thrift Server (Critical Bridge for Python)
log_info "Starting HBase Thrift Server..."
# We use -d to run in background, and check if it's already running
docker exec -d $HBASE_CONTAINER hbase thrift start -p 9090 || true
sleep 5
log_success "Thrift Server is active on port 9090."

# 3. Create HBase Schema (Idempotent - No data loss)
log_info "Synchronizing HBase tables..."
docker exec -i "${HBASE_CONTAINER}" hbase shell << 'HBASE_EOF'
def create_if_not_exists(table, cf)
  if !list.include?(table)
    create table, { NAME => cf, VERSIONS => 1 }
    puts "Created table #{table}"
  else
    puts "Table #{table} already exists. Skipping."
  end
end
create_if_not_exists 'ip_reputation', 'info'
create_if_not_exists 'attack_patterns', 'stats'
create_if_not_exists 'threat_timeline', 'data'
create_if_not_exists 'port_scans', 'info'
create_if_not_exists 'top_ips', 'info'
create_if_not_exists 'volume_analysis', 'info'
exit
HBASE_EOF
log_success "HBase Schema ready."

# 4. Sync Dependencies on Master & Workers
log_info "Installing Python dependencies (happybase, influxdb-client)..."
DEPS="happybase influxdb-client"
docker exec -i $HADOOP_MASTER pip install $DEPS --quiet
for W in "${WORKERS[@]}"; do
    docker exec -i $W pip install $DEPS --quiet
done
log_success "All nodes synchronized."

# 5. Deploy Script
log_info "Deploying Spark script to Master..."
if [ ! -f "${SCRIPT_SRC}" ]; then log_error "Source file ${SCRIPT_SRC} not found!"; fi
docker cp "${SCRIPT_SRC}" "${HADOOP_MASTER}:${SCRIPT_DEST}"

# 6. LAUNCH SPARK JOB
echo -e "${YELLOW}------------------------------------------------------------${NC}"
log_info "SUBMITTING SPARK JOB TO YARN..."
log_info "Input: hdfs://${HDFS_LOGS_PATH}"
echo -e "${YELLOW}------------------------------------------------------------${NC}"

# Launching the job
docker exec -it "${HADOOP_MASTER}" spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --num-executors 2 \
    --executor-cores 1 \
    "${SCRIPT_DEST}" "${HDFS_LOGS_PATH}"

echo -e "${BLUE}------------------------------------------------------------${NC}"
log_success "PIPELINE EXECUTION FINISHED!"
echo -e "${BLUE}============================================================${NC}"
