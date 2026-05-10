#!/bin/bash
# =============================================================================
# setup.sh — The Bulletproof Data Ingest & Partitioning Engine
# =============================================================================

set -e

# --- Configuration ---
CONTAINER_NAME="hadoop-master"
CSV_FILENAME="cybersecurity_threat_detection_logs.csv"
HDFS_LOGS_BASE="/data/cybersecurity/logs"
PARTITION_SCRIPT="partition_logs.py"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}      CYBERSECURITY LOGS — INGESTION & PARTITIONING         ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Check Container Health
log_info "Verifying Hadoop Container..."
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    log_error "Container $CONTAINER_NAME is not running. Please start your docker-compose."
fi

# 2. Check Hadoop Daemons
log_info "Checking HDFS Status..."
HDFS_READY=$(docker exec $CONTAINER_NAME jps | grep "NameNode" || true)
if [ -z "$HDFS_READY" ]; then
    log_warn "HDFS is down. Starting services..."
    docker exec $CONTAINER_NAME start-dfs.sh
    sleep 10
fi

# Force Leave Safe Mode
docker exec $CONTAINER_NAME hdfs dfsadmin -safemode leave || true

# 3. Create Base Directory
log_info "Preparing HDFS structure..."
docker exec $CONTAINER_NAME hdfs dfs -mkdir -p $HDFS_LOGS_BASE

log_info "Cleaning up stale processes..."
pkill -f partition_logs.py || true
pkill -f "docker exec" || true
sudo chown -R $(whoami) /tmp

# 4. Run the Partitioning Script (The Core Step)
log_info "Launching Parallel Partitioning Engine..."
if [ ! -f "$PARTITION_SCRIPT" ]; then
    log_error "$PARTITION_SCRIPT not found in current directory!"
fi

# We run it on the HOST because your script uses 'docker exec' internally
# Make sure python3 is installed on your Ubuntu Host
python3 $PARTITION_SCRIPT

# 5. Final Verification
echo -e "${BLUE}------------------------------------------------------------${NC}"
log_info "Verifying Created Partitions in HDFS:"
docker exec $CONTAINER_NAME hdfs dfs -ls -R $HDFS_LOGS_BASE | grep "year=" | head -n 10
echo "... (showing first 10 partitions)"
echo -e "${BLUE}------------------------------------------------------------${NC}"

log_success "Data Ingestion & Partitioning finished successfully!"
