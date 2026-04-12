#!/bin/bash
# =============================================================================
# setup.sh — HDFS Data Ingestion Layer (Production Ready)
# =============================================================================

set -e 

# --- Color Definitions ---
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m' 

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}   HDFS Setup — Cybersecurity Threat Detection Logs         ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Configuration Constants
CONTAINER_NAME="hadoop-master"
CSV_FILENAME="cybersecurity_threat_detection_logs.csv"
HDFS_RAW_PATH="/data/cybersecurity/raw"

# 2. Check Container and Hadoop Daemon Status
log_info "Verifying if '${CONTAINER_NAME}' container is running..."
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_error "Container '${CONTAINER_NAME}' is not running. Run 'docker-compose up -d' first."
fi

# Search for NameNode and ResourceManager in JPS
CHECK_JPS=$(docker exec $CONTAINER_NAME jps 2>/dev/null | grep -E "NameNode|ResourceManager" || true)

if [ -z "$CHECK_JPS" ]; then
    log_warn "Hadoop daemons are NOT running. Initializing start-hadoop.sh..."
    docker exec $CONTAINER_NAME ./start-hadoop.sh || log_error "Failed to execute start-hadoop.sh"
    
    # Wait for NameNode to initialize and listen on port 9000
    log_info "Waiting 15s for NameNode to initialize..."
    sleep 15
else
    log_success "Hadoop daemons are already active."
fi

# --- BULLETPROOF ADDITION: Force Leave Safe Mode ---
log_info "Ensuring HDFS is out of Safe Mode..."
docker exec $CONTAINER_NAME hdfs dfsadmin -safemode leave || true

# 3. Locate the Dataset File
log_info "Searching for dataset file: ${CSV_FILENAME}"
CSV_PATH=""
SEARCH_PATHS=(
    "$HOME/GuardLogs/data/${CSV_FILENAME}" 
    "./data/${CSV_FILENAME}" 
    "./${CSV_FILENAME}"
    "$HOME/Downloads/${CSV_FILENAME}"
)

for path in "${SEARCH_PATHS[@]}"; do
    if [ -f "$path" ]; then 
        CSV_PATH="$path"
        break 
    fi
done

# If not found, prompt user for manual path
if [ -z "$CSV_PATH" ]; then
    log_warn "Dataset not found in standard paths."
    echo -e "${YELLOW}[?] Please enter the full path to the CSV file:${NC}"
    read -r CSV_PATH
    # Clean up tilde (~) if entered by user
    CSV_PATH="${CSV_PATH/#\~/$HOME}"
fi

if [ ! -f "$CSV_PATH" ]; then
    log_error "File not found at: $CSV_PATH"
fi
log_success "Using dataset located at: $CSV_PATH"

# 4. Create HDFS Directory Structure
log_info "Creating HDFS directory structure..."
# Using -p to avoid 'Already Exists' errors
docker exec "${CONTAINER_NAME}" hdfs dfs -mkdir -p "${HDFS_RAW_PATH}"
docker exec "${CONTAINER_NAME}" hdfs dfs -mkdir -p "/data/cybersecurity/logs/year=2023/month=10/day=15"
log_success "HDFS directories are ready."

# 5. Data Transfer (Local -> Container -> HDFS)
log_info "Copying dataset from Host to Container..."
docker cp "$CSV_PATH" "${CONTAINER_NAME}:/tmp/${CSV_FILENAME}"

log_info "Uploading from Container to HDFS..."
# Using -f (force) to overwrite if file already exists
docker exec "${CONTAINER_NAME}" hdfs dfs -put -f "/tmp/${CSV_FILENAME}" "${HDFS_RAW_PATH}/${CSV_FILENAME}"
log_success "Data ingestion to HDFS completed."

# 6. Verification & Listing
echo ""
log_info "Verifying HDFS structure:"
echo "------------------------------------------------------------"
docker exec "${CONTAINER_NAME}" hdfs dfs -ls -R /data/cybersecurity/ | awk '{print $8}' || echo "No files found."
echo "------------------------------------------------------------"

log_success "HDFS setup completed successfully!"

