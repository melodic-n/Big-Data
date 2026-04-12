#!/bin/bash
# =============================================================================
# setup.sh — HDFS Data Ingestion Layer (Final Fixed Version)
# =============================================================================

set -e 

# الألوان
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' 

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}   HDFS Setup — Cybersecurity Threat Detection Logs         ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Configuration
CONTAINER_NAME="hadoop-master"
CSV_FILENAME="cybersecurity_threat_detection_logs.csv"
HDFS_RAW_PATH="/data/cybersecurity/raw"

# 2. Check Hadoop Status
log_info "Checking Hadoop status on ${CONTAINER_NAME}..."
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_error "Container '${CONTAINER_NAME}' is not running. Run 'docker-compose up -d' first."
fi

# التأكد من jps بلا ما يوقف السكريبت إلا كانت خاوية
CHECK_JPS=$(docker exec $CONTAINER_NAME jps 2>/dev/null | grep -E "NameNode|ResourceManager" || true)

if [ -z "$CHECK_JPS" ]; then
    log_warn "Hadoop is NOT running. Launching start-hadoop.sh..."
    # تأكد من المسار الصحيح (غالبا /scripts/ داخل hadoop-master)
    docker exec $CONTAINER_NAME /scripts/start-hadoop.sh || docker exec $CONTAINER_NAME ./start-hadoop.sh
    log_info "Waiting for NameNode to exit Safe Mode..."
    sleep 10
else
    log_success "Hadoop is already active."
fi

# 3. Locate the CSV file (صلحت ليك المشكل ديال ~)
log_info "Locating dataset file..."
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

if [ -z "$CSV_PATH" ]; then
    log_warn "Dataset not found in standard paths."
    echo -e "${YELLOW}[?] Please enter the full path to the CSV file:${NC}"
    read -r CSV_PATH
    # تنظيف المسار إلا المستعمل دخل ~ بيديو
    CSV_PATH="${CSV_PATH/#\~/$HOME}"
fi

if [ ! -f "$CSV_PATH" ]; then
    log_error "File not found at: $CSV_PATH"
fi
log_success "Using dataset: $CSV_PATH"

# 4. Create HDFS Directories
log_info "Creating HDFS directory structure..."
docker exec "${CONTAINER_NAME}" hdfs dfs -mkdir -p "${HDFS_RAW_PATH}"
docker exec "${CONTAINER_NAME}" hdfs dfs -mkdir -p "/data/cybersecurity/logs/year=2023/month=10/day=15"
log_success "HDFS directories created."

# 5. Transfer Data
log_info "Copying dataset to container..."
docker cp "$CSV_PATH" "${CONTAINER_NAME}:/tmp/${CSV_FILENAME}"

log_info "Uploading from container to HDFS..."
docker exec "${CONTAINER_NAME}" hdfs dfs -put -f "/tmp/${CSV_FILENAME}" "${HDFS_RAW_PATH}/${CSV_FILENAME}"
log_success "Data ingestion complete."

# 6. Verification
echo ""
log_info "Verification — HDFS structure:"
echo "------------------------------------------------------------"
docker exec "${CONTAINER_NAME}" hdfs dfs -ls -R /data/cybersecurity/ | awk '{print $8}' || echo "No files found."
echo "------------------------------------------------------------"

log_success "HDFS setup completed successfully!"
