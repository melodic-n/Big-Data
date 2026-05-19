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
HDFS_LOGS_PATH="/data/cybersecurity/logs"
LAST_PARTITION_FILE="/root/last_partition.txt"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[1;34m'; YELLOW='\033[1;33m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}         SOC BIG DATA PIPELINE — FULL SETUP & RUN           ${NC}"
echo -e "${BLUE}============================================================${NC}"

# =============================================================================
# 1. Start Core Services
# =============================================================================
log_info "Ensuring HDFS and YARN are running..."
docker exec $HADOOP_MASTER start-dfs.sh  || log_warn "HDFS might be already running"
docker exec $HADOOP_MASTER start-yarn.sh || log_warn "YARN might be already running"
sleep 5

# =============================================================================
# 2. HBase Thrift Server
# =============================================================================
log_info "Starting HBase Thrift Server..."
docker exec -d $HBASE_CONTAINER hbase thrift start -p 9090 || true
sleep 5
log_success "Thrift Server is active on port 9090."

# =============================================================================
# 3. Create HBase Schema (Idempotent)
# =============================================================================
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
create_if_not_exists 'ip_reputation',   'info'
create_if_not_exists 'attack_patterns', 'stats'
create_if_not_exists 'threat_timeline', 'data'
create_if_not_exists 'port_scans',      'info'
create_if_not_exists 'top_ips',         'info'
create_if_not_exists 'volume_analysis', 'info'
exit
HBASE_EOF
log_success "HBase Schema ready."

# =============================================================================
# 4. Sync Dependencies on Master & Workers
# =============================================================================
log_info "Installing Python dependencies (happybase, influxdb-client)..."
DEPS="happybase influxdb-client"
docker exec -i $HADOOP_MASTER pip install $DEPS --quiet
for W in "${WORKERS[@]}"; do
    docker exec -i $W pip install $DEPS --quiet
done
log_success "All nodes synchronized."

# =============================================================================
# 5. Deploy Script
# =============================================================================
log_info "Deploying Spark script to Master..."
if [ ! -f "${SCRIPT_SRC}" ]; then log_error "Source file ${SCRIPT_SRC} not found!"; fi
docker cp "${SCRIPT_SRC}" "${HADOOP_MASTER}:${SCRIPT_DEST}"
log_success "Script deployed to ${HADOOP_MASTER}:${SCRIPT_DEST}"

# =============================================================================
# 6. PARTITION TRACKING — only process new partitions
# =============================================================================
echo -e "${YELLOW}------------------------------------------------------------${NC}"
log_info "Scanning HDFS for available partitions..."

# List all day-level partitions from HDFS (inside the container)
ALL_PARTITIONS=$(docker exec $HADOOP_MASTER \
    hdfs dfs -ls -R "${HDFS_LOGS_PATH}" 2>/dev/null \
    | grep "^d" \
    | awk '{print $NF}' \
    | grep -E "day=[0-9]+$" \
    | sed 's|.*/year=||' \
    | sed 's|/month=|-|' \
    | sed 's|/day=|-|' \
    | sort -t'-' -k1,1n -k2,2n -k3,3n)

if [ -z "${ALL_PARTITIONS}" ]; then
    log_error "No partitions found in ${HDFS_LOGS_PATH}. Aborting."
fi

log_info "Available partitions:"
echo "${ALL_PARTITIONS}" | while read p; do
    Y=$(echo $p | cut -d'-' -f1)
    M=$(echo $p | cut -d'-' -f2)
    D=$(echo $p | cut -d'-' -f3)
    echo "         → year=${Y}/month=${M}/day=${D}"
done

# Read last processed partition (stored inside the container)
log_info "Checking last processed partition..."
LAST_PARTITION=$(docker exec $HADOOP_MASTER bash -c \
    "[ -f ${LAST_PARTITION_FILE} ] && cat ${LAST_PARTITION_FILE} || echo ''")

if [ -n "${LAST_PARTITION}" ]; then
    LY=$(echo $LAST_PARTITION | cut -d'-' -f1)
    LM=$(echo $LAST_PARTITION | cut -d'-' -f2)
    LD=$(echo $LAST_PARTITION | cut -d'-' -f3)
    log_info "Last processed partition: year=${LY}/month=${LM}/day=${LD}"
else
    log_info "No previous run found → Full first run"
fi

# Build list of pending partitions
PENDING_PARTITIONS=""
if [ -z "${LAST_PARTITION}" ]; then
    PENDING_PARTITIONS="${ALL_PARTITIONS}"
else
    FOUND=false
    while read partition; do
        if [ "${FOUND}" = true ]; then
            PENDING_PARTITIONS="${PENDING_PARTITIONS}${partition}"$'\n'
        fi
        if [ "${partition}" = "${LAST_PARTITION}" ]; then
            FOUND=true
        fi
    done <<< "${ALL_PARTITIONS}"
fi

PENDING_PARTITIONS=$(echo "${PENDING_PARTITIONS}" | sed '/^$/d')

if [ -z "${PENDING_PARTITIONS}" ]; then
    log_warn "No new partitions to process. All up to date."
    echo "       Add new data to HDFS and re-run to continue."
    exit 0
fi

NB_PENDING=$(echo "${PENDING_PARTITIONS}" | wc -l)
log_info "${NB_PENDING} partition(s) pending:"
echo "${PENDING_PARTITIONS}" | while read p; do
    Y=$(echo $p | cut -d'-' -f1)
    M=$(echo $p | cut -d'-' -f2)
    D=$(echo $p | cut -d'-' -f3)
    echo "         → year=${Y}/month=${M}/day=${D}"
done

# =============================================================================
# 7. SPARK JOB LOOP — one partition at a time
# =============================================================================
echo -e "${YELLOW}------------------------------------------------------------${NC}"
log_info "Starting Spark job loop..."
echo -e "${YELLOW}------------------------------------------------------------${NC}"

COUNTER=0
TOTAL=${NB_PENDING}

# ✅ -u 3 : lit depuis fd3, isole du stdin de docker exec
while read -u 3 CURRENT_PARTITION; do
    COUNTER=$((COUNTER + 1))
    CY=$(echo $CURRENT_PARTITION | cut -d'-' -f1)
    CM=$(echo $CURRENT_PARTITION | cut -d'-' -f2)
    CD=$(echo $CURRENT_PARTITION | cut -d'-' -f3)
    CURRENT_HDFS_PATH="${HDFS_LOGS_PATH}/year=${CY}/month=${CM}/day=${CD}"

    echo ""
    log_info "[RUN ${COUNTER}/${TOTAL}] Processing year=${CY}/month=${CM}/day=${CD}"
    log_info "            Path: ${CURRENT_HDFS_PATH}"
    echo ""

    docker exec "${HADOOP_MASTER}" spark-submit \
        --master yarn \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 1g \
        --num-executors 2 \
        --executor-cores 1 \
        "${SCRIPT_DEST}" "${CURRENT_HDFS_PATH}" </dev/null

    SPARK_EXIT=$?

    if [ "${SPARK_EXIT}" -eq 0 ]; then
        docker exec $HADOOP_MASTER bash -c "echo '${CURRENT_PARTITION}' > ${LAST_PARTITION_FILE}"
        log_success "Partition done: year=${CY}/month=${CM}/day=${CD}"
        log_success "Checkpoint updated → ${CURRENT_PARTITION}"
    else
        log_error "Spark failed on year=${CY}/month=${CM}/day=${CD} (exit: ${SPARK_EXIT}). Re-run to resume."
    fi

    echo -e "${YELLOW}------------------------------------------------------------${NC}"

done 3<<< "${PENDING_PARTITIONS}"   

# =============================================================================
# 8. Summary
# =============================================================================
echo ""
echo -e "${BLUE}============================================================${NC}"
log_success "ALL PARTITIONS PROCESSED"
echo -e "   Total processed : ${COUNTER}/${TOTAL}"
echo -e "   Finished at     : $(date '+%Y-%m-%d %H:%M:%S')"
echo -e "${BLUE}============================================================${NC}"
