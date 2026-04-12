#!/bin/bash
# =============================================================================
# run_batch.sh — Submit the Spark batch job with HBase connector
# =============================================================================
# The Spark-HBase connector (SHC) JAR must be available inside hadoop-master.
# It is downloaded automatically on first run into /tmp/shc.jar
# =============================================================================

set -e
CONTAINER="hadoop-master"
SCRIPT_SRC="./batch_hbase.py"
SCRIPT_DEST="/tmp/batch_hbase.py"

# SHC connector — pre-built fat JAR compatible with Spark 2/3 + HBase 2.x
SHC_JAR_URL="https://repo1.maven.org/maven2/com/hortonworks/shc/shc-core/1.1.0.3.1.5.3-3/shc-core-1.1.0.3.1.5.3-3-tests.jar"
SHC_JAR_LOCAL="/tmp/shc-core.jar"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }

echo ""
echo "============================================================"
echo "  Spark Batch — HBase Write"
echo "============================================================"

# 1. Copy Python script into container
log_info "Copying batch script into container..."
docker cp "${SCRIPT_SRC}" "${CONTAINER}:${SCRIPT_DEST}"
log_success "Script copied."

# 2. Download SHC JAR inside container if not present
log_info "Checking SHC connector JAR..."
docker exec "${CONTAINER}" bash -c "
    if [ ! -f ${SHC_JAR_LOCAL} ]; then
        echo '[JAR] Downloading SHC connector...'
        wget -q -O ${SHC_JAR_LOCAL} ${SHC_JAR_URL}
        echo '[JAR] Downloaded.'
    else
        echo '[JAR] Already present.'
    fi
"
log_success "SHC JAR ready."

# -----------------------------------------------------------------------------
# PRE-CHECK: Ensure YARN ResourceManager is running
# -----------------------------------------------------------------------------
echo -e "\033[0;34m[CHECK] Verifying YARN ResourceManager status...\033[0m"

# Check if ResourceManager is in the JPS list
RM_STATUS=$(docker exec hadoop-master jps | grep "ResourceManager" || true)

if [ -z "$RM_STATUS" ]; then
    echo -e "\033[1;33m[WARN] ResourceManager is NOT running. Attempting to start YARN...\033[0m"
    
    # Start YARN
    docker exec hadoop-master start-yarn.sh
    
    # Wait a few seconds for RM to initialize
    echo -ne "\033[0;34m[WAIT] Waiting for ResourceManager to wake up...\033[0m"
    for i in {1..15}; do
        sleep 1
        echo -ne "."
        RM_CHECK=$(docker exec hadoop-master jps | grep "ResourceManager" || true)
        if [ ! -z "$RM_CHECK" ]; then
            echo -e "\n\033[0;32m[OK] ResourceManager is now UP!\033[0m"
            break
        fi
        if [ $i -eq 15 ]; then
            echo -e "\n\033[0;31m[ERROR] ResourceManager failed to start after 15s. Check Hadoop logs.\033[0m"
            exit 1
        fi
    done
else
    echo -e "\033[0;32m[OK] ResourceManager is already running.\033[0m"
fi

# 3. Submit Spark job
log_info "Submitting Spark job..."
docker exec "${CONTAINER}" bash -c "
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --jars ${SHC_JAR_LOCAL} \
        --conf spark.hbase.host=hbase:2181 \
        --conf spark.executor.memory=2g \
        --conf spark.driver.memory=1g \
        ${SCRIPT_DEST}
"

log_success "Spark batch job completed."
echo ""
echo "============================================================"
echo "  Verify results in HBase:"
echo ""
echo "  docker exec -it hbase hbase shell"
echo "  hbase> scan 'ip_reputation',   {LIMIT => 5}"
echo "  hbase> scan 'attack_patterns', {LIMIT => 5}"
echo "  hbase> scan 'threat_timeline', {LIMIT => 5}"
echo "============================================================"
echo ""
