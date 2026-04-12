#!/bin/bash
# =============================================================================
# setup_and_run.sh — The Ultimate Bulletproof SOC Pipeline Orchestrator
# =============================================================================

set -e

# Configuration
HBASE_CONTAINER="hbase"
HADOOP_MASTER="hadoop-master"
SCRIPT_SRC="./batch_hbase.py"
SCRIPT_DEST="/tmp/batch_hbase.py"
SHC_JAR_URL="https://repo1.maven.org/maven2/com/hortonworks/shc/shc-core/1.1.0.3.1.5.3-3/shc-core-1.1.0.3.1.5.3-3-tests.jar"
SHC_JAR_LOCAL="/tmp/shc-core.jar"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}        SOC BIG DATA PIPELINE — FULL SETUP & RUN            ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. Check & Start YARN ResourceManager
log_info "Verifying YARN ResourceManager..."
RM_CHECK=$(docker exec $HADOOP_MASTER jps | grep "ResourceManager" || true)
if [ -z "$RM_CHECK" ]; then
    log_warn "ResourceManager is down. Starting YARN..."
    docker exec $HADOOP_MASTER start-yarn.sh
    sleep 5
fi
log_success "YARN is ready."

# 2. Check & Start HBase Thrift Server (Required for Happybase)
log_info "Verifying HBase Thrift Server (Port 9090)..."
THRIFT_CHECK=$(docker exec $HBASE_CONTAINER netstat -tulnp | grep 9090 || true)
if [ -z "$THRIFT_CHECK" ]; then
    log_warn "Thrift Server is down. Starting it..."
    docker exec -d $HBASE_CONTAINER hbase thrift start -p 9090
    sleep 3
fi
log_success "HBase Thrift is ready."

# 3. Create/Reset HBase Tables
log_info "Setting up HBase tables..."
docker exec -i "${HBASE_CONTAINER}" hbase shell << 'HBASE_EOF'
def setup_table(name, cf, versions=1)
  if list.include?(name)
    puts "=> Table #{name} exists, recreating..."
    disable name
    drop name
  end
  create name, { NAME => cf, VERSIONS => versions, BLOOMFILTER => 'ROW' }
end

setup_table 'ip_reputation', 'info'
setup_table 'attack_patterns', 'stats', 5
setup_table 'threat_timeline', 'data'
exit
HBASE_EOF
log_success "HBase tables are clean and ready."

# 4. Prepare Spark Environment
log_info "Copying Spark script to $HADOOP_MASTER..."
docker cp "${SCRIPT_SRC}" "${HADOOP_MASTER}:${SCRIPT_DEST}"

log_info "Checking SHC connector JAR..."
docker exec "${HADOOP_MASTER}" bash -c "
    if [ ! -f ${SHC_JAR_LOCAL} ]; then
        wget -q -O ${SHC_JAR_LOCAL} ${SHC_JAR_URL}
        echo '[JAR] Downloaded SHC connector.'
    fi
"

# 5. Install Python Dependencies on Workers
log_info "Installing happybase on executors..."
# We run this on master and assume shared env or handle it in the script via parallelize
docker exec "${HADOOP_MASTER}" pip3 install happybase --quiet

# 6. Launch Spark Job
echo -e "${BLUE}------------------------------------------------------------${NC}"
log_info "Submitting Spark Job to YARN Cluster..."
docker exec "${HADOOP_MASTER}" bash -c "
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --jars ${SHC_JAR_LOCAL} \
        --conf spark.hbase.host=hbase:2181 \
        --conf spark.executor.memory=2g \
	--conf spark.driver.memory=1g \
        ${SCRIPT_DEST}
"
echo -e "${BLUE}------------------------------------------------------------${NC}"

log_success "Pipeline execution finished!"
echo -e "${YELLOW}Check your HBase Web UI: http://localhost:16010${NC}"
