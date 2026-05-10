#!/bin/bash

# =============================================================================
# CYBERSECURITY BIG DATA PIPELINE LAUNCHER
# =============================================================================

set -e

PROJECT_DIR=~/Big-Data
SPEED_LAYER_DIR=$PROJECT_DIR/speed-layer
ML_DIR=$PROJECT_DIR/ML
VENV_DIR=$PROJECT_DIR/venv

HADOOP_MASTER="hadoop-master"
WORKERS=("hadoop-worker1" "hadoop-worker2")

echo "===================================================="
echo "      SOC BIG DATA PIPELINE AUTO SETUP"
echo "===================================================="

cd $PROJECT_DIR

# =============================================================================
# STEP 0 — CREATE VENV IF MISSING
# =============================================================================

echo "[0/9] Checking Python virtual environment..."

if [ ! -d "$VENV_DIR" ]; then
    echo "[INFO] Creating virtual environment..."

    sudo apt update
    sudo apt install -y python3-venv python3-pip

    python3 -m venv venv
fi

source venv/bin/activate

echo "[INFO] Installing local Python dependencies..."

pip install --upgrade pip

pip install \
    pyspark \
    pandas \
    numpy \
    kafka-python \
    cassandra-driver \
    influxdb-client \
    happybase \
    requests

# =============================================================================
# STEP 1 — VERIFY DOCKER CONTAINERS
# =============================================================================

echo "[1/9] Checking Hadoop containers..."

if ! docker ps | grep -q $HADOOP_MASTER; then
    echo "[ERROR] Hadoop master container is not running"
    exit 1
fi

for W in "${WORKERS[@]}"; do
    if ! docker ps | grep -q $W; then
        echo "[ERROR] Worker container $W is not running"
        exit 1
    fi
done

echo "[OK] Hadoop containers are running."

# =============================================================================
# STEP 2 — START SSH ON WORKERS
# =============================================================================

echo "[2/9] Starting SSH services on workers..."

for W in "${WORKERS[@]}"; do
    docker exec -it $W service ssh start || true
done

sleep 3

echo "[OK] SSH services started."

# =============================================================================
# STEP 3 — START HDFS + YARN
# =============================================================================

echo "[3/9] Starting Hadoop services..."

docker exec -it $HADOOP_MASTER start-dfs.sh || true
sleep 10

docker exec -it $HADOOP_MASTER start-yarn.sh || true
sleep 10

docker exec -it $HADOOP_MASTER hdfs dfsadmin -safemode leave || true

echo "[OK] Hadoop services started."

# =============================================================================
# STEP 4 — VERIFY DATANODES
# =============================================================================

echo "[4/9] Verifying Hadoop cluster health..."

echo ""
echo "================ MASTER JPS ================"
docker exec -it $HADOOP_MASTER jps

echo ""
echo "================ WORKER STATUS ================"

DATANODE_COUNT=0

for W in "${WORKERS[@]}"; do

    echo ""
    echo "----- $W -----"

    docker exec -it $W jps || true

    if docker exec $W jps | grep -q DataNode; then
        echo "[OK] DataNode active on $W"
        DATANODE_COUNT=$((DATANODE_COUNT + 1))
    else
        echo "[WARN] DataNode missing on $W"

        echo "[INFO] Cleaning corrupted DataNode storage..."

        docker exec -it $W rm -rf /tmp/hadoop-root/dfs/data/* || true

        echo "[INFO] Restarting SSH on $W..."

        docker exec -it $W service ssh restart || true
    fi
done

# Restart HDFS after cleanup
if [ "$DATANODE_COUNT" -eq 0 ]; then

    echo "[WARN] No DataNodes active. Restarting HDFS..."

    docker exec -it $HADOOP_MASTER stop-dfs.sh || true
    sleep 5

    docker exec -it $HADOOP_MASTER start-dfs.sh || true
    sleep 15
fi

echo ""
echo "================ HDFS REPORT ================"

docker exec -it $HADOOP_MASTER hdfs dfsadmin -report || true

LIVE_NODES=$(docker exec $HADOOP_MASTER \
    hdfs dfsadmin -report 2>/dev/null \
    | grep "Live datanodes" \
    | awk '{print $3}')

if [ -z "$LIVE_NODES" ] || [ "$LIVE_NODES" -eq 0 ]; then
    echo "[ERROR] HDFS has 0 active DataNodes."
    echo "[ERROR] Spark jobs cannot run safely."
    exit 1
fi

echo "[OK] Hadoop cluster healthy with $LIVE_NODES DataNode(s)."

# =============================================================================
# STEP 5 — INSTALL CONTAINER DEPENDENCIES
# =============================================================================

echo "[5/9] Installing dependencies inside Hadoop containers..."

DEPS="numpy pandas pyspark happybase influxdb-client kafka-python"

docker exec -it $HADOOP_MASTER pip install $DEPS

for W in "${WORKERS[@]}"; do
    docker exec -it $W pip install $DEPS || true
done

echo "[OK] Python dependencies installed."

# =============================================================================
# STEP 6 — COPY TRAIN MODEL SCRIPT
# =============================================================================

echo "[6/9] Copying train-model.py to Hadoop..."

cd $ML_DIR

docker cp train-model.py $HADOOP_MASTER:/tmp/train-model.py

echo "[OK] ML script copied."

# =============================================================================
# STEP 7 — TRAIN MODEL
# =============================================================================

echo "[7/9] Training ML model..."

docker exec -it $HADOOP_MASTER spark-submit /tmp/train-model.py

echo "[OK] Model training completed."

# =============================================================================
# STEP 8 — VERIFY MODEL + START STREAMING
# =============================================================================

echo "[8/9] Verifying HDFS model..."

docker exec -it $HADOOP_MASTER hdfs dfs -ls /models

echo "================ CASSANDRA TABLES ================"

docker exec -it cassandra cqlsh -e "
USE cybersecurity;
DESCRIBE TABLES;
"
echo "================ SPARK LOGS ================"

tail -20 $SPEED_LAYER_DIR/spark.log || true

echo ""
echo "===================================================="
echo "SOC BIG DATA PIPELINE IS RUNNING"
echo "===================================================="
echo ""
echo "Grafana URL: http://YOUR_SERVER_IP:3000"
echo "Login: admin / admin"
echo ""
