#!/bin/bash
# =============================================================================
# Kafka Pipeline - Orchestrator
# =============================================================================

set -e

# الألوان
BLUE='\033[0;34m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

CONTAINER_NAME="kafka"
TOPIC_NAME="cybersecurity-logs"
HOST_DATA_PATH="../data/cybersecurity_threat_detection_logs_streaming.csv"
HOST_PRODUCER_PATH="./producer.py" # تأكد بلي السكريبت لي فوق سميتو هكا

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}          KAFKA MULTI-THREADED PIPELINE SETUP               ${NC}"
echo -e "${BLUE}============================================================${NC}"

# 1. التحقق من وجود الملفات
if [ ! -f "$HOST_DATA_PATH" ] || [ ! -f "$HOST_PRODUCER_PATH" ]; then
    echo -e "${RED}[ERROR] Required files (CSV or producer.py) are missing!${NC}"
    exit 1
fi

# 2. نقل الملفات لـ Kafka
echo -e "[1/4] Copying files to Kafka container..."
docker cp "$HOST_DATA_PATH" "${CONTAINER_NAME}:/tmp/"
docker cp "$HOST_PRODUCER_PATH" "${CONTAINER_NAME}:/tmp/producer.py"
echo -e "${GREEN}[OK] Files transferred.${NC}"

# 3. تثبيت المكتبة اللازمة (kafka-python خفيفة)
echo -e "[2/4] Ensuring kafka-python is installed..."
docker exec -u root "${CONTAINER_NAME}" pip3 install kafka-python --quiet
echo -e "${GREEN}[OK] Requirements ready.${NC}"

# 4. إنشاء الـ Topic بـ 3 Partitions
echo -e "[3/4] Checking Kafka Topic..."
docker exec "${CONTAINER_NAME}" kafka-topics --create --topic "${TOPIC_NAME}" \
    --bootstrap-server localhost:9092 \
    --partitions 3 --replication-factor 1 --if-not-exists
echo -e "${GREEN}[OK] Topic ready.${NC}"

# 5. تشغيل الـ Producer
echo -e "[4/4] Launching Multi-threaded Stream..."
echo -e "${BLUE}------------------------------------------------------------${NC}"
docker exec -it "${CONTAINER_NAME}" python3 /tmp/producer.py
echo -e "${BLUE}------------------------------------------------------------${NC}"

echo -e "${GREEN}[FINISH] Pipeline task completed.${NC}"
