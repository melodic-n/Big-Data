# HBase Setup for Cybersecurity Threat Detection – Complete Guide

This guide explains how to integrate HBase into your Hadoop + Spark architecture as a serving layer for cybersecurity analytics.

---

## Architecture Overview

```
HDFS (raw logs)
      ↓
Spark Batch (incremental processing)
      ↓
HBase Tables (serving layer)
      ↓
Dashboard / Queries
```

### Description
- HDFS stores raw logs
- Spark processes logs
- HBase stores processed data
- Dashboard visualizes results

---

## File 1: Dockerfile.hbase

```dockerfile
FROM dajobe/hbase:latest

USER root

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends openjdk-8-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
```

---

## File 2: docker-compose.yml

```yaml
hbase-master:
  build:
    context: .
    dockerfile: Dockerfile.hbase
  image: hbase-fixed:latest
  container_name: hbase-master
  hostname: hbase-master
  tty: true
  stdin_open: true
  ports:
    - "16000:16000"
    - "16010:16010"
    - "16020:16020"
    - "16030:16030"
    - "2181:2181"
  environment:
    - JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
  volumes:
    - hbase_data:/data/hbase
  depends_on:
    - hadoop-master

volumes:
  hbase_data:
```

---

## File 3: hbase_setup.sh

```bash
#!/bin/bash

echo "
create 'attacks', 'info'
create 'ip_reputation', 'info'
create 'threat_timeline', 'info'
" | hbase shell
```

---

## Commands

```bash
docker-compose build hbase-master
docker-compose up -d
docker exec -it hbase-master bash
chmod +x hbase_setup.sh
./hbase_setup.sh
echo "list" | hbase shell
```
