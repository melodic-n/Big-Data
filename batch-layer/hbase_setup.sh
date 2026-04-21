#!/bin/bash
# =============================================================================
# hbase_setup.sh — Safe HBase Table Initialization
# =============================================================================

CONTAINER="hbase"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo "============================================================"
echo "  HBase Setup — Cybersecurity Tables"
echo "============================================================"
echo ""

# ── Wait for HBase ─────────────────────────────────────────
log_info "Checking HBase shell..."

for i in $(seq 1 30); do
    RESULT=$(echo "status" | docker exec -i "${CONTAINER}" hbase shell 2>/dev/null)
    if echo "${RESULT}" | grep -q "servers"; then
        log_success "HBase is ready."
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 5
done

# ── Function to recreate table safely ──────────────────────
create_table() {
    TABLE=$1
    CF=$2

    docker exec -i "${CONTAINER}" hbase shell <<EOF
disable '$TABLE'
drop '$TABLE'
EOF

    docker exec -i "${CONTAINER}" hbase shell <<EOF
create '$TABLE', { NAME => '$CF', VERSIONS => 1, BLOOMFILTER => 'ROW' }
EOF

    echo "=> $TABLE created"
}

# ── Create tables ──────────────────────────────────────────
log_info "Creating tables..."

create_table "ip_reputation" "info"
create_table "attack_patterns" "stats"
create_table "threat_timeline" "data"
create_table "top_ips" "info"
create_table "port_scans" "info"
create_table "volume_analysis" "info"

# ── Verify ─────────────────────────────────────────────────
log_info "Listing tables..."
docker exec -i "${CONTAINER}" hbase shell <<EOF
list
exit
EOF

log_success "HBase setup completed successfully!"

echo ""
echo "============================================================"
echo "Tables ready:"
echo "  ip_reputation"
echo "  attack_patterns"
echo "  threat_timeline"
echo "  top_ips"
echo "  port_scans"
echo "  volume_analysis"
echo ""
echo "UI: http://localhost:16010"
echo "============================================================"