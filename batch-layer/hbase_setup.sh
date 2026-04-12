#!/bin/bash
# =============================================================================
# hbase_setup.sh — Create HBase tables for the Batch Layer
# =============================================================================

CONTAINER="hbase"

RED='\033[0;31m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; NC='\033[0m'
log_info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $1"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

echo ""
echo "============================================================"
echo "  HBase Setup — Cybersecurity Threat Detection Tables"
echo "============================================================"
echo ""

# ── 1. Wait for HBase shell to respond ───────────────────────────────────────
log_info "Waiting for HBase shell to be ready..."

for i in $(seq 1 5); do
    # Pipe 'list' command into hbase shell
    RESULT=$(echo "list" | docker exec -i "${CONTAINER}" hbase shell 2>&1)
    if echo "${RESULT}" | grep -q "row(s)"; then
        log_success "HBase shell is ready."
        break
    fi
    echo "  Attempt $i/30 — waiting 5s..."
    sleep 5
    if [ "$i" -eq 30 ]; then
        log_error "HBase shell not responding. Run: docker logs ${CONTAINER}"
    fi
done

# ── 2. Create tables ──────────────────────────────────────────────────────────
log_info "Creating HBase tables..."

# Use pipe for each command to avoid syntax issues
docker exec -i "${CONTAINER}" hbase shell << 'HBASE_EOF'
exists? 'ip_reputation' do |v|
  if v
    disable 'ip_reputation'
    drop 'ip_reputation'
  end
end
create 'ip_reputation', { NAME => 'info', VERSIONS => 1, BLOOMFILTER => 'ROW' }
puts "=> ip_reputation created"

exists? 'attack_patterns' do |v|
  if v
    disable 'attack_patterns'
    drop 'attack_patterns'
  end
end
create 'attack_patterns', { NAME => 'stats', VERSIONS => 5, BLOOMFILTER => 'ROW' }
puts "=> attack_patterns created"

exists? 'threat_timeline' do |v|
  if v
    disable 'threat_timeline'
    drop 'threat_timeline'
  end
end
create 'threat_timeline', { NAME => 'data', VERSIONS => 1, BLOOMFILTER => 'ROW' }
puts "=> threat_timeline created"

list
exit
HBASE_EOF

log_success "All HBase tables created."

# ── 3. Verify ─────────────────────────────────────────────────────────────────
echo ""
log_info "Verifying table schemas..."

docker exec -i "${CONTAINER}" hbase shell << 'VERIFY_EOF'
describe 'ip_reputation'
describe 'attack_patterns'
describe 'threat_timeline'
exit
VERIFY_EOF

echo ""
log_success "HBase setup complete!"
echo ""
echo "============================================================"
echo "  Tables:"
echo "    ip_reputation   — CF: info"
echo "    attack_patterns — CF: stats"
echo "    threat_timeline — CF: data"
echo "  HBase Web UI : http://localhost:16010"
echo "  ZooKeeper    : hbase-master:2181"
echo "============================================================"
echo ""
