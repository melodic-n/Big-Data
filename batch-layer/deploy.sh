#!/bin/bash

# ─────────────────────────────────────────────────────────────────────────────
#   SITCN — Deploy & Run on hadoop-master
#   Copies run_batch.sh + batch_base_v2.py into the container, then runs.
# ─────────────────────────────────────────────────────────────────────────────

CONTAINER="hadoop-master"                        # Docker container name
LOCAL_BATCH_SCRIPT="batch-layer/run_batch.sh"              # path on your Ubuntu machine
LOCAL_SPARK_SCRIPT="batch-layer/batch_base_v2.py"          # path on your Ubuntu machine
REMOTE_DIR="/root"                               # destination inside container

echo ""
echo "========================================"
echo "   SITCN — Deploy & Run"
echo "   $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# ── 1. Check both files exist locally ────────────────────────────────────────
echo ""
echo "[1] Vérification des fichiers locaux..."

if [ ! -f "${LOCAL_BATCH_SCRIPT}" ]; then
    echo "[ERREUR] Fichier introuvable : ${LOCAL_BATCH_SCRIPT}"
    exit 1
fi

if [ ! -f "${LOCAL_SPARK_SCRIPT}" ]; then
    echo "[ERREUR] Fichier introuvable : ${LOCAL_SPARK_SCRIPT}"
    exit 1
fi

echo "[OK] run_batch.sh        trouvé"
echo "[OK] batch_base_v2.py    trouvé"

# ── 2. Check the container is running ────────────────────────────────────────
echo ""
echo "[2] Vérification du conteneur Docker '${CONTAINER}'..."

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "[ERREUR] Le conteneur '${CONTAINER}' n'est pas en cours d'exécution."
    echo "         Lancez votre cluster avec : docker-compose up -d"
    exit 1
fi

echo "[OK] Conteneur '${CONTAINER}' actif"

# ── 3. Copy files into the container ─────────────────────────────────────────
echo ""
echo "[3] Copie des fichiers dans ${CONTAINER}:${REMOTE_DIR} ..."

docker cp "${LOCAL_BATCH_SCRIPT}" "${CONTAINER}:${REMOTE_DIR}/run_batch.sh"
if [ $? -ne 0 ]; then
    echo "[ERREUR] Échec de la copie de run_batch.sh"
    exit 1
fi
echo "[OK] run_batch.sh copié"

docker cp "${LOCAL_SPARK_SCRIPT}" "${CONTAINER}:${REMOTE_DIR}/batch_base_v2.py"
if [ $? -ne 0 ]; then
    echo "[ERREUR] Échec de la copie de batch_base_v2.py"
    exit 1
fi
echo "[OK] batch_base_v2.py copié"

# ── 4. Make run_batch.sh executable inside the container ─────────────────────
echo ""
echo "[4] Attribution des permissions d'exécution..."

docker exec "${CONTAINER}" chmod +x "${REMOTE_DIR}/run_batch.sh"
echo "[OK] chmod +x appliqué"

# ── 5. Run the batch script inside the container ─────────────────────────────
echo ""
echo "[5] Lancement de run_batch.sh dans '${CONTAINER}'..."
echo "========================================"
echo ""

docker exec -it "${CONTAINER}" bash "${REMOTE_DIR}/run_batch.sh"

EXIT_CODE=$?

echo ""
echo "========================================"
if [ ${EXIT_CODE} -eq 0 ]; then
    echo "   [OK] Batch terminé avec succès."
else
    echo "   [ERREUR] Batch échoué (code : ${EXIT_CODE})"
fi
echo "   $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""

exit ${EXIT_CODE}