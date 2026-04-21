#!/bin/bash

HDFS_LOGS_DIR="/data/cybersecurity/logs"
LAST_PARTITION_FILE="/root/last_partition.txt"
SPARK_SCRIPT="/root/batch_base_v2.py"

echo ""
echo "========================================"
echo "   SITCN — Auto Batch Runner"
echo "   $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# ─────────────────────────────────────────────────────────────────────────────
# 1. LISTER TOUTES LES PARTITIONS HDFS (dossiers uniquement)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "[1] Lecture des partitions HDFS disponibles..."

ALL_PARTITIONS=$(hdfs dfs -ls -R "${HDFS_LOGS_DIR}" 2>/dev/null \
    | grep "^d" \
    | awk '{print $NF}' \
    | grep -E "day=[0-9]+$" \
    | sed 's|.*/year=||' \
    | sed 's|/month=|-|' \
    | sed 's|/day=|-|' \
    | sort -t'-' -k1,1n -k2,2n -k3,3n)

if [ -z "${ALL_PARTITIONS}" ]; then
    echo "[ERREUR] Aucune partition trouvée dans ${HDFS_LOGS_DIR}"
    exit 1
fi

echo "[INFO] Partitions disponibles :"
echo "${ALL_PARTITIONS}" | while read p; do
    Y=$(echo $p | cut -d'-' -f1)
    M=$(echo $p | cut -d'-' -f2)
    D=$(echo $p | cut -d'-' -f3)
    echo "         → year=${Y}/month=${M}/day=${D}"
done

# ─────────────────────────────────────────────────────────────────────────────
# 2. LIRE LA DERNIÈRE PARTITION TRAITÉE
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "[2] Vérification de la dernière partition traitée..."

if [ -f "${LAST_PARTITION_FILE}" ]; then
    LAST_PARTITION=$(cat "${LAST_PARTITION_FILE}")
    LY=$(echo $LAST_PARTITION | cut -d'-' -f1)
    LM=$(echo $LAST_PARTITION | cut -d'-' -f2)
    LD=$(echo $LAST_PARTITION | cut -d'-' -f3)
    echo "[INFO] Dernière partition traitée : year=${LY}/month=${LM}/day=${LD}"
else
    LAST_PARTITION=""
    echo "[INFO] Aucune partition traitée → Premier run complet"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. CONSTRUIRE LA LISTE DES PARTITIONS EN ATTENTE
#    Toutes les partitions APRÈS last_partition
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "[3] Calcul des partitions en attente..."

PENDING_PARTITIONS=""

if [ -z "${LAST_PARTITION}" ]; then
    # Premier run → toutes les partitions sont en attente
    PENDING_PARTITIONS="${ALL_PARTITIONS}"
else
    # Garder uniquement les partitions après LAST_PARTITION
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

# Supprimer la dernière ligne vide
PENDING_PARTITIONS=$(echo "${PENDING_PARTITIONS}" | sed '/^$/d')

if [ -z "${PENDING_PARTITIONS}" ]; then
    echo "[INFO] Aucune nouvelle partition à traiter."
    echo "       Ajoutez un nouveau dossier dans HDFS pour continuer."
    exit 0
fi

# Compter les partitions en attente
NB_PENDING=$(echo "${PENDING_PARTITIONS}" | wc -l)
echo "[INFO] ${NB_PENDING} partition(s) en attente de traitement :"
echo "${PENDING_PARTITIONS}" | while read p; do
    Y=$(echo $p | cut -d'-' -f1)
    M=$(echo $p | cut -d'-' -f2)
    D=$(echo $p | cut -d'-' -f3)
    echo "         → year=${Y}/month=${M}/day=${D}"
done

# ─────────────────────────────────────────────────────────────────────────────
# 4. BOUCLE — traiter toutes les partitions en attente une par une
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "[4] Début du traitement en boucle..."
echo "------------------------------------"

COUNTER=0
TOTAL=${NB_PENDING}

while read CURRENT_PARTITION; do

    COUNTER=$((COUNTER + 1))
    CY=$(echo $CURRENT_PARTITION | cut -d'-' -f1)
    CM=$(echo $CURRENT_PARTITION | cut -d'-' -f2)
    CD=$(echo $CURRENT_PARTITION | cut -d'-' -f3)
    CURRENT_HDFS_PATH="${HDFS_LOGS_DIR}/year=${CY}/month=${CM}/day=${CD}"

    echo ""
    echo "[RUN ${COUNTER}/${TOTAL}] Traitement de year=${CY}/month=${CM}/day=${CD}"
    echo "            Chemin : ${CURRENT_HDFS_PATH}"
    echo ""

    # Lancer Spark pour cette partition
    spark-submit \
    --master local[*] \
    --deploy-mode client \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    --driver-memory 1g \
    "${SPARK_SCRIPT}" "${CURRENT_HDFS_PATH}"

    SPARK_EXIT=$?

    if [ "${SPARK_EXIT}" -eq 0 ]; then
        # Sauvegarder cette partition comme traitée
        echo "${CURRENT_PARTITION}" > "${LAST_PARTITION_FILE}"
        echo "[OK] Partition traitée : year=${CY}/month=${CM}/day=${CD}"
        echo "[OK] Marqueur mis à jour → ${CURRENT_PARTITION}"
    else
        echo "[ERREUR] Échec sur year=${CY}/month=${CM}/day=${CD} (code : ${SPARK_EXIT})"
        echo "[ERREUR] Arrêt de la boucle — les partitions suivantes ne seront pas traitées."
        echo "[ERREUR] Relancez le script pour reprendre depuis cette partition."
        exit 1
    fi

    echo "------------------------------------"

done <<< "${PENDING_PARTITIONS}"

# ─────────────────────────────────────────────────────────────────────────────
# 5. RÉSUMÉ FINAL
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "========================================"
echo "   Toutes les partitions traitées"
echo "   Total traité  : ${COUNTER}/${TOTAL}"
echo "   Dernier run   : $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
echo ""