#!/usr/bin/env python3
# =============================================================================
# split_dataset.py
# Divise le dataset en deux parties :
#   - 75% → cybersecurity_threat_detection_logs.csv        (Batch)
#   - 25% → cybersecurity_threat_detection_logs_streaming.csv (Streaming Kafka)
# Usage : python3 logs/split_dataset.py
# =============================================================================

import csv
import os

# Chemin du dataset original (doit être dans le même dossier que ce script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(SCRIPT_DIR, "cybersecurity_threat_detection_logs.csv")
OUTPUT_BATCH = os.path.join(SCRIPT_DIR, "cybersecurity_threat_detection_logs.csv")
OUTPUT_STREAMING = os.path.join(SCRIPT_DIR, "cybersecurity_threat_detection_logs_streaming.csv")

print("=" * 60)
print("  Dataset Splitter — Batch (75%) + Streaming (25%)")
print("=" * 60)

# Vérifier que le dataset existe
if not os.path.exists(INPUT_FILE):
    print(f"\n[ERREUR] Dataset introuvable : {INPUT_FILE}")
    print("Placez le fichier cybersecurity_threat_detection_logs.csv")
    print(f"dans le dossier : {SCRIPT_DIR}")
    exit(1)

# Lire toutes les lignes
print("\n[1/4] Lecture du dataset...")
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    header = reader.fieldnames
    rows = list(reader)

total = len(rows)
split_index = int(total * 0.75)

print(f"      Total lignes      : {total:,}")
print(f"      Batch (75%)       : {split_index:,} lignes")
print(f"      Streaming (25%)   : {total - split_index:,} lignes")

# Écrire le fichier Batch (75%)
print("\n[2/4] Ecriture fichier Batch (75%)...")
batch_rows = rows[:split_index]
with open(OUTPUT_BATCH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    writer.writerows(batch_rows)
print(f"      Sauvegarde : {OUTPUT_BATCH}")

# Écrire le fichier Streaming (25%)
print("\n[3/4] Ecriture fichier Streaming (25%)...")
streaming_rows = rows[split_index:]
with open(OUTPUT_STREAMING, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=header)
    writer.writeheader()
    writer.writerows(streaming_rows)
print(f"      Sauvegarde : {OUTPUT_STREAMING}")

# Résumé final
print("\n[4/4] Verification...")
batch_size = os.path.getsize(OUTPUT_BATCH) / (1024 * 1024)
streaming_size = os.path.getsize(OUTPUT_STREAMING) / (1024 * 1024)
print(f"      {os.path.basename(OUTPUT_BATCH):<55} {batch_size:.1f} MB")
print(f"      {os.path.basename(OUTPUT_STREAMING):<55} {streaming_size:.1f} MB")

print("\n" + "=" * 60)
print("  TERMINE !")
print("=" * 60)
print(f"\n  Batch     → {OUTPUT_BATCH}")
print(f"  Streaming → {OUTPUT_STREAMING}")
print("\n  Instructions pour les camarades :")
print("  1. Placez le dataset original dans le dossier logs/")
print("  2. Lancez : python3 logs/split_dataset.py")
print("  3. Batch    : utilisez cybersecurity_threat_detection_logs.csv")
print("  4. Streaming: utilisez cybersecurity_threat_detection_logs_streaming.csv")
print("=" * 60)
