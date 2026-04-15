import csv
import json
import threading
import time
from kafka import KafkaProducer
import sys
# إعدادات الـ Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8'),
        batch_size=32768,
        linger_ms=20
    )
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    sys.exit(1)
TOPIC_NAME = 'cybersecurity-logs'
# صلحنا المسار هنا:
CSV_PATH = '/tmp/cybersecurity_threat_detection_logs_streaming.csv'
NUM_THREADS = 4
def send_batch(rows, thread_id):
    print(f"[Thread-{thread_id}] Sending {len(rows)} logs...")
    for row in rows:
        ip_key = str(row.get('source_ip', '0.0.0.0'))
        producer.send(TOPIC_NAME, key=ip_key, value=dict(row))
    producer.flush()
    print(f"[Thread-{thread_id}] Done.")
def main():
    try:
        print(f"[*] Opening file: {CSV_PATH}")
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)

        total_rows = len(all_rows)
        chunk_size = total_rows // NUM_THREADS
        threads = []
        print(f"[*] Starting Multi-threaded Producer: {total_rows} total rows.")
        for i in range(NUM_THREADS):
            start = i * chunk_size
            end = (i + 1) * chunk_size if i != NUM_THREADS - 1 else total_rows
            chunk = all_rows[start:end]

            t = threading.Thread(target=send_batch, args=(chunk, i))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        print("[SUCCESS] All logs have been streamed.")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_PATH}")
    except Exception as e:
        print(f"Error: {e}")
if __name__ == "__main__":
    main()
