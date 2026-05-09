import csv
import json
import time
import sys
from kafka import KafkaProducer

# ===================== CONFIGURATION =====================
KAFKA_SERVER = 'kafka:29092' 
TOPIC_NAME   = 'cybersecurity-logs'
CSV_PATH     = '/tmp/streaming_data.csv'
DELAY = 0.1 

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )
    print(f"[*] Connected to Kafka at {KAFKA_SERVER}")
except Exception as e:
    print(f"[ERROR] Kafka Connection: {e}")
    sys.exit(1)

def start_streaming():
    print(f"[*] Starting to send logs from {CSV_PATH}...")
    sent_count = 0
    
    try:
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ip_key = str(row.get('source_ip', '0.0.0.0'))
                producer.send(TOPIC_NAME, key=ip_key, value=dict(row))
                
                sent_count += 1
  
                if sent_count % 100 == 0:
                    print(f"[PROGRESS] Sent {sent_count} rows...")
                
               
                time.sleep(DELAY)
                
    except FileNotFoundError:
        print(f"[ERROR] File not found: {CSV_PATH}")
    except KeyboardInterrupt:
        print("\n[!] User stopped the process.")
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        producer.flush()
        print(f"\n[DONE] Finished sending {sent_count} rows. Exiting.")

if __name__ == "__main__":
    start_streaming()
