import csv
import json
import threading
import sys
from kafka import KafkaProducer

# ===================== CONFIGURATION =====================
BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'cybersecurity-logs'
CSV_PATH = '/tmp/cybersecurity_threat_detection_logs_streaming.csv'
NUM_THREADS = 4

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serialize dictionary to JSON and encode to bytes
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Key must be bytes for Kafka partitioning
        key_serializer=lambda k: k.encode('utf-8'),
        # Performance tuning: Batching settings
        batch_size=65536,  # 64KB batches
        linger_ms=10       # Wait 10ms before sending batch
    )
except Exception as e:
    print(f"[ERROR] Failed to connect to Kafka: {e}")
    sys.exit(1)

def send_batch(rows, thread_id):
    """
    Function to be executed by each thread to send a chunk of logs.
    """
    print(f"[Thread-{thread_id}] Started processing {len(rows)} records...")
    
    for row in rows:
        # Extract source_ip to use as a key for consistent partitioning
        ip_key = str(row.get('source_ip', '0.0.0.0'))
        
        try:
            # Asynchronous send (Non-blocking)
            producer.send(TOPIC_NAME, key=ip_key, value=dict(row))
        except Exception as e:
            print(f"[Thread-{thread_id}] Error sending record: {e}")
            
    print(f"[Thread-{thread_id}] Finished pushing data to buffer.")

def main():
    try:
        # Read the entire CSV file into memory for faster thread distribution
        print(f"[*] Opening CSV file: {CSV_PATH}")
        with open(CSV_PATH, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)

        total_rows = len(all_rows)
        if total_rows == 0:
            print("[!] CSV file is empty. Exiting.")
            return

        # Calculate chunk size for each thread
        chunk_size = total_rows // NUM_THREADS
        threads = []

        print(f"[*] Starting Multi-threaded Producer: {total_rows} total rows with {NUM_THREADS} threads.")

        for i in range(NUM_THREADS):
            # Define indices for slicing the row list
            start_idx = i * chunk_size
            # The last thread takes all remaining rows
            end_idx = (i + 1) * chunk_size if i != NUM_THREADS - 1 else total_rows
            
            chunk = all_rows[start_idx:end_idx]

            # Create and start thread
            t = threading.Thread(target=send_batch, args=(chunk, i))
            threads.append(t)
            t.start()

        # Wait for all threads to complete their tasks
        for t in threads:
            t.join()

        # CRITICAL: Flush remaining records in producer buffer to Kafka
        print("[*] Flushing final records to Kafka...")
        producer.flush()
        
        print(f"[SUCCESS] All {total_rows} logs have been streamed successfully.")

    except FileNotFoundError:
        print(f"[ERROR] CSV file not found at: {CSV_PATH}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred: {e}")
    finally:
        # Close producer connection
        producer.close()
        print("[*] Producer connection closed.")

if __name__ == "__main__":
    main()
