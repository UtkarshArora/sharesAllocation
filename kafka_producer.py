import time
import json
import csv
from datetime import datetime
from dateutil import parser
from kafka import KafkaProducer


KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'mock_11_stream'
CSV_FILE = 'l1_day.csv'
START_TIME = '13:36:32'
END_TIME = '13:45:14'

def parse_time(time_str):
    return datetime.strptime(time_str, '%H:%M:%S').time()

def is_in_time_window(row_time):
    row_time = parser.isoparse(row_time).time()
    return parse_time(START_TIME) <= row_time <= parse_time(END_TIME)

def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    snapshots = []
    with open(CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if is_in_time_window(row['ts_event']):
                snapshots.append({
                    'timestamp': row['ts_event'],
                    'publisher_id': row['publisher_id'],
                    'ask_px_00': float(row['ask_px_00']),
                    'ask_sz_00': int(row['ask_sz_00'])
                })
    
    snapshots.sort(key=lambda x: x['timestamp'])
    prev_ts = None
    
    for snapshot in snapshots:
        current_ts = parser.isoparse(snapshot['timestamp'])
        
        if prev_ts:
            delta = (current_ts - prev_ts).total_seconds()
            time.sleep(max(delta, 0))  
            
        producer.send(TOPIC_NAME, value=snapshot)
        print(f"Sent: {snapshot['timestamp']} | {snapshot['publisher_id']}")
        
        prev_ts = current_ts
    
    producer.flush()
    print(f"Produced {len(snapshots)} snapshots to topic {TOPIC_NAME}")

if __name__ == "__main__":
    kafka_producer()