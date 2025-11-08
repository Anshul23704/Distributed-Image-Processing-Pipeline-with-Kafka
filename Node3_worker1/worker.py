import sys
import json
import time
import uuid
import base64
from io import BytesIO
from threading import Thread

import cv2
import numpy as np
from PIL import Image
from confluent_kafka import Producer, Consumer

import config

WORKER_ID = str(uuid.uuid4())


def process_tile(image_data: bytes, effect: str) -> bytes:
    """Apply specified effect to image tile"""
    
    nparr = np.frombuffer(image_data, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    if effect == 'grayscale':
        processed = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        processed = cv2.cvtColor(processed, cv2.COLOR_GRAY2BGR)
    
    elif effect == 'edge_detection':
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 100, 200)
        processed = cv2.cvtColor(edges, cv2.COLOR_GRAY2BGR)
    
    elif effect == 'blur':
        processed = cv2.GaussianBlur(img, (15, 15), 0)
    
    elif effect == 'sharpen':
        kernel = np.array([[-1, -1, -1],
                          [-1,  9, -1],
                          [-1, -1, -1]])
        processed = cv2.filter2D(img, -1, kernel)
    
    else:
        processed = img
    
    _, buffer = cv2.imencode('.png', processed)
    return buffer.tobytes()


def send_heartbeat(producer):
    """Send periodic heartbeats"""
    while True:
        try:
            heartbeat = {
                'worker_id': WORKER_ID,
                'timestamp': time.time()
            }
            producer.produce(
                config.HEARTBEATS_TOPIC,
                value=json.dumps(heartbeat).encode('utf-8')
            )
            producer.flush()
        except Exception as e:
            print(f"Error sending heartbeat: {e}")
        
        time.sleep(config.HEARTBEAT_INTERVAL)


def main():
    """Main worker loop"""
    print(f"🔧 Worker {WORKER_ID[:8]} starting...")
    print(f"🔗 Connecting to Kafka: {config.KAFKA_BROKER}")
    
    # Initialize Kafka producer
    producer_conf = {'bootstrap.servers': config.KAFKA_BROKER}
    producer = Producer(producer_conf)
    
    # Initialize Kafka consumer
    consumer_conf = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': config.CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([config.TASKS_TOPIC])
    
    # Start heartbeat thread
    heartbeat_thread = Thread(target=send_heartbeat, args=(producer,), daemon=True)
    heartbeat_thread.start()
    
    print(f"✅ Worker {WORKER_ID[:8]} ready and listening...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            try:
                task = json.loads(msg.value().decode('utf-8'))
                job_id = task['job_id']
                tile_id = task['tile_id']
                effect = task['effect']
                
                print(f"🔥 Processing {tile_id[:16]}... (effect: {effect})")
                
                # Decode image data
                tile_data = base64.b64decode(task['data'])
                
                # Process tile
                start_time = time.time()
                processed_data = process_tile(tile_data, effect)
                processing_time = time.time() - start_time
                
                # Prepare result
                result = {
                    'job_id': job_id,
                    'tile_id': tile_id,
                    'x': task['x'],
                    'y': task['y'],
                    'width': task['width'],
                    'height': task['height'],
                    'data': base64.b64encode(processed_data).decode('utf-8'),
                    'worker_id': WORKER_ID,
                    'processing_time': processing_time
                }
                
                # Send result
                producer.produce(
                    config.RESULTS_TOPIC,
                    value=json.dumps(result).encode('utf-8')
                )
                producer.flush()
                
                print(f"✅ Completed {tile_id[:16]} in {processing_time:.2f}s")
            
            except Exception as e:
                print(f"❌ Error processing task: {e}")
    
    except KeyboardInterrupt:
        print(f"\n🛑 Worker {WORKER_ID[:8]} shutting down...")
    
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
