"""
Master Node: FastAPI Web UI + Image Processing Orchestrator + Metadata Storage
Run on Node 1 - ZeroTier Network
"""

import io
import json
import time
import uuid
import base64
import asyncio
from pathlib import Path
from typing import Dict, List

import numpy as np
from PIL import Image
from confluent_kafka import Producer, Consumer
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
import uvicorn

import config
from database import MetadataDB
from fastapi import Form

app = FastAPI(title="Distributed Image Processing Pipeline")

# Global state
jobs: Dict[str, dict] = {}
active_workers: Dict[str, float] = {}
producer = None
results_consumer = None
heartbeat_consumer = None
db = None


def init_kafka():
    """Initialize Kafka producer and consumers"""
    global producer, results_consumer, heartbeat_consumer
    
    print(f"🔗 Connecting to Kafka at: {config.KAFKA_BROKER}")
    
    producer_conf = {'bootstrap.servers': config.KAFKA_BROKER}
    producer = Producer(producer_conf)
    
    consumer_conf = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': 'master-results',
        'auto.offset.reset': 'latest'
    }
    results_consumer = Consumer(consumer_conf)
    results_consumer.subscribe([config.RESULTS_TOPIC])
    
    hb_consumer_conf = {
        'bootstrap.servers': config.KAFKA_BROKER,
        'group.id': 'master-heartbeats',
        'auto.offset.reset': 'latest'
    }
    heartbeat_consumer = Consumer(hb_consumer_conf)
    heartbeat_consumer.subscribe([config.HEARTBEATS_TOPIC])
    
    print("✅ Kafka initialized")


def init_database():
    """Initialize SQLite database"""
    global db
    db = MetadataDB()
    print("✅ Database initialized")


def split_image(image: Image.Image, tile_size: int) -> List[dict]:
    """Split image into tiles"""
    width, height = image.size
    tiles = []
    
    for y in range(0, height, tile_size):
        for x in range(0, width, tile_size):
            tile_width = min(tile_size, width - x)
            tile_height = min(tile_size, height - y)
            tile = image.crop((x, y, x + tile_width, y + tile_height))
            
            buffer = io.BytesIO()
            tile.save(buffer, format='PNG')
            tile_bytes = buffer.getvalue()
            
            tiles.append({
                'x': x,
                'y': y,
                'width': tile_width,
                'height': tile_height,
                'data': base64.b64encode(tile_bytes).decode('utf-8')
            })
    
    return tiles


def reconstruct_image(tiles: List[dict], original_size: tuple) -> Image.Image:
    """Reconstruct image from processed tiles"""
    result_image = Image.new('RGB', original_size)
    
    for tile in tiles:
        tile_data = base64.b64decode(tile['data'])
        tile_image = Image.open(io.BytesIO(tile_data))
        result_image.paste(tile_image, (tile['x'], tile['y']))
    
    return result_image


async def consume_results():
    """Background task: Consume processed tiles"""
    while True:
        msg = results_consumer.poll(0.1)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        
        if msg.error():
            continue
        
        try:
            result = json.loads(msg.value().decode('utf-8'))
            job_id = result['job_id']
            tile_id = result['tile_id']
            worker_id = result['worker_id']
            processing_time = result['processing_time']
            
            db.update_tile_status(tile_id, 'completed', worker_id, processing_time)
            db.update_worker_stats(worker_id, processing_time)
            
            if job_id in jobs:
                jobs[job_id]['results'].append(result)
                jobs[job_id]['completed_tiles'] += 1
                
                db.update_job_progress(job_id, jobs[job_id]['completed_tiles'])
                
                if jobs[job_id]['completed_tiles'] == jobs[job_id]['total_tiles']:
                    tiles = jobs[job_id]['results']
                    original_size = jobs[job_id]['original_size']
                    final_image = reconstruct_image(tiles, original_size)
                    
                    buffer = io.BytesIO()
                    final_image.save(buffer, format='PNG')
                    jobs[job_id]['result_image'] = base64.b64encode(
                        buffer.getvalue()
                    ).decode('utf-8')
                    jobs[job_id]['status'] = 'completed'
                    jobs[job_id]['end_time'] = time.time()
                    
                    total_time = jobs[job_id]['end_time'] - jobs[job_id]['start_time']
                    db.complete_job(job_id, total_time)
                    
                    print(f"✅ Job {job_id[:8]} completed in {total_time:.2f}s")
        
        except Exception as e:
            print(f"Error processing result: {e}")


async def consume_heartbeats():
    """Background task: Monitor worker heartbeats"""
    while True:
        msg = heartbeat_consumer.poll(0.1)
        if msg is None:
            await asyncio.sleep(0.1)
            continue
        
        if msg.error():
            continue
        
        try:
            heartbeat = json.loads(msg.value().decode('utf-8'))
            worker_id = heartbeat['worker_id']
            active_workers[worker_id] = time.time()
            
            db.update_worker_heartbeat(worker_id)
            
        except Exception as e:
            print(f"Error processing heartbeat: {e}")


async def cleanup_workers():
    """Background task: Remove dead workers"""
    while True:
        current_time = time.time()
        dead_workers = [
            wid for wid, last_seen in active_workers.items()
            if current_time - last_seen > config.WORKER_TIMEOUT
        ]
        
        for wid in dead_workers:
            del active_workers[wid]
            db.mark_worker_inactive(wid)
            print(f"⚠️ Worker {wid[:8]} marked as dead")
        
        await asyncio.sleep(5)


@app.on_event("startup")
async def startup_event():
    """Start background tasks"""
    init_database()
    init_kafka()
    asyncio.create_task(consume_results())
    asyncio.create_task(consume_heartbeats())
    asyncio.create_task(cleanup_workers())
    print("🚀 Master node started")


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve web UI"""
    html_path = Path(__file__).parent / "static" / "index.html"
    return HTMLResponse(content=html_path.read_text())


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    stats = db.get_statistics()
    return {
        "status": "healthy",
        "active_workers": len(active_workers),
        "active_jobs": len([j for j in jobs.values() if j['status'] == 'processing']),
        "statistics": stats
    }


@app.get("/api/workers")
async def get_workers():
    """Get active workers"""
    workers = db.get_active_workers(config.WORKER_TIMEOUT)
    return {
        "workers": workers,
        "count": len(workers)
    }


@app.get("/api/effects")
async def get_effects():
    """Get available processing effects"""
    return {"effects": config.PROCESSING_EFFECTS}


@app.get("/api/jobs")
async def get_all_jobs():
    """Get all jobs history"""
    jobs_list = db.get_all_jobs(limit=50)
    return {"jobs": jobs_list}


@app.post("/api/process")
async def process_image(file: UploadFile = File(...), effect: str = Form(...)):
    """Process uploaded image"""
    if not effect:
    	raise HTTPException(400, "Effect not specified")
    
    if effect not in config.PROCESSING_EFFECTS:
        raise HTTPException(400, f"Invalid effect: {effect}")
    
    if len(active_workers) == 0:
        raise HTTPException(503, "No active workers available")
    
    try:
        image_data = await file.read()
        image = Image.open(io.BytesIO(image_data))
        
        if image.mode != 'RGB':
            image = image.convert('RGB')
        
        width, height = image.size
        if width < config.MIN_IMAGE_SIZE or height < config.MIN_IMAGE_SIZE:
            raise HTTPException(
                400,
                f"Image must be at least {config.MIN_IMAGE_SIZE}x{config.MIN_IMAGE_SIZE}"
            )
        
        job_id = str(uuid.uuid4())
        tiles = split_image(image, config.TILE_SIZE)
        
        db.create_job(job_id, effect, len(tiles), width, height)
        
        jobs[job_id] = {
            'id': job_id,
            'status': 'processing',
            'effect': effect,
            'total_tiles': len(tiles),
            'completed_tiles': 0,
            'results': [],
            'original_size': (width, height),
            'start_time': time.time(),
            'end_time': None,
            'result_image': None
        }
        
        for i, tile in enumerate(tiles):
            tile_id = f"{job_id}-{i}"
            
            db.create_tile(tile_id, job_id, i, 
                          tile['x'], tile['y'], 
                          tile['width'], tile['height'])
            
            task = {
                'job_id': job_id,
                'tile_id': tile_id,
                'effect': effect,
                **tile
            }
            
            producer.produce(
                config.TASKS_TOPIC,
                value=json.dumps(task).encode('utf-8')
            )
        
        producer.flush()
        
        print(f"📤 Published {len(tiles)} tiles for job {job_id[:8]}")
        
        return {
            "job_id": job_id,
            "total_tiles": len(tiles),
            "status": "processing"
        }
    
    except Exception as e:
        raise HTTPException(500, f"Processing error: {str(e)}")


@app.get("/api/job/{job_id}")
async def get_job_status(job_id: str):
    """Get job status"""
    if job_id in jobs:
        job = jobs[job_id]
        response = {
            "job_id": job_id,
            "status": job['status'],
            "progress": (job['completed_tiles'] / job['total_tiles']) * 100,
            "total_tiles": job['total_tiles'],
            "completed_tiles": job['completed_tiles'],
            "effect": job['effect']
        }
        
        if job['status'] == 'completed':
            response['result_image'] = job['result_image']
            response['processing_time'] = job['end_time'] - job['start_time']
        
        return response
    
    job_data = db.get_job(job_id)
    if not job_data:
        raise HTTPException(404, "Job not found")
    
    return {
        "job_id": job_id,
        "status": job_data['status'],
        "progress": (job_data['completed_tiles'] / job_data['total_tiles']) * 100,
        "total_tiles": job_data['total_tiles'],
        "completed_tiles": job_data['completed_tiles'],
        "effect": job_data['effect']
    }


@app.get("/api/statistics")
async def get_statistics():
    """Get system statistics"""
    return db.get_statistics()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
