

"""
Shared configuration for all nodes - ZeroTier Network
IMPORTANT: Change KAFKA_BROKER to your Node 2 ZeroTier IP!
"""

# ============ Kafka Configuration ============
# CHANGE THIS to your Node 2 ZeroTier IP address!
KAFKA_BROKER = "10.147.17.70:9092"  # <-- UPDATE THIS!

TASKS_TOPIC = "tasks"
RESULTS_TOPIC = "results"
HEARTBEATS_TOPIC = "heartbeats"

# ============ Image Configuration ============
MIN_IMAGE_SIZE = 1024
TILE_SIZE = 512
CONSUMER_GROUP = "image-workers"

# ============ Processing Configuration ============
HEARTBEAT_INTERVAL = 5
WORKER_TIMEOUT = 15

# ============ Database Configuration ============
DATABASE_PATH = "pipeline_metadata.db"

# ============ Processing Effects ============
PROCESSING_EFFECTS = {
    'grayscale': 'Convert to grayscale',
    'edge_detection': 'Detect edges using Canny',
    'blur': 'Apply Gaussian blur',
    'sharpen': 'Enhance image sharpness'
}