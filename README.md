# Distributed-Image-Processing-Pipeline-with-Kafka

### *Kafka-Powered • ZeroTier-Connected • Multi-Node Image Processing*

4-node distributed system that performs **parallel image processing** by splitting images into tiles, distributing tiles to worker nodes, processing them with OpenCV, and reconstructing the final output — all controlled through a live web dashboard.

Nodes communicate **over a ZeroTier virtual network**, ensuring secure, LAN-independent connectivity.

---

# Repository Structure

```
Distributed-Image-Processing-Pipeline-with-Kafka/
│
├── KafkaBroker/                       # Node 2 (Broker)
│   ├── KafkaBroker&Topics_commands.txt
│   ├── server_properties.txt
│
├── Node1_MasterNode/                  # Node 1 (Master)
│   ├── config.py
│   ├── database.py
│   ├── master.py
│   └── static/
│       └── index.html
│
├── Node3_worker1/                     # Node 3 (Worker 1)
│   ├── config.py
│   └── worker.py
│
├── Node4_worker2/                     # Node 4 (Worker 2)
│   ├── config.py
│   └── worker.py
│
├── requirements.txt
└── README.md
```

---

# ZeroTier Networking Setup

### *All communication depends on ZeroTier. Configure this before running anything.*

---

## **1 Install ZeroTier (all four nodes)**

Linux:

```bash
curl -s https://install.zerotier.com | sudo bash
```

Windows/Mac:
Download from [https://www.zerotier.com/download](https://www.zerotier.com/download)

---

## **Join the same ZeroTier network**

Run on every node:

```bash
sudo zerotier-cli join <NETWORK_ID>
```

Example:

```bash
sudo zerotier-cli join 8056c2e21c000001
```

---

## **3️ Approve nodes in the ZeroTier web dashboard**

Visit:

```
https://my.zerotier.com
```

Approve all 4 devices.

You will get IPs like:

```
10.147.20.101  (Node 1 - Master)
10.147.20.102  (Node 2 - Kafka Broker)
10.147.20.103  (Node 3 - Worker 1)
10.147.20.104  (Node 4 - Worker 2)
```

---

## **4️ Verify IPs**

On each node:

```bash
ip addr show | grep zt
```

---

## **5️Check node-to-node communication**

Node1 → Node2:

```bash
ping 10.147.20.102
```

Node1 → Worker1:

```bash
ping 10.147.20.103
```

Every node must be reachable.

---

## **6️Update `config.py` (Master + Workers)**

Set:

```
KAFKA_BROKER = "<Node2-ZeroTier-IP>:9092"
```

Example:

```python
KAFKA_BROKER = "10.147.20.102:9092"
```

---

# **System Startup Order (VERY IMPORTANT)**

All nodes will fail unless started in this exact sequence:

## **1️ Start the Kafka Broker (Node 2) — FIRST**

Kafka must be running before anything else.

Start Zookeeper:

```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka:

```bash
bin/kafka-server-start.sh config/server.properties
```

Create topics:

```bash
kafka-topics.sh --create --topic tasks --partitions 2 --replication-factor 1
kafka-topics.sh --create --topic results --partitions 2 --replication-factor 1
kafka-topics.sh --create --topic heartbeats --partitions 1 --replication-factor 1
```

---

## **2️Start the Master Node (Node 1) — SECOND**

On Node1:

```bash
cd Node1_MasterNode
pip install -r ../requirements.txt
python3 master.py
```

Master will connect to Kafka and wait for workers.

Web UI:

```
http://<Node1-ZeroTier-IP>:8000
```

---

## **3️ Start the Worker Nodes (Node 3 & Node 4) — LAST**

Only start workers after the master is up.

Worker 1:

```bash
cd Node3_worker1
pip install -r ../requirements.txt
python3 worker.py
```

Worker 2:

```bash
cd Node4_worker2
pip install -r ../requirements.txt
python3 worker.py
```

Workers will:

* Send **heartbeats**
* Pull **tiles** from Kafka
* Push **processed results** back

They appear in the dashboard automatically.

---

#  Features

* Distributed tile-based image processing
* Real-time worker monitoring
* Kafka-backed task distribution
* Effects: Grayscale, Edge Detection, Blur, Sharpen
* Fail-safe worker timeout detection
* SQLite job & tile metadata tracking
* Beautiful Web UI with progress bars

---
