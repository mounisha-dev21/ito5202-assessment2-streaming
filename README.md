# ITO5202 Assessment 2: ML Pipeline and Real-Time Streaming
 
**Student ID:** 35721588
**Unit:** ITO5202
**Dataset:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)
**Dataset Link:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

## Repository contents
 
| Path | Purpose |
|---|---|
| `assessment2.ipynb` | Main notebook: Parts A and B |
| `producer.py` | Kafka producer replaying the held-out streaming subset |
| `models/a2_model/` | Persisted Spark ML PipelineModel from Part A |
| `data/README.md` | Instructions for obtaining the source data |
| `requirements.txt` | Python dependencies |

## Project overview

This project seeks to train a Spark MLlib model to predict the freight charge on Olist order items. It then looks to apply the saved model to a simulated real-time stream of held-out orders sent through Kafka.

## Reproducing the environment

All results were produced in Spark local mode on a single machine.

| Property | Value |
|---|---|
| Machine | MacBook Air (2018), dual-core Intel i5, 8 GB RAM |
| Operating system | macOS Sonoma 14.7.8 |
| Java | Eclipse Temurin JDK 17 |
| Python | 3.11.9 |
| PySpark | 3.5.1 (local mode) |
| Kafka | 3.9.2 (Scala 2.13), ZooKeeper mode |
| Spark Kafka connector | org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 |

**1. Python 3.11.9.** This was installed from https://www.python.org/downloads/release/python-3119/

```bash
git clone <repo-url>
cd ito5202-assessment2-streaming
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**2. Kafka**

Since we need ZooKeeper, we need to use an older version of Kafka due to the fact that the most recent version does not support it. We note that 3.9.2 is the newest version that still includes it, and as such need to download that specific version as follows:

```bash
cd ~
curl -O https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz
```

Note that ZooKeeper and Kafka each run continuously, so each needs its own terminal.

*Terminal 1: ZooKeeper*

```bash
cd ~/kafka_2.13-3.9.2
bin/zookeeper-server-start.sh config/zookeeper.properties
```

*Terminal 2: Kafka* (can only be started once ZooKeeper is already running)

```bash
cd ~/kafka_2.13-3.9.2
export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"
bin/kafka-server-start.sh config/server.properties --override listeners=PLAINTEXT://localhost:9092 --override advertised.listeners=PLAINTEXT://localhost:9092
```

The above `heap` setting limits Kafka to 512 MB of memory to leave room for Spark on an 8 GB machine, and the listener overrides make Kafka identify itself as `localhost`, in order to avoid hostname resolution issues.

*Stopping Kafka*

We need to ensure that we always stop Kafka before ZooKeeper. Press Ctrl+C (on a Mac, as is used in this project) while in the Kafka terminal and wait for the prompt to return, then press Ctrl+C in the ZooKeeper terminal.