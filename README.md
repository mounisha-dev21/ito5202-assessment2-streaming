# ITO5202 Assessment 2: ML Pipeline and Real-Time Streaming
 
**Student ID:** 35721588
**Unit:** ITO5202
**Dataset:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)
**Dataset Link:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

---

## Repository contents
 
| Path | Purpose |
|---|---|
| `assessment2.ipynb` | Main notebook: Parts A and B |
| `producer.py` | Kafka producer replaying the held-out streaming subset |
| `models/a2_model/` | Persisted Spark ML PipelineModel from Part A |
| `data/README.md` | Instructions for obtaining the source data |
| `requirements.txt` | Python dependencies |
| `.gitignore` | Excludes the virtual environment, raw data, generated data, streaming outputs and checkpoints |

The following artefacts were also created locally when the project runs but are not committed: 
- `data/raw/` (source CSVs)
- `data/stream_data.parquet/` (streaming subset)
- `output/predictions/` (streamed predictions)
- `checkpoints/` (streaming query checkpoints)

---

## Project overview

This project seeks to train a Spark MLlib model to predict the freight charge on Olist order items. It then looks to apply the saved model to a simulated real-time stream of held-out orders sent through Kafka.

---

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

We want to use the above `heap` setting so that Kafka is limited to 512 MB of memory. This ensure that we leave enough room for Spark on our 8 GB machine. Also, the listener overrides makes sure that Kafka identifies itself as `localhost`, in order to avoid hostname resolution issues.

*Terminal 3: creating the topic* _**(first run only)**_

```bash
cd ~/kafka_2.13-3.9.2
bin/kafka-topics.sh --create --if-not-exists --topic events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

We note that the `events` topic uses one partition. This ensures that our records are kept in the same order that they are sent by the producer, which is useful for us here because the streaming data is being replayed in purchase-time order.

We also note that `--if-not-exists` makes this safe for us to rerun as needed. This is important due to the fact that macOS clears `/tmp` on restart, which is where Kafka stores its data, so the topic may need recreating after a reboot.

*Stopping Kafka*

We need to ensure that we always stop Kafka before ZooKeeper. Press Ctrl+C (on a Mac, as is used in this project) while in the Kafka terminal and wait for the prompt to return, then press Ctrl+C in the ZooKeeper terminal.

---

## Running the notebook

### Part A and saving the model

The project runs in two main phases. Importantly, **Kafka should be stopped during Part A**. We found during development that the model training component of Part A was the most memory-intensive step, and running Spark alongside ZooKeeper and Kafka on an 8 GB machine caused our Spark driver to crash. As such, we want to complete Part A first, clear the cached data, and only then start Kafka for Part B.

Below, we detail the steps in chronological order for running the notebook successfully.

### Part A and saving the model

We want to first make sure that the dataset is available in data/raw/, as described in data/README.md. We can then open the main assessment2.ipynb notebook file and select the .venv kernel.

Rather than running the whole notebook at once, we want to click into the last code cell of Part A, which contains the following code:

`spark.catalog.clearCache()`

From here, in VS Code we can use **Notebook: Execute Above Cells** from the Command Palette to run all cells, except for the final one, of Part A (this should take ~15 minutes to complete).

Once all of the earlier cells have finished, we then want to run the `spark.catalog.clearCache()` cell itself. This releases the cached Part A data before we start Kafka and begin the streaming work.

To verify that this has completed successfully, Part A should have created the held-out streaming data at data/stream_data.parquet/ and save the fitted pipeline to models/a2_model/.

### Starting the Kafka producer

Once Part A has finished, we can start ZooKeeper, Kafka and the events topic in three terminals, following the instructions in the Starting Kafka section above.

From our repository folder, with the virtual environment active (as created above in the Python instructions section), we want to run:

```bash
python producer.py
```

We note that the producer also accepts a few optional arguments:

| Option | Default | Description |
|---|---|---|
| `--batch-size` | 500 | Number of records sent in each batch |
| `--topic` | `events` | Kafka topic to publish to |
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--input` | `data/stream_data.parquet` | Location of the saved streaming data |
| `--max-batches` | none | Optional limit on the number of batches, mainly for testing |

For example, if we ran the below:

```bash
python producer.py --batch-size 1000
```

This would send up to 1,000 records in each batch.

**Batch size**

As we noted above, the default batch size is 500 records, which we note already sits inside the 100–1,000 range suggested in the assessment instructions. If we consider our specific use case, we know that our streaming subset contains 32,826 records, so this would produce 66 batches under the default batch size.

With a five-second pause between batches, we can see that using the default size would mean that the full dataset would be replayed over roughly 5.5 minutes. Furthermore, a batch of 500 records is around 255 KB as JSON, so it also stays comfortably below Kafka's default 1 MB message size. As such, we want to stick to the default size rather than trying to set any other size restriction.

In order to get comfort in our choice, we also note that the producer logs each batch as it is sent, including the number of records, its publish time and the total sent so far. An example of this in the logs is as follows:

```text
2026-10-05 11:14:13,501 | INFO | Batch 1/66 | records: 500 | event_timestamp: 2026-10-05T00:14:13.321+00:00 | size: 254.6 KB | total sent: 500
```

### 3. Run the streaming consumer

_To be completed._