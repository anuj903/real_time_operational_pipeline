# MQTT to Kafka to SQL Server Pipeline

## Overview

This project implements an end-to-end real-time data pipeline where sensor data is published via MQTT, streamed through Apache Kafka, and finally persisted into SQL Server for analytics and visualization.

The system is designed to support:

- Real-time ingestion
- Historical replay
- Fault tolerance
- Scalable architecture

---

## Architecture Overview

### Systems Involved

| System   | Role                                                                  |
|----------|-----------------------------------------------------------------------|
| System A | ESP32 + Ultrasonic Sensor - publishes data via MQTT                  |
| System B | MQTT Broker + Kafka + MQTT-Kafka Bridge (core system)                |
| System C | SQL Server (storage) + Power BI (visualization)                      |

### Final Data Flow

```
ESP32 (System A)
   ↓
MQTT Broker (System B)
   ↓
Python MQTT–Kafka Bridge (System B)
   ↓
Kafka Topic (esp32.distance)
   ↓
Kafka Consumer (System C)
   ↓
SQL Server Table
   ↓
Power BI / Analytics
```

---

## Data Format (End-to-End)

### MQTT Payload (from ESP32)

```json
{
  "cm": 2178.46,
  "topic": "esp32/distance",
  "ts": 1766050420385
}
```

### Kafka Message

- Kafka stores the payload as-is
- No transformation happens at the broker level
- JSON remains intact end-to-end

### SQL Table Fields

| Column        | Meaning                        |
|---------------|--------------------------------|
| cm            | Distance reading               |
| topic         | Kafka topic name               |
| ts_ms         | Event timestamp (epoch ms)     |
| reading_time  | Computed UTC datetime          |
| ingested_at   | SQL insert time                |

---

## System B - Detailed Setup (MOST IMPORTANT)

### Installed Components

- Apache Kafka (KRaft mode, no ZooKeeper)
- Python 3.10+
- paho-mqtt
- kafka-python
- Python virtual environment
- Kafka Windows binaries

---

## Kafka Configuration (System B)

### server.properties (Final, Working)

```properties
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
advertised.listeners=PLAINTEXT://<System B IP>:9092

inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

log.dirs=D:/kafka-data/kraft-logs
num.partitions=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
```

### Key Learnings

- **9092** - Client traffic (producer / consumer)
- **9093** - Controller traffic (internal only)
- `advertised.listeners` must match client access IP
- Kafka metadata may look healthy even when producers fail

---

## MQTT to Kafka Bridge (System B)

### MQTT Client

- Subscribes to `esp32/distance`
- Uses callback to forward messages to Kafka
- MQTT layer worked correctly from day one

### Confirmation Log

```
[Bridge] MQTT → {'cm': 2178.46, 'topic': 'esp32/distance', 'ts': ...}
```

---

## Kafka Producer (System B)

### Final Correct Producer Logic

```python
def send(self, data):
    self.producer.send(self.topic, data).add_errback(
        lambda e: print(f"[Kafka] async send error: {e!r}")
    )

def close(self):
    self.producer.flush(timeout=15)
    self.producer.close(timeout=5)
```

### Critical Fix

**Removed periodic flush() from the hot path**

**Why?**

- `flush()` is blocking
- MQTT callback is latency-sensitive
- Caused repeated `KafkaTimeoutError`
- Kafka already batches messages internally

---

## Kafka Topic

### Topic Name

```
esp32.distance
```

### Naming Notes

- Kafka supports dots (`.`)
- Slashes (`/`) are invalid
- Earlier naming mismatch caused silent failures

---

## Major Errors Encountered and Fixes

### Error 1: KafkaTimeoutError - Failed to update metadata

**Cause:**
- Wrong broker IP
- `advertised.listeners` mismatch

**Fix:**
- Aligned `bootstrap_servers` with `advertised.listeners`
- Verified using producer smoke test

---

### Error 2: Topic exists but no data

**Symptom:**
- `__cluster_metadata-0` growing
- Topic log empty

**Cause:**
- Producer reached controller, not broker listener

**Fix:**
- Corrected `advertised.listeners`
- Verified using `partitions_for(topic)`

---

### Error 3: broker-list is not recognized

**Cause:**
- Deprecated Kafka CLI option

**Fix:**
- Use `--bootstrap-server` instead

---

### Error 4: IllegalArgumentException - listeners on same port

**Cause:**
- Controller and broker bound to same port

**Fix:**
- Broker on port **9092**
- Controller on port **9093**

---

### Error 5: SQL Login failed (18456)

**Cause:**
- Wrong SQL port (1433 instead of custom)

**Fix:**
- Use correct port **56661**
- Enabled SQL authentication + firewall rule

---

### Error 6: pyodbc fast_executemany missing

**Cause:**
- Outdated pyodbc version

**Fix:**
```bash
pip install --upgrade pyodbc
```

---

### Error 7: Kafka consumer stuck with no output

**Cause:**
- Topic had no data
- Producer never succeeded

**Fix:**
- Fixed producer first
- Verified via console consumer

---

## Kafka Smoke Test (Turning Point)

### Standalone Producer Test

```python
producer.send({
  "cm": 123.45,
  "topic": "debug/test",
  "ts": ...
})
```

### Console Consumer Output

```json
{"cm":123.45,"topic":"debug/test","ts":...}
```

**Result:** Confirmed Kafka cluster health and networking correctness

---

## SQL Server (System C)

### Table Definition

```sql
CREATE TABLE <Schema>.<Table name> (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    cm FLOAT NOT NULL,
    topic NVARCHAR(200) NOT NULL,
    ts_ms BIGINT NOT NULL,
    reading_time AS 
        DATEADD(MILLISECOND, ts_ms % 1000,
        DATEADD(SECOND, ts_ms / 1000, '1970-01-01')) PERSISTED,
    ingested_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
);
```

---

## Kafka to SQL Consumer Logic

### Behavior Achieved

| Scenario       | Result                                |
|----------------|---------------------------------------|
| First run      | Dumps all past Kafka data             |
| While running  | Inserts live data                     |
| Restart        | Resumes from last committed offset    |
| Crash          | At-least-once delivery                |

### Key Kafka Configuration

```python
group_id="sql-sink-ultrasonic"
auto_offset_reset="earliest"
```

---

## Final Achievements

- End-to-end data flow
- Real-time ingestion
- Historical replay
- Fault tolerance
- Scalable architecture
- Production-grade Kafka setup

---

## Key Lessons Learned

- Kafka issues are usually networking / `advertised.listeners`
- Metadata success does not equal producer success
- Never call `flush()` in streaming hot paths
- Always isolate Kafka with smoke tests
- Config consistency beats code changes
- Kafka is powerful but unforgiving

---

## Future Enhancements

- Power BI dashboards
- Schema validation
- Batch insert optimizations
- TimescaleDB + Grafana
- Spark Structured Streaming
- Kafka Connect JDBC Sink
