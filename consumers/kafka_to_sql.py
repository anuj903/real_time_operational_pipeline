import json
import pyodbc
from kafka import KafkaConsumer
import time


class KafkaToSQLConsumer:
    def __init__(self, kafka_cfg, sql_cfg, topic, group_id, value_field, table):
        self.topic = topic
        self.value_field = value_field
        self.table = table

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_cfg["bootstrap_servers"],
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        conn_str = (
            f"DRIVER={{{sql_cfg['driver']}}};"
            f"SERVER={sql_cfg['server']};"
            f"DATABASE={sql_cfg['database']};"
            f"UID={sql_cfg['username']};"
            f"PWD={sql_cfg['password']}"
        )

        self.cn = pyodbc.connect(conn_str, autocommit=False)
        self.cur = self.cn.cursor()

        print(f"[SQL] Connected → {sql_cfg['server']} / {sql_cfg['database']}")
        print(f"[Kafka] Consuming {topic}")

   

    def run(self):
        batch_size = 50
        commit_interval = 2  # seconds

        count = 0
        last_commit = time.time()

        for msg in self.consumer:
            event = msg.value
            payload_value = event.get("payload")
            ts_ms = event.get("ts")

            if payload_value is None or ts_ms is None:
                continue

            sql = f"""
                INSERT INTO {self.table} ({self.value_field}, ts_ms)
                VALUES (?, ?)
            """

            self.cur.execute(sql, payload_value, ts_ms)
            count += 1

            now = time.time()
            if count % batch_size == 0 or (now - last_commit) >= commit_interval:
                self.cn.commit()
                last_commit = now
                print(f"[SQL] Committed {count} rows")



# from src.logger import get_logger

# logger = get_logger("kafka_to_sql")

# logger.info("[SQL] Connected")
# logger.info("[Kafka] Consuming sensor.distance")