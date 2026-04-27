# run_humidity_sink.py
from consumers.kafka_to_sql import KafkaToSQLConsumer
from src.config_loader import load_yaml

kafka_cfg = load_yaml("config/kafka.yaml")["kafka"]
sql_cfg = load_yaml("config/sqlserver.yaml")["sqlserver"]

KafkaToSQLConsumer(
    kafka_cfg=kafka_cfg,
    sql_cfg=sql_cfg,
    topic="sensor.humidity",
    group_id="sql-humidity-sink",
    value_field="humidity_pct",
    table="sensor.HumidityReadings",
).run()
