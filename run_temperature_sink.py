# run_temperature_sink.py
from consumers.kafka_to_sql import KafkaToSQLConsumer
from src.config_loader import load_yaml

kafka_cfg = load_yaml("config/kafka.yaml")["kafka"]
sql_cfg = load_yaml("config/sqlserver.yaml")["sqlserver"]

KafkaToSQLConsumer(
    kafka_cfg=kafka_cfg,
    sql_cfg=sql_cfg,
    topic="sensor.temperature",
    group_id="sql-temperature-sink",
    value_field="temperature_c",
    table="sensor.TemperatureReadings",
).run()
