# run_distance_sink.py
from consumers.kafka_to_sql import KafkaToSQLConsumer
from src.config_loader import load_yaml

kafka_cfg = load_yaml("config/kafka.yaml")["kafka"]
sql_cfg = load_yaml("config/sqlserver.yaml")["sqlserver"]


KafkaToSQLConsumer(
    kafka_cfg=kafka_cfg,
    sql_cfg=sql_cfg,
    topic="sensor.distance",
    group_id="sql-distance-sink",
    value_field="distance_m",   # or "cm" if that’s your payload
    table="sensor.DistanceReadings",
).run()

