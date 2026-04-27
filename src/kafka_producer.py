from kafka import KafkaProducer
import json, time

class KafkaProducerClient:
    def __init__(self, cfg: dict):
        self.bootstrap_servers = cfg["bootstrap_servers"]

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks=cfg.get("acks", 1),
            retries=cfg.get("retries", 10),
            linger_ms=cfg.get("linger_ms", 50),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        print(f"[Kafka] Producer connected to {self.bootstrap_servers}")

    def send(self, topic: str, data: dict):
        future = self.producer.send(topic, data)
        future.add_errback(
            lambda e: print(f"[Kafka] async send error (topic={topic}): {e!r}")
        )

    def close(self):
        self.producer.flush(timeout=15)
        self.producer.close(timeout=5)
        print("[Kafka] Producer closed")

# class KafkaProducerClient:
#     def __init__(self, cfg):
#         self.topic = cfg["topic"]
#         print(f"[DEBUG] Kafka topic = {self.topic}")

#         self.bs = cfg["bootstrap_servers"]
#         print(f"[DEBUG] Kafka bootstrap_servers = {self.bs}")

#         self.producer = KafkaProducer(
#             bootstrap_servers=self.bs,
#             acks=1,
#             retries=10,
#             retry_backoff_ms=500,
#             request_timeout_ms=30000,
#             max_block_ms=60000,
#             linger_ms=50,
#             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
#         )

#         # Fail fast if topic metadata is missing
#         parts = self.producer.partitions_for(self.topic)
#         if not parts:
#             raise RuntimeError(f"No metadata for topic '{self.topic}' at 172.21.0.194:9092")
#         print(f"[Kafka] Topic {self.topic} partitions: {sorted(parts)}")

#         self._last_flush = time.time()

#     def send(self, data):
#         self.producer.send(self.topic, data).add_errback(
#             lambda e: print(f"[Kafka] async send error: {e!r}")
#     )

#         # periodic flush; give generous timeout so it doesn't raise
#         #now = time.time()
#         #if now - self._last_flush >= 5.0:
#         #    try:
#         #        self.producer.flush(timeout=10)   # longer grace
#         #    except Exception as e:
#         #        print(f"[Kafka] flush warning: {e!r}")
#         #      self._last_flush = now

#     def close(self):
#         try:
#             self.producer.flush(timeout=15)
#         finally:
#             self.producer.close(timeout=5)
