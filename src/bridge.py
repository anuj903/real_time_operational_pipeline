# src/bridge.py
import json
import time
from src.mqtt_client import MQTTClient
from src.kafka_producer import KafkaProducerClient


class MQTTKafkaBridge:
    def __init__(self, mqtt_cfg, kafka_cfg):
        self.kafka = KafkaProducerClient(kafka_cfg)
        self.mqtt = MQTTClient(mqtt_cfg, self.on_mqtt_message)

        # Build MQTT → Kafka routing map from config
        topics_cfg = kafka_cfg["topics"]

        self.mqtt_to_kafka = {
            "esp32/ultrasonic/distance": topics_cfg["distance"],
            "esp32/dht11/temperature": topics_cfg["temperature"],
            "esp32/dht11/humidity": topics_cfg["humidity"],
        }

        print("[Bridge] MQTT → Kafka routing:")
        for m, k in self.mqtt_to_kafka.items():
            print(f"  {m}  →  {k}")

    def on_mqtt_message(self, client, userdata, msg):
        raw = msg.payload.decode(errors="replace").strip()

        # Try JSON first, fallback to raw value
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"value": raw}

        kafka_topic = self.mqtt_to_kafka.get(msg.topic)

        if not kafka_topic:
            print(f"[Bridge] Ignoring unmapped MQTT topic: {msg.topic}")
            return

        event = {
            "topic": msg.topic,
            "payload": payload,
            "ts": int(time.time() * 1000)
        }

        print(f"[Bridge] {msg.topic} → {kafka_topic} | {event}")

        self.kafka.send(kafka_topic, event)

    def start(self):
        try:
            self.mqtt.start()
        finally:
            self.kafka.close()
