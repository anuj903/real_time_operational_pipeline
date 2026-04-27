import yaml
from src.bridge import MQTTKafkaBridge
from src.mqtt_client import MQTTClient
from src.config_loader import load_yaml


if __name__ == "__main__":
    mqtt_cfg = load_yaml("config/mqtt.yaml")["mqtt"]

    kafka_cfg = load_yaml("config/kafka.yaml")["kafka"]
    bridge = MQTTKafkaBridge(mqtt_cfg, kafka_cfg)
    bridge.start()
   