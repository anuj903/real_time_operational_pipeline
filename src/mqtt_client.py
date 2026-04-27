import paho.mqtt.client as mqtt
import yaml
from pathlib import Path

class MQTTClient:
    def __init__(self, cfg: dict, on_msg_cb):
        self.host = cfg["broker_ip"]
        self.port = int(cfg.get("broker_port", 1883))

        # NEW: list of topics from yaml
        self.topics = cfg["topics"]

        self.username = cfg.get("username") or None
        self.password = cfg.get("password") or None
        self.client_id = cfg.get("client_id", "bridge-mqtt-client")

        self._cb = on_msg_cb
        self.client = mqtt.Client(client_id=self.client_id, clean_session=True)

        if self.username:
            self.client.username_pw_set(self.username, self.password)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._cb

    def _on_connect(self, client, userdata, flags, rc):
        print(f"[MQTT] Connected rc={rc}")

        subs = []
        for t in self.topics:
            subs.append((t["name"], int(t.get("qos", 0))))

        client.subscribe(subs)

        print("[MQTT] Subscribed topics:")
        for t, q in subs:
            print(f"  - {t} (qos={q})")

    def _on_disconnect(self, client, userdata, rc):
        print(f"[MQTT] Disconnected rc={rc} — reconnecting will be handled by loop")

    def start(self):
        print(f"[MQTT] Connecting to {self.host}:{self.port} …")
        self.client.connect(self.host, self.port, keepalive=60)
        # robust auto-reconnect loop
        self.client.loop_forever(retry_first_connection=True)
