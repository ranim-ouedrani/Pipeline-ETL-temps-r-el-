"""Producer temps réel : écoute la pointeuse en continu (live_capture) et publie dans Kafka."""
import json

from kafka import KafkaProducer
from zk import ZK

import config

producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

zk = ZK(config.POINTEUSE_IP, port=config.POINTEUSE_PORT, timeout=5)
conn = zk.connect()
conn.enable_device()

print("En écoute en temps réel... (Ctrl+C pour arrêter)")
for attendance in conn.live_capture():
    if attendance:
        event = {
            "user_id": attendance.user_id,
            "timestamp": attendance.timestamp.isoformat(),
            "status": attendance.status,
            "punch": attendance.punch,
        }
        producer.send(config.KAFKA_TOPIC, value=event)
        print("Nouveau pointage détecté et envoyé :", event)
