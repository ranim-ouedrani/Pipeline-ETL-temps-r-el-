"""Producer : lit les pointages de la pointeuse (par IP) et les publie dans Kafka."""
import json
import os

from kafka import KafkaProducer
from zk import ZK

import config

producer = KafkaProducer(
    bootstrap_servers=config.KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def get_dernier_timestamp():
    if os.path.exists(config.STATE_FILE):
        with open(config.STATE_FILE) as f:
            return json.load(f)["dernier"]
    return None


def sauvegarder_dernier_timestamp(ts):
    with open(config.STATE_FILE, "w") as f:
        json.dump({"dernier": ts.isoformat()}, f)


def envoyer_pointages():
    zk = ZK(config.POINTEUSE_IP, port=config.POINTEUSE_PORT, timeout=15)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()

        dernier_ts = get_dernier_timestamp()
        attendances = conn.get_attendance()

        nouveaux = [
            a for a in attendances
            if dernier_ts is None or a.timestamp.isoformat() > dernier_ts
        ]

        if nouveaux:
            nouveaux.sort(key=lambda x: x.timestamp)
            for a in nouveaux:
                message = {
                    "user_id": a.user_id,
                    "timestamp": a.timestamp.isoformat(),
                    "status": a.status,
                    "punch": a.punch,
                }
                producer.send(config.KAFKA_TOPIC, message)
                print(f"Envoyé : {message}")

            producer.flush()
            sauvegarder_dernier_timestamp(nouveaux[-1].timestamp)
        else:
            print("Aucun nouveau pointage.")

    except Exception as e:
        print(f"Erreur : {e}")

    finally:
        if conn:
            try:
                conn.enable_device()
                conn.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    envoyer_pointages()
