"""Configuration centralisée, lue depuis les variables d'environnement (.env)."""
import os
from dotenv import load_dotenv

load_dotenv()

# Pointeuse biométrique (ZKTeco)
POINTEUSE_IP = os.getenv("POINTEUSE_IP", "192.168.1.100")
POINTEUSE_PORT = int(os.getenv("POINTEUSE_PORT", "4370"))

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "pointages")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "pointage-group")

# Oracle
ORACLE_USER = os.getenv("ORACLE_USER", "")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_DSN = os.getenv("ORACLE_DSN", "localhost:1522/xepdb1")

# Fichier d'état (anti-doublons côté producer)
STATE_FILE = os.getenv("STATE_FILE", "dernier_timestamp.json")
