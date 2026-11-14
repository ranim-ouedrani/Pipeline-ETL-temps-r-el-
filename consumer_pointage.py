"""Consumer : lit le topic Kafka et insère les pointages dans Oracle."""
import json

import oracledb
from kafka import KafkaConsumer

import config

consumer = KafkaConsumer(
    config.KAFKA_TOPIC,
    bootstrap_servers=config.KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id=config.KAFKA_GROUP_ID,
    consumer_timeout_ms=5000,  # s'arrête après 5 s sans nouveau message
)

connection = oracledb.connect(
    user=config.ORACLE_USER,
    password=config.ORACLE_PASSWORD,
    dsn=config.ORACLE_DSN,
)
cursor = connection.cursor()

aucun_message = True

for message in consumer:
    aucun_message = False
    event = message.value
    try:
        cursor.execute("""
            INSERT INTO pointage (user_id, timestamp_pointage, punch, status)
            VALUES (:1, TO_TIMESTAMP(:2, 'YYYY-MM-DD"T"HH24:MI:SS'), :3, :4)
        """, [event["user_id"], event["timestamp"], event["punch"], event["status"]])
        connection.commit()
        print("Inséré dans Oracle :", event)
    except oracledb.IntegrityError:
        connection.rollback()  # doublon ignoré (contrainte d'unicité)

if aucun_message:
    print("Aucun nouveau pointage.")

consumer.close()
connection.close()
