"""Crée la table pointage dans Oracle (supprime l'ancienne si elle existe)."""
import oracledb

import config

connection = oracledb.connect(
    user=config.ORACLE_USER, password=config.ORACLE_PASSWORD, dsn=config.ORACLE_DSN
)
cursor = connection.cursor()

try:
    cursor.execute("DROP TABLE pointage")
    print("Ancienne table supprimée.")
except oracledb.DatabaseError:
    pass

cursor.execute("""
    CREATE TABLE pointage (
        id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        user_id VARCHAR2(20),
        timestamp_pointage TIMESTAMP,
        punch NUMBER,
        status NUMBER,
        CONSTRAINT uniq_pointage UNIQUE (user_id, timestamp_pointage, punch)
    )
""")

connection.commit()
print("Table créée avec succès (avec contrainte anti-doublons).")
connection.close()
