"""Affiche le nombre de pointages présents dans Oracle."""
import oracledb

import config

connection = oracledb.connect(
    user=config.ORACLE_USER, password=config.ORACLE_PASSWORD, dsn=config.ORACLE_DSN
)
cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM pointage")
print(f"Nombre total de pointages dans Oracle : {cursor.fetchone()[0]}")
connection.close()
