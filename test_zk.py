"""Teste la connexion à la pointeuse."""
from zk import ZK

import config

conn = None
try:
    zk = ZK(config.POINTEUSE_IP, port=config.POINTEUSE_PORT, timeout=5)
    conn = zk.connect()
    print("Connexion réussie !")
    print("Firmware :", conn.get_firmware_version())
    print("N° de série :", conn.get_serialnumber())
except Exception as e:
    print("Erreur :", e)
finally:
    if conn:
        conn.disconnect()
