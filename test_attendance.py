"""Affiche les pointages stockés dans la pointeuse."""
from zk import ZK

import config

zk = ZK(config.POINTEUSE_IP, port=config.POINTEUSE_PORT, timeout=5)
conn = zk.connect()
for record in conn.get_attendance():
    print(record.user_id, record.timestamp, record.punch, record.status)
conn.disconnect()
