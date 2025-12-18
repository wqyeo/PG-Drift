from __future__ import annotations
from typing import List

import os

class PgConfig:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    @staticmethod
    def load_from_env() -> List[PgConfig]:
        configs = []

        db_count = int(os.getenv("DB_COUNT", "1"))
        for i in range(1, db_count + 1):
            host = os.getenv(f"PG_DRIFT_DB_HOST_{i}", "localhost")
            port = int(os.getenv(f"PG_DRIFT_DB_PORT_{i}", "5432"))
            user = os.getenv(f"PG_DRIFT_DB_USER_{i}", "postgres")
            password = os.getenv(f"PG_DRIFT_DB_PASSWORD_{i}", "password")

            config = PgConfig(host, port, user, password)
            configs.append(config)
        return configs