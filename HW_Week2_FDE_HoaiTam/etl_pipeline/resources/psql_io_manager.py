from contextlib import contextmanager

import pandas as pd
from dagster import IOManager, InputContext, OutputContext, io_manager
from sqlalchemy import create_engine

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    # TODO: your code here
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2],context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
        # insert new data
            pass

@io_manager(
    config_schema={
        "user": str,
        "password": str,
        "host": str,
        "port": int,
        "database": str,
    }
)
def postgres_io_manager(init_context):
    return PostgreSQLIOManager(init_context.resource_config)
