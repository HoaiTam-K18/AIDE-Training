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
        upstream_asset_key = context.asset_key_for_input(context.input_name)
        schema = upstream_asset_key.path[-2] if len(upstream_asset_key.path) >= 2 else None
        table = upstream_asset_key.path[-1]

        with connect_psql(self._config) as engine:
            df = pd.read_sql_table(table_name=table, con=engine, schema=schema)
        context.log.info(f"📥 Đã đọc {len(df)} bản ghi từ PostgreSQL bảng {schema}.{table}")
        return df

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema = context.asset_key.path[-2] if len(context.asset_key.path) >= 2 else None
        table = context.asset_key.path[-1]

        with connect_psql(self._config) as engine:
            obj.to_sql(
                name=table,
                con=engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
            )
        context.log.info(f"✅ Đã ghi {len(obj)} bản ghi vào PostgreSQL bảng {schema}.{table}")

