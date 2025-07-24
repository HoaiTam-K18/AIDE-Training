from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, InputContext, OutputContext, io_manager
from sqlalchemy import create_engine

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )
    engine = create_engine(conn_info)
    try:
        yield engine
    finally:
        engine.dispose()

class MySQLIOManager(IOManager):
    def __init__(self, config: dict):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        table = context.asset_key.path[-1]
        schema = self._config.get("schema", None)  # tùy chọn schema

        with connect_mysql(self._config) as engine:
            obj.to_sql(
                name=table,
                con=engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
            )
        context.log.info(f"Wrote {len(obj)} rows to MySQL table `{schema + '.' if schema else ''}{table}`")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        upstream = context.asset_key_for_input(context.input_name)
        table = upstream.path[-1]
        schema = self._config.get("schema", None)

        with connect_mysql(self._config) as engine:
            df = pd.read_sql_table(table_name=table, con=engine, schema=schema)
        context.log.info(f"Read {len(df)} rows from MySQL table `{schema + '.' if schema else ''}{table}`")
        return df

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as engine:
            return pd.read_sql_query(sql, engine)

