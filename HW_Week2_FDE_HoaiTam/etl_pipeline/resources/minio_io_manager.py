import os
import pandas as pd
from datetime import datetime
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from contextlib import contextmanager
from typing import Union

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config["endpoint_url"],
        access_key=config["aws_access_key_id"],
        secret_key=config["aws_secret_access_key"],
        secure=False,
    )
    yield client

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        layer, schema, table = context.asset_key.path
        key = f"{layer}/{schema}/{table}.parquet"
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        tmp_dir = "/tmp"
        os.makedirs(tmp_dir, exist_ok=True)
        tmp_file_path = os.path.join(tmp_dir, f"{layer}-{schema}-{table}-{timestamp}.parquet")
        return key, tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        key_name, tmp = self._get_path(context)
        obj.to_parquet(tmp, index=False)
        
        with connect_minio(self._config) as client:
            bucket = self._config["bucket"]
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
            client.fput_object(bucket, key_name, tmp)
        
        os.remove(tmp)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        key_name, tmp = self._get_path(context)
        with connect_minio(self._config) as client:
            client.fget_object(self._config["bucket"], key_name, tmp)
        df = pd.read_parquet(tmp)
        os.remove(tmp)
        return df
