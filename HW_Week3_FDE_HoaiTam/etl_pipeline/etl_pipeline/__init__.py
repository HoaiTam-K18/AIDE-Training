# etl_pipeline/__init__.py

from dagster import repository
from etl_pipeline.assets import my_first_asset  # giả sử bạn có job trong file này

@repository
def etl_pipeline():
    return [my_first_asset]
