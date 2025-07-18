import pandas as pd
from dagster import asset, Output, AssetIn, Definitions
from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation
from assets.silver_layer import dim_products, fact_sales
from assets.gold_layer import sales_values_by_category
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager

@asset(
    ins={
        "sales_values_by_category": AssetIn(key_prefix=["gold", "ecom"])
    },
    io_manager_key="psql_io_manager",
    key_prefix=["warehouse", "ecom"],
    compute_kind="MinIO",
)

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "database": "brazillian_ecommerce",
    "user": "admin",
    "password": "admin123",
}

MINIO_CONFIG = {
    "endpoint_url": "localhost:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
}

PSQL_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "postgres",
    "user": "admin",
    "password": "admin123",
}

defs = Definitions(
    assets=[
        # Bronze
        bronze_olist_orders_dataset,
        bronze_olist_order_items_dataset,
        bronze_olist_order_payments_dataset,
        bronze_olist_products_dataset,
        bronze_product_category_name_translation,
        # Silver
        dim_products,
        fact_sales,
        # Gold
        sales_values_by_category,
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
