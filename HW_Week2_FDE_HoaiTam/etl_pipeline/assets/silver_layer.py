import pandas as pd
from dagster import asset, Output, Definitions, AssetIn, multi_asset,AssetOut
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation

@asset(
    ins={
        "bronze_olist_products_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_product_category_name_translation": AssetIn(key_prefix=["bronze", "ecom"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="MinIO",
)
def dim_products(
    bronze_olist_products_dataset,
    bronze_product_category_name_translation
) -> Output[pd.DataFrame]:
    df = bronze_olist_products_dataset.merge(
        bronze_product_category_name_translation,
        on="product_category_name",
        how="left"
    )[['product_id', 'product_category_name_english']]

    return Output(df, metadata={
        "table": "dim_products",
        "records_count": len(df),
    })

@asset(
    ins={
        "bronze_olist_orders_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_order_items_dataset": AssetIn(key_prefix=["bronze", "ecom"]),
        "bronze_olist_order_payments_dataset": AssetIn(key_prefix=["bronze", "ecom"])
    },
    io_manager_key="minio_io_manager",
    key_prefix=["silver", "ecom"],
    compute_kind="MinIO",
)
def fact_sales(
    bronze_olist_orders_dataset,
    bronze_olist_order_items_dataset,
    bronze_olist_order_payments_dataset
) -> Output[pd.DataFrame]:
    df = (
        bronze_olist_orders_dataset
        .merge(bronze_olist_order_items_dataset,on="order_id",how="left")
        .merge(bronze_olist_order_payments_dataset,on="order_id",how="left")
        [['order_id','customer_id', 'order_purchase_timestamp','product_id','payment_value','order_status']]
    )

    return Output(df, metadata={
        "table": "fact_sales",
        "records_count": len(df),
    })


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

defs = Definitions(
    assets=[
        bronze_olist_orders_dataset,
        bronze_product_category_name_translation,
        bronze_olist_products_dataset,
        bronze_olist_order_payments_dataset,
        bronze_olist_order_items_dataset,
        dim_products,
        fact_sales
    ],
    resources={
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG)
    },
)