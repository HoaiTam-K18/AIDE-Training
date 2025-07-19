import pandas as pd
from dagster import asset, multi_asset, Output, AssetIn, AssetOut, Definitions
from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation
from assets.silver_layer import dim_products, fact_sales
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager

@asset(
    ins={
        "fact_sales": AssetIn(key_prefix=["silver", "ecom"]),
        "dim_products": AssetIn(key_prefix=["silver", "ecom"]),
    },
    io_manager_key="minio_io_manager", 
    key_prefix=["gold", "ecom"],
    compute_kind="MinIO",
)
def gold_sales_values_by_category(fact_sales: pd.DataFrame, dim_products: pd.DataFrame) -> Output[pd.DataFrame]:
    # Tính doanh thu & số hóa đơn theo ngày và sản phẩm
    daily = fact_sales.copy()
    daily["order_purchase_timestamp"] = pd.to_datetime(daily["order_purchase_timestamp"])
    daily["daily"] = daily["order_purchase_timestamp"].dt.date
    df = (
        daily[daily["order_status"] == "delivered"]
        .groupby(["daily", "product_id"])
        .agg(sales=("payment_value", "sum"), bills=("order_id", "nunique"))
        .reset_index()
    )
    df["sales"] = df["sales"].round(2)

    # Thêm thông tin category từ dim_product
    df = df.merge(dim_products, on="product_id", how="left")

    # Tính tổng theo tháng & danh mục
    df["monthly"] = pd.to_datetime(df["daily"]).dt.to_period("M").dt.strftime("%Y-%m")
    grouped = (
        df.groupby(["monthly", "product_category_name_english"])
        .agg(
            total_sales=("sales", "sum"),
            total_bills=("bills", "sum"),
        )
        .reset_index()
    )
    grouped["values_per_bills"] = grouped["total_sales"] / grouped["total_bills"]

    return Output(
        grouped,
        metadata={
            "table": "sales_values_by_category",
            "records_count": len(grouped),
        },
    )

@multi_asset(
    ins={
        "gold_sales_values_by_category": AssetIn(
            key_prefix=["gold", "ecom"],
        )
    },
    outs={
        "sales_values_by_category": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "gold"],
        )
    },
    compute_kind="PostgreSQL"
)
def sales_values_by_category(gold_sales_values_by_category) -> Output[pd.DataFrame]:
    return Output(
        gold_sales_values_by_category,
        metadata={
            "schema": "gold",
            "table": "sales_values_by_category",
            "records counts": len(gold_sales_values_by_category),
        },
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
        gold_sales_values_by_category,

        # Warehouse
        sales_values_by_category
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)
