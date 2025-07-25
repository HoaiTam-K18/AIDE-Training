import pandas as pd
from dagster import (
    asset,
    multi_asset,
    AssetIn,
    AssetOut,
    Output,
    DailyPartitionsDefinition
)

@asset(
    name="bronze_olist_products_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_products_dataset(context) -> Output[pd.DataFrame]:
    table = "olist_products_dataset"
    sql_stm = f"SELECT * FROM {table}"

    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "ecom", "bronze_olist_products_dataset"],
        )
    },
    outs={
        f"olist_products_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "product_id"
                ],
                "columns": [
                    "product_id",
                    "product_category_name",
                    "product_name_lenght",
                    "product_description_lenght",
                    "product_photos_qty",
                    "product_weight_g",
                    "product_length_cm",
                    "product_height_cm",
                    "product_width_cm",
                ]
            },
        )
    },
    compute_kind="PostgreSQL",
    name="olist_products_dataset"
)
def dwh_olist_products_dataset(context, upstream) -> Output[pd.DataFrame]:
    context.log.info("Generate asset")
    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )

@asset(
    name="bronze_olist_orders_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2017-01-01")
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    table = "olist_orders_dataset"

    try:
        partition_date_str = context.asset_partition_key_for_output()
        context.log.info(f"Partition key: {partition_date_str}")
        # TODO: your code here, sql_stm query by partition?
        sql_stm = f"""
            SELECT * FROM {table}
            WHERE DATE(order_purchase_timestamp) = '{partition_date_str}'
        """
    except Exception:
        context.log.info(f"{table} has no partition key!")

    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "ecom", "bronze_olist_orders_dataset"],
        )
    },
    outs={
        f"olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id"
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ],
                "datetime_columns": [
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ]
            },
        )
    },
    compute_kind="PostgreSQL",
    name="olist_orders_dataset",
    partitions_def=DailyPartitionsDefinition(start_date="2017-01-01")
)
def dwh_olist_orders_dataset(context, upstream) -> Output[pd.DataFrame]:
    context.log.info("Generate asset")
    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )

@asset(
    name="bronze_olist_order_items_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_items_dataset(context) -> Output[pd.DataFrame]:
    table = "olist_order_items_dataset"
    sql_stm = f"SELECT * FROM {table}"


    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "ecom", "bronze_olist_order_items_dataset"],
        )
    },
    outs={
        f"olist_order_items_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id",
                    "order_item_id",
                    "product_id",
                    "seller_id"
                ],
                "columns": [
                    "order_id",
                    "order_item_id",
                    "product_id",
                    "seller_id",
                    "shipping_limit_date",
                    "price",
                    "freight_value",
                    "created_at",
                    "updated_at"
                ],
                "datetime_columns": [
                    "shipping_limit_date",
                    "created_at",
                    "updated_at"
                ]
            },
        )
    },
    compute_kind="PostgreSQL",
    name="olist_order_items_dataset"
)
def dwh_olist_order_items_dataset(context, upstream: pd.DataFrame) -> Output[pd.DataFrame]:
    context.log.info("Generate asset")

    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )


@asset(
    name="bronze_olist_order_payments_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL"
)
def bronze_olist_order_payments_dataset(context) -> Output[pd.DataFrame]:
    table = "olist_order_payments_dataset"
    sql_stm = f"SELECT * FROM {table}"

    try:
        partition_date_str = context.asset_partition_key_for_output()
        # TODO: your code here, sql_stm query by partition?
    except Exception:
        context.log.info(f"{table} has no partition key!")

    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(pd_data)


@multi_asset(
    ins={
        "upstream": AssetIn(
            key=["bronze", "ecom", "bronze_olist_order_payments_dataset"],
        )
    },
    outs={
        f"olist_order_payments_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id",
                    "payment_sequential",
                ],
                "columns": [
                    "order_id",
                    "payment_sequential",
                    "payment_type",
                    "payment_installments",
                    "payment_value"
                ]
            },
        )
    },
    compute_kind="PostgreSQL",
    name="olist_order_payments_dataset"
)
def dwh_olist_order_payments_dataset(context, upstream) -> Output[pd.DataFrame]:
    context.log.info("Generate asset")
    return Output(
        upstream,
        metadata={
            "schema": "public",
            "records counts": len(upstream),
        },
    )
