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
    name="bronze_olist_orders_dataset",
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    partitions_def=DailyPartitionsDefinition(start_date="2025-07-15")
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
    table = "olist_orders_dataset"
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
            key=["bronze", "ecom", "bronze_olist_orders_dataset"],
        )
    },
    outs={
        f"olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id",
                    "customer_id",
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
    partitions_def=DailyPartitionsDefinition(start_date="2025-07-15")
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