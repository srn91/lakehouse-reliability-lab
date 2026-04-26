from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def build_spark_session(app_name: str = "lakehouse-reliability-lab") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _write_parquet(dataframe, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    dataframe.write.mode("overwrite").parquet(str(destination))


def run_pipeline(input_glob: str, output_root: Path) -> dict[str, str]:
    spark = build_spark_session()
    try:
        raw_orders = (
            spark.read.option("header", True).csv(input_glob)
            .withColumn("source_file", F.input_file_name())
            .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
            .withColumn("event_ts", F.to_timestamp("event_ts"))
            .withColumn("ingestion_ts", F.to_timestamp("ingestion_ts"))
        )

        bronze_orders_path = output_root / "bronze" / "bronze_orders.parquet"
        _write_parquet(raw_orders, bronze_orders_path)

        dedupe_window = Window.partitionBy("event_id").orderBy(
            F.col("ingestion_ts").desc(),
            F.col("event_ts").desc(),
        )
        silver_orders = (
            raw_orders.where(
                F.col("event_id").isNotNull()
                & F.col("order_id").isNotNull()
                & F.col("customer_id").isNotNull()
                & F.col("region").isNotNull()
                & F.col("status").isNotNull()
            )
            .withColumn("dedupe_rank", F.row_number().over(dedupe_window))
            .where(F.col("dedupe_rank") == 1)
            .drop("dedupe_rank")
            .withColumn("region", F.lower("region"))
            .withColumn("status", F.lower("status"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("ingestion_date", F.to_date("ingestion_ts"))
        )
        silver_orders_path = output_root / "silver" / "silver_orders.parquet"
        _write_parquet(silver_orders, silver_orders_path)

        latest_window = Window.partitionBy("order_id").orderBy(
            F.col("event_ts").desc(),
            F.col("ingestion_ts").desc(),
        )
        silver_latest_order_state = (
            silver_orders.withColumn("latest_rank", F.row_number().over(latest_window))
            .where(F.col("latest_rank") == 1)
            .drop("latest_rank", "source_file")
        )
        silver_latest_order_state_path = output_root / "silver" / "silver_latest_order_state.parquet"
        _write_parquet(silver_latest_order_state, silver_latest_order_state_path)

        gold_daily_region_sales = (
            silver_latest_order_state.where(F.col("status") == F.lit("delivered"))
            .groupBy("event_date", "region")
            .agg(
                F.count("*").alias("delivered_orders"),
                F.sum("order_amount").cast("decimal(12,2)").alias("delivered_revenue"),
            )
            .orderBy("event_date", "region")
        )
        gold_daily_region_sales_path = output_root / "gold" / "gold_daily_region_sales.parquet"
        _write_parquet(gold_daily_region_sales, gold_daily_region_sales_path)

        gold_customer_order_metrics = (
            silver_latest_order_state.groupBy("customer_id")
            .agg(
                F.count("*").alias("active_orders"),
                F.sum(F.when(F.col("status") == "delivered", F.lit(1)).otherwise(F.lit(0)))
                .cast("bigint")
                .alias("delivered_orders"),
                F.sum(
                    F.when(F.col("status") == "delivered", F.col("order_amount")).otherwise(
                        F.lit(0).cast("decimal(12,2)")
                    )
                )
                .cast("decimal(12,2)")
                .alias("delivered_revenue"),
                F.max("event_ts").alias("latest_order_event_ts"),
            )
            .orderBy("customer_id")
        )
        gold_customer_order_metrics_path = output_root / "gold" / "gold_customer_order_metrics.parquet"
        _write_parquet(gold_customer_order_metrics, gold_customer_order_metrics_path)
    finally:
        spark.stop()

    return {
        "bronze_orders": str(bronze_orders_path),
        "silver_orders": str(silver_orders_path),
        "silver_latest_order_state": str(silver_latest_order_state_path),
        "gold_daily_region_sales": str(gold_daily_region_sales_path),
        "gold_customer_order_metrics": str(gold_customer_order_metrics_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Local Spark execution path for lakehouse-reliability-lab")
    parser.add_argument("--input-glob", default="data/raw/*.csv")
    parser.add_argument("--output-root", default="warehouse_spark")
    args = parser.parse_args()

    outputs = run_pipeline(args.input_glob, Path(args.output_root))
    for name, path in outputs.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
