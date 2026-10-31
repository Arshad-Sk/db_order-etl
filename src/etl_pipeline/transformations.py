"""Pure DataFrame transformations (no I/O) so they are easy to unit test."""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

ALLOWED_STATUS = ["PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
REQUIRED_RAW_COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts", "status", "country"]

SILVER_COLUMNS = [
    "order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount",
    "order_ts", "order_date", "status", "country", "_ingest_ts", "_source_file", "_updated_ts",
]


def _clean_str(name: str) -> Column:
    c = F.trim(F.col(name).cast("string"))
    return F.when(c == "", F.lit(None)).otherwise(c)


def clean_orders(df: DataFrame) -> DataFrame:
    """Type-cast, trim, standardise and enrich raw bronze records."""
    for col in REQUIRED_RAW_COLUMNS:  # tolerate missing columns -> they fail DQ rules instead of crashing
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))
    for col in ("_ingest_ts", "_source_file"):
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None))

    raw_cols = df.columns
    return (
        df.select(
            _clean_str("order_id").alias("order_id"),
            _clean_str("customer_id").alias("customer_id"),
            _clean_str("product_id").alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
            F.to_timestamp(F.col("order_ts")).alias("order_ts"),
            F.upper(_clean_str("status")).alias("status"),
            F.upper(_clean_str("country")).alias("country"),
            F.col("_ingest_ts").cast("timestamp").alias("_ingest_ts"),
            F.col("_source_file").cast("string").alias("_source_file"),
            F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("_raw_record"),
        )
        .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
        .withColumn("order_date", F.to_date("order_ts"))
    )


def get_quality_rules() -> dict[str, Column]:
    """rule name -> boolean Column that must be TRUE for a valid row."""
    return {
        "order_id_not_null": F.col("order_id").isNotNull(),
        "customer_id_not_null": F.col("customer_id").isNotNull(),
        "product_id_not_null": F.col("product_id").isNotNull(),
        "quantity_positive": F.col("quantity") > 0,
        "unit_price_non_negative": F.col("unit_price") >= 0,
        "order_ts_valid": F.col("order_ts").isNotNull(),
        "order_ts_not_in_future": F.col("order_ts") <= F.expr("current_timestamp() + INTERVAL 1 DAY"),
        "status_allowed": F.col("status").isin(ALLOWED_STATUS),
    }


def apply_quality_rules(df: DataFrame, rules: dict[str, Column] | None = None) -> tuple[DataFrame, DataFrame]:
    """Return (valid_df, invalid_df). invalid_df has an array column `_dq_failures`."""
    rules = rules or get_quality_rules()
    failures = F.array(*[F.when(~F.coalesce(expr, F.lit(False)), F.lit(name)) for name, expr in rules.items()])
    flagged = df.withColumn("_dq_failures", F.filter(failures, lambda x: x.isNotNull()))
    valid = flagged.filter(F.size("_dq_failures") == 0).drop("_dq_failures")
    invalid = flagged.filter(F.size("_dq_failures") > 0)
    return valid, invalid


def dedupe_latest(df: DataFrame, key: str = "order_id") -> DataFrame:
    """Keep the most recent version of each key within a batch."""
    w = Window.partitionBy(key).orderBy(F.col("order_ts").desc(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def build_daily_sales(silver: DataFrame) -> DataFrame:
    return (
        silver.filter(F.col("status") != "CANCELLED")
        .groupBy("order_date", "country", "product_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("total_amount").alias("revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_load_ts", F.current_timestamp())
    )
