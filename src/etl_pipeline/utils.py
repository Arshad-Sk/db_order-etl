from pyspark.sql import SparkSession

from .config import PipelineConfig


def ensure_schemas(spark: SparkSession, cfg: PipelineConfig) -> None:
    for schema in ("bronze", "silver", "gold", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")


def total_input_rows(query) -> int:
    return int(sum(p.get("numInputRows", 0) for p in query.recentProgress))
