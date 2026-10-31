"""Silver: clean, validate, de-duplicate and upsert (MERGE). Bad rows go to a quarantine table."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import transformations as T
from .config import PipelineConfig
from .logger import get_logger
from .utils import total_input_rows

log = get_logger(__name__)


def _ensure_tables(spark: SparkSession, cfg: PipelineConfig) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.silver_table} (
            order_id STRING, customer_id STRING, product_id STRING,
            quantity INT, unit_price DECIMAL(18,2), total_amount DECIMAL(18,2),
            order_ts TIMESTAMP, order_date DATE, status STRING, country STRING,
            _ingest_ts TIMESTAMP, _source_file STRING, _updated_ts TIMESTAMP
        ) USING DELTA
        TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true,
                       delta.enableChangeDataFeed = true)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.quarantine_table} (
            _raw_record STRING, _dq_failures ARRAY<STRING>, _source_file STRING, _quarantined_ts TIMESTAMP
        ) USING DELTA
    """)


def _make_batch_processor(cfg: PipelineConfig, counters: dict):
    def process(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        cleaned = T.clean_orders(batch_df).persist()
        try:
            valid, invalid = T.apply_quality_rules(cleaned)

            bad = invalid.select(
                "_raw_record", "_dq_failures", "_source_file", F.current_timestamp().alias("_quarantined_ts")
            )
            bad_count = bad.count()
            if bad_count:
                bad.write.mode("append").saveAsTable(cfg.quarantine_table)

            good = (
                T.dedupe_latest(valid)
                .withColumn("_updated_ts", F.current_timestamp())
                .select(*T.SILVER_COLUMNS)
            )
            (
                DeltaTable.forName(spark, cfg.silver_table).alias("t")
                .merge(good.alias("s"), "t.order_id = s.order_id")
                .whenMatchedUpdateAll(condition="s.order_ts >= t.order_ts")
                .whenNotMatchedInsertAll()
                .execute()
            )
            counters["quarantined"] += bad_count
            log.info("silver batch %s done (quarantined=%s)", batch_id, bad_count)
        finally:
            cleaned.unpersist()

    return process


def run_silver(spark: SparkSession, cfg: PipelineConfig, metrics: dict) -> None:
    _ensure_tables(spark, cfg)
    counters = {"quarantined": 0}

    query = (
        spark.readStream.table(cfg.bronze_table)
        .writeStream.foreachBatch(_make_batch_processor(cfg, counters))
        .option("checkpointLocation", f"{cfg.checkpoint_root}/silver")
        .queryName("silver_orders")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    metrics["rows_read"] = total_input_rows(query)
    metrics["rows_quarantined"] = counters["quarantined"]
