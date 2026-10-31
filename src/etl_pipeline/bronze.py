"""Bronze: incremental, exactly-once raw ingestion from ADLS landing using Auto Loader."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig
from .logger import get_logger
from .utils import total_input_rows

log = get_logger(__name__)


def run_bronze(spark: SparkSession, cfg: PipelineConfig, metrics: dict) -> None:
    ckpt = f"{cfg.checkpoint_root}/bronze"
    log.info("Ingesting %s -> %s", cfg.landing_path, cfg.bronze_table)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", cfg.source_format)
        .option("cloudFiles.schemaLocation", f"{ckpt}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        # New columns => stream stops once, schema is updated, job retry picks it up.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.includeExistingFiles", "true")
        # For very high file volumes switch to event-driven discovery:
        # .option("cloudFiles.useNotifications", "true")
        .load(cfg.landing_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        stream.writeStream.format("delta")
        .option("checkpointLocation", f"{ckpt}/checkpoint")
        .option("mergeSchema", "true")
        .queryName("bronze_orders")
        .trigger(availableNow=True)
        .toTable(cfg.bronze_table)
    )
    query.awaitTermination()
    metrics["rows_ingested"] = total_input_rows(query)
