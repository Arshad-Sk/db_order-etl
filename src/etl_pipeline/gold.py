"""Gold: business-level aggregate. Incremental via replaceWhere on a rolling look-back window."""
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig
from .logger import get_logger
from .transformations import build_daily_sales

log = get_logger(__name__)


def run_gold(spark: SparkSession, cfg: PipelineConfig, metrics: dict) -> None:
    silver = spark.read.table(cfg.silver_table)

    if cfg.full_refresh or not spark.catalog.tableExists(cfg.gold_table):
        log.info("Full (re)build of %s", cfg.gold_table)
        out = build_daily_sales(silver)
        out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.gold_table)
    else:
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=cfg.lookback_days)).isoformat()
        log.info("Incremental refresh of %s for order_date >= %s", cfg.gold_table, cutoff)
        out = build_daily_sales(silver.filter(F.col("order_date") >= F.lit(cutoff).cast("date")))
        (
            out.write.mode("overwrite")
            .option("replaceWhere", f"order_date >= '{cutoff}'")
            .saveAsTable(cfg.gold_table)
        )

    metrics["gold_rows_total"] = spark.read.table(cfg.gold_table).count()
