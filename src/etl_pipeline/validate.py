"""Post-load data-quality gate. Raises (-> job fails -> alert) if any check fails."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig
from .logger import get_logger

log = get_logger(__name__)


class DataQualityError(Exception):
    pass


def run_validate(spark: SparkSession, cfg: PipelineConfig, metrics: dict) -> None:
    silver = spark.read.table(cfg.silver_table)
    gold = spark.read.table(cfg.gold_table)
    results: list[tuple[str, bool, str]] = []

    silver_rows = silver.count()
    results.append(("silver_not_empty", silver_rows > 0, f"rows={silver_rows}"))

    dups = silver.groupBy("order_id").count().filter("count > 1").count()
    results.append(("silver_unique_order_id", dups == 0, f"duplicate_keys={dups}"))

    gold_rows = gold.count()
    results.append(("gold_not_empty", gold_rows > 0, f"rows={gold_rows}"))

    neg = gold.filter((F.col("revenue") < 0) | (F.col("units_sold") < 0)).count()
    results.append(("gold_no_negative_values", neg == 0, f"bad_rows={neg}"))

    quarantined = (
        spark.read.table(cfg.quarantine_table).filter(F.col("_quarantined_ts") >= F.current_date()).count()
    )
    processed = silver.filter(F.col("_updated_ts") >= F.current_date()).count()
    total = quarantined + processed
    ratio = (quarantined / total) if total else 0.0
    results.append((
        "quarantine_ratio_ok", ratio <= cfg.max_quarantine_ratio,
        f"ratio={ratio:.3f} (max {cfg.max_quarantine_ratio})",
    ))

    for name, ok, detail in results:
        log.info("DQ %-28s %s  %s", name, "PASS" if ok else "FAIL", detail)
    metrics["checks_passed"] = sum(ok for _, ok, _ in results)
    metrics["checks_failed"] = sum(not ok for _, ok, _ in results)

    failed = [f"{n} ({d})" for n, ok, d in results if not ok]
    if failed:
        raise DataQualityError("Data quality checks failed: " + "; ".join(failed))
