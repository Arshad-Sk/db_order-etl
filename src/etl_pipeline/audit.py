from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from .config import PipelineConfig
from .logger import get_logger

log = get_logger(__name__)

_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("task", T.StringType()),
    T.StructField("env", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("start_ts", T.TimestampType()),
    T.StructField("end_ts", T.TimestampType()),
    T.StructField("duration_sec", T.DoubleType()),
    T.StructField("metrics", T.MapType(T.StringType(), T.LongType())),
    T.StructField("error", T.StringType()),
])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _write_audit(spark: SparkSession, cfg: PipelineConfig, status, start, end, metrics, error) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg.audit_table} (
            run_id STRING, task STRING, env STRING, status STRING,
            start_ts TIMESTAMP, end_ts TIMESTAMP, duration_sec DOUBLE,
            metrics MAP<STRING, BIGINT>, error STRING
        ) USING DELTA
    """)
    row = (cfg.run_id, cfg.task, cfg.env, status, start, end, (end - start).total_seconds(),
           {k: int(v) for k, v in metrics.items()}, error)
    spark.createDataFrame([row], _SCHEMA).write.mode("append").saveAsTable(cfg.audit_table)


@contextmanager
def audited_task(spark: SparkSession, cfg: PipelineConfig):
    """Wrap a task: logs, records an audit row (SUCCESS/FAILED) and re-raises errors."""
    start, metrics, status, error = _utcnow(), {}, "SUCCESS", None
    log.info("Task '%s' started (run_id=%s, env=%s)", cfg.task, cfg.run_id, cfg.env)
    try:
        yield metrics
    except Exception as exc:  # noqa: BLE001
        status, error = "FAILED", f"{type(exc).__name__}: {exc}"[:4000]
        log.exception("Task '%s' failed", cfg.task)
        raise
    finally:
        try:
            _write_audit(spark, cfg, status, start, _utcnow(), metrics, error)
        except Exception:  # noqa: BLE001  (never mask the real error)
            log.exception("Could not write audit record")
        log.info("Task '%s' finished with status=%s metrics=%s", cfg.task, status, metrics)
