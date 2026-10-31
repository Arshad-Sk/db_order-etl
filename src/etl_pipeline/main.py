from pyspark.sql import SparkSession

from .audit import audited_task
from .bronze import run_bronze
from .config import parse_args
from .gold import run_gold
from .logger import get_logger
from .silver import run_silver
from .utils import ensure_schemas
from .validate import run_validate

log = get_logger(__name__)

TASK_REGISTRY = {
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
    "validate": run_validate,
}


def main(argv: list[str] | None = None) -> None:
    cfg = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    ensure_schemas(spark, cfg)
    with audited_task(spark, cfg) as metrics:
        TASK_REGISTRY[cfg.task](spark, cfg, metrics)


if __name__ == "__main__":
    main()
