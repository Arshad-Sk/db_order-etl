from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass

TASKS = ("bronze", "silver", "gold", "validate")


@dataclass(frozen=True)
class PipelineConfig:
    task: str
    env: str
    catalog: str
    storage_account: str
    run_id: str
    source_name: str = "orders"
    landing_container: str = "landing"
    lakehouse_container: str = "lakehouse"
    source_format: str = "json"
    lookback_days: int = 7
    full_refresh: bool = False
    max_quarantine_ratio: float = 0.2

    # ---- ADLS paths -------------------------------------------------------
    @property
    def landing_path(self) -> str:
        return f"abfss://{self.landing_container}@{self.storage_account}.dfs.core.windows.net/{self.source_name}"

    @property
    def checkpoint_root(self) -> str:
        return (
            f"abfss://{self.lakehouse_container}@{self.storage_account}.dfs.core.windows.net"
            f"/_checkpoints/{self.env}/{self.source_name}"
        )

    # ---- Unity Catalog tables --------------------------------------------
    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    @property
    def bronze_table(self) -> str:
        return self.table("bronze", self.source_name)

    @property
    def silver_table(self) -> str:
        return self.table("silver", self.source_name)

    @property
    def quarantine_table(self) -> str:
        return self.table("silver", f"{self.source_name}_quarantine")

    @property
    def gold_table(self) -> str:
        return self.table("gold", "daily_sales")

    @property
    def audit_table(self) -> str:
        return self.table("ops", "pipeline_audit")


def parse_args(argv: list[str] | None = None) -> PipelineConfig:
    p = argparse.ArgumentParser(description="Orders ETL pipeline")
    p.add_argument("--task", required=True, choices=TASKS)
    p.add_argument("--env", required=True)
    p.add_argument("--catalog", required=True)
    p.add_argument("--storage_account", required=True)
    p.add_argument("--run_id", default=str(uuid.uuid4()))
    p.add_argument("--source_name", default="orders")
    p.add_argument("--source_format", default="json")
    p.add_argument("--lookback_days", type=int, default=7)
    p.add_argument("--full_refresh", action="store_true")
    p.add_argument("--max_quarantine_ratio", type=float, default=0.2)
    a, _ = p.parse_known_args(argv)
    return PipelineConfig(**vars(a))
