# Orders ETL – Azure Databricks + ADLS Gen2 (PySpark, Delta, Unity Catalog)

```
ADLS landing (json) ──Auto Loader──▶ bronze.orders ──MERGE+DQ──▶ silver.orders ──aggregate──▶ gold.daily_sales
                                                         └──▶ silver.orders_quarantine        │
                                         ops.pipeline_audit (every task) ◀────────────────────┘ validate (DQ gate)
```

## One-time setup
1. `./infra/provision_azure.sh dev` – storage account (HNS), containers `landing` + `lakehouse`, Access Connector + RBAC.
2. In Databricks Catalog Explorer create a **Storage Credential** from the Access Connector id, then run `sql/setup_unity_catalog.sql`.
3. Create a service principal for prod (Databricks account console), add it to the workspace, grant the privileges in the SQL file.
4. Replace all `REPLACE_*` values in `databricks.yml`.

## Local dev
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check src tests && pytest -q          # needs Java 17
```

## Deploy & run
```bash
databricks auth login --host https://adb-XXXX.azuredatabricks.net
databricks bundle validate -t dev
databricks bundle deploy   -t dev
# import scripts/generate_sample_data.py as a notebook and run it to drop test files into landing
databricks bundle run orders_etl -t dev
databricks bundle deploy   -t prod        # prod job runs on its schedule as the service principal
```
Full gold rebuild: run task `gold` with the extra parameter `--full_refresh`.

## CI/CD (GitHub Actions)
Secrets: `DATABRICKS_HOST_DEV`, `DATABRICKS_HOST_PROD`, `DATABRICKS_CLIENT_ID(_PROD)`, `DATABRICKS_CLIENT_SECRET(_PROD)` (OAuth M2M service principals).
PR → lint + tests + bundle validate. Merge to main → deploy dev + smoke run → (approval) → deploy prod.

## Inspect results
```sql
SELECT * FROM dev_catalog.ops.pipeline_audit ORDER BY start_ts DESC;
SELECT * FROM dev_catalog.silver.orders_quarantine;
SELECT * FROM dev_catalog.gold.daily_sales ORDER BY order_date DESC;
```
