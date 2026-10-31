# Databricks notebook source
# MAGIC %md
# MAGIC ### Generate sample order files into the ADLS landing zone (includes ~5% bad rows and duplicates)

# COMMAND ----------
import random
from datetime import datetime, timedelta

dbutils.widgets.text("storage_account", "REPLACE_devstorageacct")
dbutils.widgets.text("n_rows", "2000")
sa = dbutils.widgets.get("storage_account")
n = int(dbutils.widgets.get("n_rows"))

batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
path = f"abfss://landing@{sa}.dfs.core.windows.net/orders/batch_{batch_id}"

statuses = ["PLACED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
countries = ["AE", "SA", "IN", "GB", "US"]
now = datetime.utcnow()

rows = []
for _ in range(n):
    r = random.random()
    rows.append((
        f"ORD-{random.randint(1, int(n * 0.95))}",                       # collisions => duplicates / updates
        None if r < 0.02 else f"CUST-{random.randint(1, 300)}",           # bad: null customer
        f"PROD-{random.randint(1, 50)}",
        -1 if 0.02 <= r < 0.04 else random.randint(1, 10),                # bad: negative qty
        round(random.uniform(5, 500), 2),
        (now - timedelta(days=random.randint(0, 5), minutes=random.randint(0, 1440))).strftime("%Y-%m-%dT%H:%M:%S"),
        "BOGUS" if 0.04 <= r < 0.05 else random.choice(statuses),         # bad: invalid status
        random.choice(countries),
    ))

schema = ("order_id string, customer_id string, product_id string, quantity int, "
          "unit_price double, order_ts string, status string, country string")
spark.createDataFrame(rows, schema).coalesce(1).write.mode("overwrite").json(path)
print(f"Wrote {n} rows to {path}")
