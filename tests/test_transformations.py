from datetime import date, datetime

from etl_pipeline import transformations as T

RAW_SCHEMA = (
    "order_id string, customer_id string, product_id string, quantity int, unit_price double, "
    "order_ts string, status string, country string, _ingest_ts timestamp, _source_file string"
)
NOW = datetime(2024, 1, 2, 0, 0, 0)


def _raw(spark, rows):
    return spark.createDataFrame(rows, RAW_SCHEMA)


def test_clean_standardises_and_computes_total(spark):
    df = _raw(spark, [(" o1 ", "c1", "p1", 2, 10.5, "2024-01-01T10:00:00", " paid ", "ae", NOW, "f1")])
    row = T.clean_orders(df).collect()[0]
    assert row.order_id == "o1" and row.status == "PAID" and row.country == "AE"
    assert float(row.total_amount) == 21.0
    assert row.order_date == date(2024, 1, 1)


def test_quality_rules_split_valid_and_invalid(spark):
    df = _raw(spark, [
        ("o1", "c1", "p1", 2, 10.0, "2024-01-01T10:00:00", "PAID", "AE", NOW, "f"),
        ("o2", None, "p1", 2, 10.0, "2024-01-01T10:00:00", "PAID", "AE", NOW, "f"),
        ("o3", "c1", "p1", -1, 10.0, "2024-01-01T10:00:00", "PAID", "AE", NOW, "f"),
        ("o4", "c1", "p1", 1, 10.0, "2024-01-01T10:00:00", "UNKNOWN", "AE", NOW, "f"),
    ])
    valid, invalid = T.apply_quality_rules(T.clean_orders(df))
    assert [r.order_id for r in valid.collect()] == ["o1"]
    reasons = {r.order_id: r._dq_failures for r in invalid.collect()}
    assert "customer_id_not_null" in reasons["o2"]
    assert "quantity_positive" in reasons["o3"]
    assert "status_allowed" in reasons["o4"]


def test_dedupe_keeps_latest_version(spark):
    df = _raw(spark, [
        ("o1", "c1", "p1", 1, 10.0, "2024-01-01T10:00:00", "PLACED", "AE", NOW, "f1"),
        ("o1", "c1", "p1", 1, 10.0, "2024-01-01T12:00:00", "SHIPPED", "AE", NOW, "f2"),
    ])
    out = T.dedupe_latest(T.clean_orders(df)).collect()
    assert len(out) == 1 and out[0].status == "SHIPPED"


def test_daily_sales_excludes_cancelled(spark):
    schema = ("order_id string, customer_id string, product_id string, quantity int, "
              "total_amount decimal(18,2), status string, country string, order_date date")
    df = spark.createDataFrame([
        ("o1", "c1", "p1", 2, 20, "PAID", "AE", date(2024, 1, 1)),
        ("o2", "c2", "p1", 1, 10, "DELIVERED", "AE", date(2024, 1, 1)),
        ("o3", "c3", "p1", 5, 50, "CANCELLED", "AE", date(2024, 1, 1)),
    ], schema)
    row = T.build_daily_sales(df).collect()[0]
    assert row.order_count == 2 and row.units_sold == 3 and float(row.revenue) == 30.0
