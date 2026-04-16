#!/usr/bin/env python3
import sys

from app.core.config import get_settings
from app.services.spark import SparkManager


def main() -> None:
    settings = get_settings()
    if not settings.polaris_enabled:
        print("Polaris is not enabled in the selected env file.")
        return

    spark = SparkManager(settings).session
    if spark is None:
        raise SystemExit("Spark session is unavailable.")

    catalog = settings.polaris_catalog_name
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.sales.customers
        USING ICEBERG
        AS SELECT * FROM VALUES
            (CAST(1 AS BIGINT), 'Acme Corp', 'west'),
            (CAST(2 AS BIGINT), 'Globex', 'east'),
            (CAST(3 AS BIGINT), 'Initech', 'central')
        AS t(customer_id, customer_name, region)
        """
    )

    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {catalog}.sales.orders
        USING ICEBERG
        AS SELECT * FROM VALUES
            (CAST(101 AS BIGINT), CAST(1 AS BIGINT), CAST(1200.50 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-01 10:15:00'),
            (CAST(102 AS BIGINT), CAST(2 AS BIGINT), CAST(845.00 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-03 14:45:00'),
            (CAST(103 AS BIGINT), CAST(1 AS BIGINT), CAST(90.99 AS DECIMAL(12, 2)), TIMESTAMP '2026-04-08 09:30:00')
        AS t(order_id, customer_id, order_total, order_ts)
        """
    )

    print(f"Seeded Polaris catalog '{settings.polaris_warehouse}' with sales.customers and sales.orders.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise
