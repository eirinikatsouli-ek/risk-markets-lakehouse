import logging
import sys
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "riskdb",
    "user": "risk",
    "password": "risk",
}

REQUIRED_TABLES = [
    "curated.market_features_daily",
    "curated.asset_pair_correlations_daily",
    "curated.market_macro_daily",
    "curated.fx_features_daily",
    "curated.asset_risk_snapshot_latest",
    "curated.asset_pair_correlation_snapshot_latest",
    "curated.fx_risk_snapshot_latest",
    "curated.market_regime_daily",
    "curated.asset_regime_context_daily",
    "curated.asset_performance_by_regime",
    "curated.pair_correlation_by_regime",
]


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def fetch_one_value(cur, query: str):
    cur.execute(query)
    row = cur.fetchone()
    return row[0] if row else None


def record_check(check_name: str, passed: bool, actual=None, expected=None):
    if passed:
        logging.info("PASS | %s | actual=%s", check_name, actual)
    else:
        logging.error("FAIL | %s | actual=%s | expected=%s", check_name, actual, expected)
    return passed


def main():
    setup_logging()
    failures = []

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            logging.info("Starting curated pipeline validation...")

            # ---------------------------
            # 1) Required tables exist
            # ---------------------------
            for table_name in REQUIRED_TABLES:
                exists_query = f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '{table_name.split('.')[0]}'
                      AND table_name = '{table_name.split('.')[1]}'
                );
                """
                exists = fetch_one_value(cur, exists_query)
                if not record_check(
                    check_name=f"table exists: {table_name}",
                    passed=bool(exists),
                    actual=exists,
                    expected=True
                ):
                    failures.append(f"Missing table: {table_name}")

            # ---------------------------
            # 2) Core row counts > 0
            # ---------------------------
            row_count_checks = {
                "curated.market_features_daily": "SELECT COUNT(*) FROM curated.market_features_daily;",
                "curated.asset_pair_correlations_daily": "SELECT COUNT(*) FROM curated.asset_pair_correlations_daily;",
                "curated.market_macro_daily": "SELECT COUNT(*) FROM curated.market_macro_daily;",
                "curated.fx_features_daily": "SELECT COUNT(*) FROM curated.fx_features_daily;",
                "curated.market_regime_daily": "SELECT COUNT(*) FROM curated.market_regime_daily;",
                "curated.asset_regime_context_daily": "SELECT COUNT(*) FROM curated.asset_regime_context_daily;",
                "curated.asset_performance_by_regime": "SELECT COUNT(*) FROM curated.asset_performance_by_regime;",
                "curated.pair_correlation_by_regime": "SELECT COUNT(*) FROM curated.pair_correlation_by_regime;",
            }

            for check_name, query in row_count_checks.items():
                count_value = fetch_one_value(cur, query)
                passed = count_value is not None and count_value > 0
                if not record_check(
                    check_name=f"row count > 0: {check_name}",
                    passed=passed,
                    actual=count_value,
                    expected="> 0"
                ):
                    failures.append(f"Empty or missing data in: {check_name}")

            # ---------------------------
            # 3) Snapshot exact row counts
            # ---------------------------
            exact_count_checks = {
                "curated.asset_risk_snapshot_latest": (
                    "SELECT COUNT(*) FROM curated.asset_risk_snapshot_latest;",
                    8
                ),
                "curated.asset_pair_correlation_snapshot_latest": (
                    "SELECT COUNT(*) FROM curated.asset_pair_correlation_snapshot_latest;",
                    4
                ),
                "curated.fx_risk_snapshot_latest": (
                    "SELECT COUNT(*) FROM curated.fx_risk_snapshot_latest;",
                    3
                ),
            }

            for check_name, (query, expected_count) in exact_count_checks.items():
                count_value = fetch_one_value(cur, query)
                passed = count_value == expected_count
                if not record_check(
                    check_name=f"exact row count: {check_name}",
                    passed=passed,
                    actual=count_value,
                    expected=expected_count
                ):
                    failures.append(f"Unexpected row count in: {check_name}")

            # ---------------------------
            # 4) Duplicate checks
            # ---------------------------
            duplicate_checks = {
                "market_features_daily key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT ticker, date, COUNT(*)
                        FROM curated.market_features_daily
                        GROUP BY ticker, date
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "asset_pair_correlations_daily key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT pair_code, date, COUNT(*)
                        FROM curated.asset_pair_correlations_daily
                        GROUP BY pair_code, date
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "market_regime_daily key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT date, COUNT(*)
                        FROM curated.market_regime_daily
                        GROUP BY date
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "asset_risk_snapshot_latest key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT ticker, COUNT(*)
                        FROM curated.asset_risk_snapshot_latest
                        GROUP BY ticker
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "asset_pair_correlation_snapshot_latest key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT pair_code, COUNT(*)
                        FROM curated.asset_pair_correlation_snapshot_latest
                        GROUP BY pair_code
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "fx_risk_snapshot_latest key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT pair_code, COUNT(*)
                        FROM curated.fx_risk_snapshot_latest
                        GROUP BY pair_code
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "asset_regime_context_daily key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT ticker, date, COUNT(*)
                        FROM curated.asset_regime_context_daily
                        GROUP BY ticker, date
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "asset_performance_by_regime key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT ticker, market_regime_label, rate_regime_20d, vix_regime, COUNT(*)
                        FROM curated.asset_performance_by_regime
                        GROUP BY ticker, market_regime_label, rate_regime_20d, vix_regime
                        HAVING COUNT(*) > 1
                    ) d;
                """,
                "pair_correlation_by_regime key": """
                    SELECT COUNT(*)
                    FROM (
                        SELECT pair_code, market_regime_label, rate_regime_20d, vix_regime, COUNT(*)
                        FROM curated.pair_correlation_by_regime
                        GROUP BY pair_code, market_regime_label, rate_regime_20d, vix_regime
                        HAVING COUNT(*) > 1
                    ) d;
                """,
            }

            for check_name, query in duplicate_checks.items():
                duplicate_count = fetch_one_value(cur, query)
                passed = duplicate_count == 0
                if not record_check(
                    check_name=f"duplicate count = 0: {check_name}",
                    passed=passed,
                    actual=duplicate_count,
                    expected=0
                ):
                    failures.append(f"Duplicate key rows found in: {check_name}")

            # ---------------------------
            # 5) Regime fields not all NULL
            # ---------------------------
            regime_not_null_checks = {
                "market_regime_label populated": """
                    SELECT COUNT(*)
                    FROM curated.market_regime_daily
                    WHERE market_regime_label IS NOT NULL;
                """,
                "vix_regime populated": """
                    SELECT COUNT(*)
                    FROM curated.market_regime_daily
                    WHERE vix_regime IS NOT NULL;
                """,
                "rate_regime_20d populated": """
                    SELECT COUNT(*)
                    FROM curated.market_regime_daily
                    WHERE rate_regime_20d IS NOT NULL;
                """,
            }

            for check_name, query in regime_not_null_checks.items():
                non_null_count = fetch_one_value(cur, query)
                passed = non_null_count is not None and non_null_count > 0
                if not record_check(
                    check_name=f"non-null rows > 0: {check_name}",
                    passed=passed,
                    actual=non_null_count,
                    expected="> 0"
                ):
                    failures.append(f"Regime field empty: {check_name}")

            # ---------------------------
            # Final result
            # ---------------------------
            if failures:
                logging.error("Curated pipeline validation failed.")
                logging.error("Failure count: %s", len(failures))
                for failure in failures:
                    logging.error(" - %s", failure)
                raise SystemExit(1)

            logging.info("Curated pipeline validation completed successfully.")
            raise SystemExit(0)

    finally:
        conn.close()


if __name__ == "__main__":
    main()