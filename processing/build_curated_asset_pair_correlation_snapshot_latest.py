import logging
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "riskdb",
    "user": "risk",
    "password": "risk",
}


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def main():
    setup_logging()

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            logging.info("Creating curated schema if not exists...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")

            logging.info("Dropping curated.asset_pair_correlation_snapshot_latest if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.asset_pair_correlation_snapshot_latest;")

            logging.info("Creating curated.asset_pair_correlation_snapshot_latest...")
            cur.execute(
                """
                CREATE TABLE curated.asset_pair_correlation_snapshot_latest AS
                WITH ranked_rows AS (
                    SELECT
                        date,
                        pair_code,
                        left_ticker,
                        right_ticker,
                        left_return_1d,
                        right_return_1d,
                        rolling_corr_20d,
                        ROW_NUMBER() OVER (
                            PARTITION BY pair_code
                            ORDER BY date DESC
                        ) AS rn
                    FROM curated.asset_pair_correlations_daily
                )
                SELECT
                    date AS as_of_date,
                    pair_code,
                    left_ticker,
                    right_ticker,
                    left_return_1d,
                    right_return_1d,
                    rolling_corr_20d,
                    CASE
                        WHEN rolling_corr_20d >= 0.50 THEN 'strong_positive'
                        WHEN rolling_corr_20d > 0.20 THEN 'positive'
                        WHEN rolling_corr_20d <= -0.20 THEN 'negative'
                        ELSE 'neutral'
                    END AS correlation_regime,
                    CASE
                        WHEN rolling_corr_20d < 0 THEN TRUE
                        ELSE FALSE
                    END AS is_negative_correlation,
                    CASE
                        WHEN rolling_corr_20d >= 0.50 THEN TRUE
                        ELSE FALSE
                    END AS is_strong_positive_correlation,
                    CURRENT_TIMESTAMP AS created_at
                FROM ranked_rows
                WHERE rn = 1
                ORDER BY pair_code;
                """
            )

            logging.info("Adding unique index on pair_code...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_asset_pair_correlation_snapshot_latest_pair
                ON curated.asset_pair_correlation_snapshot_latest (pair_code);
                """
            )

        conn.commit()
        logging.info("curated.asset_pair_correlation_snapshot_latest created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()