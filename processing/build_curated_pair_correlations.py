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

            logging.info("Dropping curated.asset_pair_correlations_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.asset_pair_correlations_daily;")

            logging.info("Creating curated.asset_pair_correlations_daily...")
            cur.execute(
                """
                CREATE TABLE curated.asset_pair_correlations_daily AS
                WITH pair_config AS (
                    SELECT 'SPY_TLT'::text AS pair_code, 'SPY'::text AS left_ticker, 'TLT'::text AS right_ticker
                    UNION ALL
                    SELECT 'SPY_GLD'::text AS pair_code, 'SPY'::text AS left_ticker, 'GLD'::text AS right_ticker
                    UNION ALL
                    SELECT 'QQQ_TLT'::text AS pair_code, 'QQQ'::text AS left_ticker, 'TLT'::text AS right_ticker
                    UNION ALL
                    SELECT 'VNQ_IEF'::text AS pair_code, 'VNQ'::text AS left_ticker, 'IEF'::text AS right_ticker
                ),
                pair_returns AS (
                    SELECT
                        l.date,
                        pc.pair_code,
                        pc.left_ticker,
                        pc.right_ticker,
                        l.return_1d AS left_return_1d,
                        r.return_1d AS right_return_1d
                    FROM pair_config pc
                    JOIN curated.market_features_daily l
                        ON l.ticker = pc.left_ticker
                    JOIN curated.market_features_daily r
                        ON r.ticker = pc.right_ticker
                       AND r.date = l.date
                ),
                pair_features AS (
                    SELECT
                        date,
                        pair_code,
                        left_ticker,
                        right_ticker,
                        left_return_1d,
                        right_return_1d,
                        CORR(left_return_1d, right_return_1d) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS rolling_corr_20d
                    FROM pair_returns
                )
                SELECT
                    date,
                    pair_code,
                    left_ticker,
                    right_ticker,
                    left_return_1d,
                    right_return_1d,
                    rolling_corr_20d
                FROM pair_features
                ORDER BY pair_code, date;
                """
            )

            logging.info("Adding unique index on pair_code, date...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_asset_pair_correlations_daily_pair_date
                ON curated.asset_pair_correlations_daily (pair_code, date);
                """
            )

            logging.info("Adding index on date...")
            cur.execute(
                """
                CREATE INDEX idx_asset_pair_correlations_daily_date
                ON curated.asset_pair_correlations_daily (date);
                """
            )

        conn.commit()
        logging.info("curated.asset_pair_correlations_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()