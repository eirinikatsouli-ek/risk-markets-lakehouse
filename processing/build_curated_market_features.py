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
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def main():
    setup_logging()

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            logging.info("Creating curated schema if not exists...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS curated;")

            logging.info("Dropping curated.market_features_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.market_features_daily;")

            logging.info("Creating curated.market_features_daily...")

            cur.execute(
                """
                CREATE TABLE curated.market_features_daily AS
                WITH base AS (
                    SELECT
                        date,
                        ticker,
                        close,
                        LAG(close) OVER (
                            PARTITION BY ticker
                            ORDER BY date
                        ) AS prev_close
                    FROM silver.market_prices_clean
                ),
                returns_calc AS (
                    SELECT
                        date,
                        ticker,
                        close,
                        CASE
                            WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                            ELSE (close / prev_close) - 1
                        END AS return_1d
                    FROM base
                ),
                features AS (
                    SELECT
                        date,
                        ticker,
                        close,
                        return_1d,

                        AVG(close) OVER (
                            PARTITION BY ticker
                            ORDER BY date
                            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                        ) AS sma_50,

                        AVG(close) OVER (
                            PARTITION BY ticker
                            ORDER BY date
                            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                        ) AS sma_200,

                        STDDEV_SAMP(return_1d) OVER (
                            PARTITION BY ticker
                            ORDER BY date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS rolling_vol_20d,

                        MAX(close) OVER (
                            PARTITION BY ticker
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS running_peak_close
                    FROM returns_calc
                )
                SELECT
                    date,
                    ticker,
                    close,
                    return_1d,
                    sma_50,
                    sma_200,
                    rolling_vol_20d,
                    CASE
                        WHEN running_peak_close IS NULL OR running_peak_close = 0 THEN NULL
                        ELSE (close / running_peak_close) - 1
                    END AS drawdown
                FROM features;
                """
            )

            logging.info("Adding index on ticker, date...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_curated_market_features_daily_ticker_date
                ON curated.market_features_daily (ticker, date);
                """
            )

        conn.commit()
        logging.info("curated.market_features_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()