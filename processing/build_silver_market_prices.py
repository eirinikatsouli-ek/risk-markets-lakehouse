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
            logging.info("Creating silver schema if not exists...")
            cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")

            logging.info("Dropping silver.market_prices_clean if it already exists...")
            cur.execute("DROP TABLE IF EXISTS silver.market_prices_clean;")

            logging.info("Creating silver.market_prices_clean...")
            cur.execute(
                """
                CREATE TABLE silver.market_prices_clean AS
                SELECT
                    date,
                    ticker,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    partition_dt,
                    loaded_at
                FROM raw.market_prices
                WHERE date IS NOT NULL
                  AND ticker IS NOT NULL;
                """
            )

            logging.info("Adding index on ticker, date...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_silver_market_prices_clean_ticker_date
                ON silver.market_prices_clean (ticker, date);
                """
            )

        conn.commit()
        logging.info("silver.market_prices_clean created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
