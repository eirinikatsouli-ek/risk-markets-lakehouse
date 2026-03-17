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

            # ---------------------------
            # silver.fx_rates_clean
            # ---------------------------
            logging.info("Dropping silver.fx_rates_clean if it already exists...")
            cur.execute("DROP TABLE IF EXISTS silver.fx_rates_clean;")

            logging.info("Creating silver.fx_rates_clean...")
            cur.execute(
                """
                CREATE TABLE silver.fx_rates_clean AS
                SELECT
                    date,
                    pair,
                    open,
                    high,
                    low,
                    close,
                    partition_dt,
                    loaded_at
                FROM raw.fx_rates
                WHERE date IS NOT NULL
                  AND pair IS NOT NULL;
                """
            )

            logging.info("Adding index on pair, date...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_silver_fx_rates_clean_pair_date
                ON silver.fx_rates_clean (pair, date);
                """
            )

            # ---------------------------
            # silver.macro_series_clean
            # ---------------------------
            logging.info("Dropping silver.macro_series_clean if it already exists...")
            cur.execute("DROP TABLE IF EXISTS silver.macro_series_clean;")

            logging.info("Creating silver.macro_series_clean...")
            cur.execute(
                """
                CREATE TABLE silver.macro_series_clean AS
                SELECT
                    date,
                    series_id,
                    value,
                    partition_dt,
                    loaded_at
                FROM raw.macro_series
                WHERE date IS NOT NULL
                  AND series_id IS NOT NULL;
                """
            )

            logging.info("Adding index on series_id, date...")
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_silver_macro_series_clean_series_date
                ON silver.macro_series_clean (series_id, date);
                """
            )

        conn.commit()
        logging.info("silver.fx_rates_clean and silver.macro_series_clean created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()