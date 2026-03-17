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

            logging.info("Dropping curated.fx_features_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.fx_features_daily;")

            logging.info("Creating curated.fx_features_daily...")
            cur.execute(
                """
                CREATE TABLE curated.fx_features_daily AS
                WITH ranked_fx AS (
                    SELECT
                        date,
                        pair AS pair_code,
                        close AS fx_rate,
                        partition_dt,
                        loaded_at,
                        ROW_NUMBER() OVER (
                            PARTITION BY pair, date
                            ORDER BY partition_dt DESC, loaded_at DESC
                        ) AS rn
                    FROM silver.fx_rates_clean
                    WHERE date IS NOT NULL
                      AND pair IS NOT NULL
                      AND close IS NOT NULL
                ),
                deduped_fx AS (
                    SELECT
                        date,
                        pair_code,
                        fx_rate
                    FROM ranked_fx
                    WHERE rn = 1
                ),
                base AS (
                    SELECT
                        date,
                        pair_code,
                        fx_rate,
                        LAG(fx_rate) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                        ) AS prev_fx_rate
                    FROM deduped_fx
                ),
                returns_calc AS (
                    SELECT
                        date,
                        pair_code,
                        fx_rate,
                        CASE
                            WHEN prev_fx_rate IS NULL OR prev_fx_rate = 0 THEN NULL
                            ELSE (fx_rate / prev_fx_rate) - 1
                        END AS return_1d
                    FROM base
                ),
                features AS (
                    SELECT
                        date,
                        pair_code,
                        fx_rate,
                        return_1d,

                        AVG(fx_rate) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                        ) AS sma_50,

                        AVG(fx_rate) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                        ) AS sma_200,

                        STDDEV_SAMP(return_1d) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS rolling_vol_20d,

                        MAX(fx_rate) OVER (
                            PARTITION BY pair_code
                            ORDER BY date
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS running_peak_fx_rate
                    FROM returns_calc
                )
                SELECT
                    date,
                    pair_code,
                    fx_rate,
                    return_1d,
                    sma_50,
                    sma_200,
                    rolling_vol_20d,
                    CASE
                        WHEN running_peak_fx_rate IS NULL OR running_peak_fx_rate = 0 THEN NULL
                        ELSE (fx_rate / running_peak_fx_rate) - 1
                    END AS drawdown
                FROM features
                ORDER BY pair_code, date;
                """
            )

            logging.info("Adding unique index on pair_code, date...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_fx_features_daily_pair_date
                ON curated.fx_features_daily (pair_code, date);
                """
            )

            logging.info("Adding index on date...")
            cur.execute(
                """
                CREATE INDEX idx_fx_features_daily_date
                ON curated.fx_features_daily (date);
                """
            )

        conn.commit()
        logging.info("curated.fx_features_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()