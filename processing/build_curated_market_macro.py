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

            logging.info("Dropping curated.market_macro_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.market_macro_daily;")

            logging.info("Creating curated.market_macro_daily...")
            cur.execute(
                """
                CREATE TABLE curated.market_macro_daily AS
                WITH macro_pivot AS (
                    SELECT
                        date,
                        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi,
                        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds,
                        MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unrate,
                        MAX(CASE WHEN series_id = 'DGS10' THEN value END) AS dgs10,
                        MAX(CASE WHEN series_id = 'VIXCLS' THEN value END) AS vix
                    FROM silver.macro_series_clean
                    GROUP BY date
                )
                SELECT
                    mf.date,
                    mf.ticker,
                    mf.close,
                    mf.return_1d,
                    mf.sma_50,
                    mf.sma_200,
                    mf.rolling_vol_20d,
                    mf.drawdown,

                    cpi_latest.cpi,
                    fed_latest.fed_funds,
                    unrate_latest.unrate,
                    dgs10_latest.dgs10,
                    vix_latest.vix

                FROM curated.market_features_daily mf

                LEFT JOIN LATERAL (
                    SELECT mp.cpi
                    FROM macro_pivot mp
                    WHERE mp.date <= mf.date
                      AND mp.cpi IS NOT NULL
                    ORDER BY mp.date DESC
                    LIMIT 1
                ) cpi_latest ON TRUE

                LEFT JOIN LATERAL (
                    SELECT mp.fed_funds
                    FROM macro_pivot mp
                    WHERE mp.date <= mf.date
                      AND mp.fed_funds IS NOT NULL
                    ORDER BY mp.date DESC
                    LIMIT 1
                ) fed_latest ON TRUE

                LEFT JOIN LATERAL (
                    SELECT mp.unrate
                    FROM macro_pivot mp
                    WHERE mp.date <= mf.date
                      AND mp.unrate IS NOT NULL
                    ORDER BY mp.date DESC
                    LIMIT 1
                ) unrate_latest ON TRUE

                LEFT JOIN LATERAL (
                    SELECT mp.dgs10
                    FROM macro_pivot mp
                    WHERE mp.date <= mf.date
                      AND mp.dgs10 IS NOT NULL
                    ORDER BY mp.date DESC
                    LIMIT 1
                ) dgs10_latest ON TRUE

                LEFT JOIN LATERAL (
                    SELECT mp.vix
                    FROM macro_pivot mp
                    WHERE mp.date <= mf.date
                      AND mp.vix IS NOT NULL
                    ORDER BY mp.date DESC
                    LIMIT 1
                ) vix_latest ON TRUE

                ORDER BY mf.ticker, mf.date;
                """
            )

            logging.info("Adding unique index on ticker, date...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_market_macro_daily_ticker_date
                ON curated.market_macro_daily (ticker, date);
                """
            )

            logging.info("Adding index on date...")
            cur.execute(
                """
                CREATE INDEX idx_market_macro_daily_date
                ON curated.market_macro_daily (date);
                """
            )

        conn.commit()
        logging.info("curated.market_macro_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()