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

            logging.info("Dropping curated.fx_risk_snapshot_latest if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.fx_risk_snapshot_latest;")

            logging.info("Creating curated.fx_risk_snapshot_latest...")
            cur.execute(
                """
                CREATE TABLE curated.fx_risk_snapshot_latest AS
                WITH ranked_rows AS (
                    SELECT
                        date,
                        pair_code,
                        fx_rate,
                        return_1d,
                        sma_50,
                        sma_200,
                        rolling_vol_20d,
                        drawdown,
                        ROW_NUMBER() OVER (
                            PARTITION BY pair_code
                            ORDER BY date DESC
                        ) AS rn
                    FROM curated.fx_features_daily
                )
                SELECT
                    date AS as_of_date,
                    pair_code,
                    fx_rate,
                    return_1d,
                    sma_50,
                    sma_200,
                    rolling_vol_20d,
                    drawdown,
                    CASE
                        WHEN fx_rate > sma_50 THEN TRUE
                        ELSE FALSE
                    END AS is_above_sma_50,
                    CASE
                        WHEN fx_rate > sma_200 THEN TRUE
                        ELSE FALSE
                    END AS is_above_sma_200,
                    CASE
                        WHEN drawdown < 0 THEN TRUE
                        ELSE FALSE
                    END AS is_in_drawdown,
                    CURRENT_TIMESTAMP AS created_at
                FROM ranked_rows
                WHERE rn = 1
                ORDER BY pair_code;
                """
            )

            logging.info("Adding unique index on pair_code...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_fx_risk_snapshot_latest_pair
                ON curated.fx_risk_snapshot_latest (pair_code);
                """
            )

        conn.commit()
        logging.info("curated.fx_risk_snapshot_latest created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()