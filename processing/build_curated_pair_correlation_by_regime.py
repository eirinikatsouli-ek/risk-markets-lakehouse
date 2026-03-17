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

            logging.info("Dropping curated.pair_correlation_by_regime if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.pair_correlation_by_regime;")

            logging.info("Creating curated.pair_correlation_by_regime...")
            cur.execute(
                """
                CREATE TABLE curated.pair_correlation_by_regime AS
                SELECT
                    pc.pair_code,
                    pc.left_ticker,
                    pc.right_ticker,
                    mr.market_regime_label,
                    mr.rate_regime_20d,
                    mr.vix_regime,
                    COUNT(*) AS obs_count,
                    AVG(pc.rolling_corr_20d) AS avg_rolling_corr_20d,
                    MIN(pc.rolling_corr_20d) AS min_rolling_corr_20d,
                    MAX(pc.rolling_corr_20d) AS max_rolling_corr_20d,
                    AVG(
                        CASE
                            WHEN pc.rolling_corr_20d < 0 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS negative_corr_ratio,
                    AVG(
                        CASE
                            WHEN pc.rolling_corr_20d >= 0.50 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS strong_positive_corr_ratio,
                    AVG(pc.left_return_1d) AS avg_left_return_1d,
                    AVG(pc.right_return_1d) AS avg_right_return_1d,
                    CURRENT_TIMESTAMP AS created_at
                FROM curated.asset_pair_correlations_daily pc
                INNER JOIN curated.market_regime_daily mr
                    ON pc.date = mr.date
                WHERE pc.rolling_corr_20d IS NOT NULL
                  AND mr.market_regime_label IS NOT NULL
                  AND mr.rate_regime_20d IS NOT NULL
                  AND mr.vix_regime IS NOT NULL
                GROUP BY
                    pc.pair_code,
                    pc.left_ticker,
                    pc.right_ticker,
                    mr.market_regime_label,
                    mr.rate_regime_20d,
                    mr.vix_regime
                ORDER BY
                    pc.pair_code,
                    mr.market_regime_label,
                    mr.rate_regime_20d,
                    mr.vix_regime;
                """
            )

            logging.info("Adding unique index on pair and regime columns...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_pair_correlation_by_regime_key
                ON curated.pair_correlation_by_regime (
                    pair_code,
                    market_regime_label,
                    rate_regime_20d,
                    vix_regime
                );
                """
            )

            logging.info("Adding index on market_regime_label...")
            cur.execute(
                """
                CREATE INDEX idx_pair_correlation_by_regime_market_regime
                ON curated.pair_correlation_by_regime (market_regime_label);
                """
            )

            logging.info("Adding index on rate_regime_20d...")
            cur.execute(
                """
                CREATE INDEX idx_pair_correlation_by_regime_rate_regime
                ON curated.pair_correlation_by_regime (rate_regime_20d);
                """
            )

        conn.commit()
        logging.info("curated.pair_correlation_by_regime created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()