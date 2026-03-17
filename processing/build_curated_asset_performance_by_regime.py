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

            logging.info("Dropping curated.asset_performance_by_regime if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.asset_performance_by_regime;")

            logging.info("Creating curated.asset_performance_by_regime...")
            cur.execute(
                """
                CREATE TABLE curated.asset_performance_by_regime AS
                SELECT
                    ticker,
                    is_equity_asset,
                    market_regime_label,
                    rate_regime_20d,
                    vix_regime,
                    COUNT(*) AS obs_count,
                    AVG(return_1d) AS avg_return_1d,
                    STDDEV_SAMP(return_1d) AS return_volatility_1d,
                    AVG(
                        CASE
                            WHEN return_1d > 0 THEN 1.0
                            ELSE 0.0
                        END
                    ) AS positive_day_ratio,
                    AVG(drawdown) AS avg_drawdown,
                    AVG(rolling_vol_20d) AS avg_rolling_vol_20d,
                    MIN(return_1d) AS min_return_1d,
                    MAX(return_1d) AS max_return_1d,
                    CURRENT_TIMESTAMP AS created_at
                FROM curated.asset_regime_context_daily
                WHERE market_regime_label IS NOT NULL
                  AND rate_regime_20d IS NOT NULL
                  AND vix_regime IS NOT NULL
                  AND return_1d IS NOT NULL
                GROUP BY
                    ticker,
                    is_equity_asset,
                    market_regime_label,
                    rate_regime_20d,
                    vix_regime
                ORDER BY
                    ticker,
                    market_regime_label,
                    rate_regime_20d,
                    vix_regime;
                """
            )

            logging.info("Adding unique index on ticker and regime columns...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_asset_performance_by_regime_key
                ON curated.asset_performance_by_regime (
                    ticker,
                    market_regime_label,
                    rate_regime_20d,
                    vix_regime
                );
                """
            )

            logging.info("Adding index on market_regime_label...")
            cur.execute(
                """
                CREATE INDEX idx_asset_performance_by_regime_market_regime
                ON curated.asset_performance_by_regime (market_regime_label);
                """
            )

            logging.info("Adding index on rate_regime_20d...")
            cur.execute(
                """
                CREATE INDEX idx_asset_performance_by_regime_rate_regime
                ON curated.asset_performance_by_regime (rate_regime_20d);
                """
            )
    
        conn.commit()
        logging.info("curated.asset_performance_by_regime created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()