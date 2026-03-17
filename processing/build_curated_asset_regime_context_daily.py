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

            logging.info("Dropping curated.asset_regime_context_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.asset_regime_context_daily;")

            logging.info("Creating curated.asset_regime_context_daily...")
            cur.execute(
                """
                CREATE TABLE curated.asset_regime_context_daily AS
                SELECT
                    mf.date,
                    mf.ticker,
                    mf.close,
                    mf.return_1d,
                    mf.sma_50,
                    mf.sma_200,
                    mf.rolling_vol_20d,
                    mf.drawdown,

                    mr.equity_proxy_ticker,
                    mr.spy_close,
                    mr.spy_return_1d,
                    mr.spy_drawdown,
                    mr.spy_above_sma_50,
                    mr.spy_above_sma_200,
                    mr.vix,
                    mr.dgs10,
                    mr.fed_funds,
                    mr.vix_change_20d,
                    mr.dgs10_change_20d,
                    mr.vix_regime,
                    mr.rate_regime_20d,
                    mr.equity_drawdown_regime,
                    mr.market_regime_label,

                    CASE
                        WHEN mf.ticker IN ('SPY', 'QQQ', 'IWM', 'EFA', 'VNQ') THEN TRUE
                        ELSE FALSE
                    END AS is_equity_asset,

                    CURRENT_TIMESTAMP AS created_at
                FROM curated.market_features_daily mf
                INNER JOIN curated.market_regime_daily mr
                    ON mf.date = mr.date
                ORDER BY mf.ticker, mf.date;
                """
            )

            logging.info("Adding unique index on ticker, date...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_asset_regime_context_daily_ticker_date
                ON curated.asset_regime_context_daily (ticker, date);
                """
            )

            logging.info("Adding index on date...")
            cur.execute(
                """
                CREATE INDEX idx_asset_regime_context_daily_date
                ON curated.asset_regime_context_daily (date);
                """
            )

            logging.info("Adding index on market_regime_label...")
            cur.execute(
                """
                CREATE INDEX idx_asset_regime_context_daily_market_regime
                ON curated.asset_regime_context_daily (market_regime_label);
                """
            )

        conn.commit()
        logging.info("curated.asset_regime_context_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()