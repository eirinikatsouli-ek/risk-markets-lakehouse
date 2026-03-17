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

            logging.info("Dropping curated.market_regime_daily if it already exists...")
            cur.execute("DROP TABLE IF EXISTS curated.market_regime_daily;")

            logging.info("Creating curated.market_regime_daily...")
            cur.execute(
                """
                CREATE TABLE curated.market_regime_daily AS
                WITH spy_base AS (
                    SELECT
                        date,
                        ticker AS equity_proxy_ticker,
                        close AS spy_close,
                        return_1d AS spy_return_1d,
                        sma_50,
                        sma_200,
                        drawdown AS spy_drawdown,
                        vix,
                        dgs10,
                        fed_funds
                    FROM curated.market_macro_daily
                    WHERE ticker = 'SPY'
                ),
                lagged AS (
                    SELECT
                        date,
                        equity_proxy_ticker,
                        spy_close,
                        spy_return_1d,
                        sma_50,
                        sma_200,
                        spy_drawdown,
                        vix,
                        dgs10,
                        fed_funds,
                        LAG(vix, 20) OVER (ORDER BY date) AS vix_20d_ago,
                        LAG(dgs10, 20) OVER (ORDER BY date) AS dgs10_20d_ago
                    FROM spy_base
                )
                SELECT
                    date,
                    equity_proxy_ticker,
                    spy_close,
                    spy_return_1d,
                    spy_drawdown,
                    CASE
                        WHEN spy_close > sma_50 THEN TRUE
                        ELSE FALSE
                    END AS spy_above_sma_50,
                    CASE
                        WHEN spy_close > sma_200 THEN TRUE
                        ELSE FALSE
                    END AS spy_above_sma_200,
                    vix,
                    dgs10,
                    fed_funds,
                    CASE
                        WHEN vix_20d_ago IS NULL THEN NULL
                        ELSE vix - vix_20d_ago
                    END AS vix_change_20d,
                    CASE
                        WHEN dgs10_20d_ago IS NULL THEN NULL
                        ELSE dgs10 - dgs10_20d_ago
                    END AS dgs10_change_20d,
                    CASE
                        WHEN vix IS NULL THEN NULL
                        WHEN vix < 15 THEN 'low_vol'
                        WHEN vix < 25 THEN 'normal_vol'
                        ELSE 'stress_vol'
                    END AS vix_regime,
                    CASE
                        WHEN dgs10_20d_ago IS NULL OR dgs10 IS NULL THEN NULL
                        WHEN (dgs10 - dgs10_20d_ago) >= 0.25 THEN 'rising_rates'
                        WHEN (dgs10 - dgs10_20d_ago) <= -0.25 THEN 'falling_rates'
                        ELSE 'stable_rates'
                    END AS rate_regime_20d,
                    CASE
                        WHEN spy_drawdown IS NULL THEN NULL
                        WHEN spy_drawdown = 0 THEN 'at_high'
                        WHEN spy_drawdown > -0.10 THEN 'pullback'
                        WHEN spy_drawdown > -0.20 THEN 'correction'
                        ELSE 'deep_drawdown'
                    END AS equity_drawdown_regime,
                    CASE
                        WHEN vix IS NULL OR spy_drawdown IS NULL THEN NULL
                        WHEN vix >= 25 OR spy_drawdown <= -0.20 THEN 'stressed_risk_off'
                        WHEN spy_drawdown <= -0.10 THEN 'risk_off'
                        WHEN vix < 15 AND spy_close > sma_50 AND spy_close > sma_200 THEN 'calm_risk_on'
                        ELSE 'mixed'
                    END AS market_regime_label,
                    CURRENT_TIMESTAMP AS created_at
                FROM lagged
                ORDER BY date;
                """
            )

            logging.info("Adding unique index on date...")
            cur.execute(
                """
                CREATE UNIQUE INDEX idx_market_regime_daily_date
                ON curated.market_regime_daily (date);
                """
            )

        conn.commit()
        logging.info("curated.market_regime_daily created successfully.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()