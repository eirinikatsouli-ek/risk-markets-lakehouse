import os
import glob
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

RAW_DIR = os.path.join("data", "raw", "fx_rates")

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "riskdb",
    "user": "risk",
    "password": "risk",
}

EXPECTED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Pair"]


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def latest_partition_path():
    parts = sorted(glob.glob(os.path.join(RAW_DIR, "dt=*")))
    if not parts:
        raise FileNotFoundError(f"No partitions found under {RAW_DIR}")
    return parts[-1]


def read_partition_csvs(partition_path):
    pair_dir = os.path.join(partition_path, "pairs")
    files = sorted(glob.glob(os.path.join(pair_dir, "*.csv")))
    if not files:
        raise FileNotFoundError(f"No pair csv files under {pair_dir}")

    frames = []
    for f in files:
        df = pd.read_csv(f)
        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True)

    missing = [c for c in EXPECTED_COLUMNS if c not in all_df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    all_df.columns = [c.lower() for c in all_df.columns]

    all_df["date"] = pd.to_datetime(all_df["date"], errors="coerce").dt.date

    for c in ["open", "high", "low", "close"]:
        all_df[c] = pd.to_numeric(all_df[c], errors="coerce")

    dt_str = os.path.basename(partition_path).replace("dt=", "")
    all_df["partition_dt"] = dt_str

    return all_df


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.fx_rates (
                date date NOT NULL,
                open double precision,
                high double precision,
                low double precision,
                close double precision,
                pair text NOT NULL,
                partition_dt date NOT NULL,
                loaded_at timestamp NOT NULL DEFAULT now()
            );
            """
        )
    conn.commit()


def load_rows(conn, df):
    cols = ["date", "open", "high", "low", "close", "pair", "partition_dt"]
    rows = [tuple(x) for x in df[cols].to_numpy()]

    partition_dt = df["partition_dt"].iloc[0]

    with conn.cursor() as cur:
        cur.execute("DELETE FROM raw.fx_rates WHERE partition_dt = %s;", (partition_dt,))
        execute_values(
            cur,
            """
            INSERT INTO raw.fx_rates (date, open, high, low, close, pair, partition_dt)
            VALUES %s;
            """,
            rows,
            page_size=5000
        )
    conn.commit()


def main():
    setup_logging()

    part = latest_partition_path()
    logging.info(f"Loading latest FX partition: {part}")

    df = read_partition_csvs(part)
    logging.info(f"FX rows to load: {len(df)}")

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        ensure_table(conn)
        load_rows(conn, df)
        logging.info("FX load completed.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()