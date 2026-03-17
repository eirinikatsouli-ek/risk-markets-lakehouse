import os
import io
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import requests

TICKERS = ["SPY", "QQQ", "IWM", "EFA", "TLT", "IEF", "GLD", "VNQ"]

BASE_RAW_DIR = os.path.join("data", "raw", "market_prices")
TIMEOUT = 30
MAX_ATTEMPTS = 3

EXPECTED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]
NUMERIC_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]


def stooq_url(ticker: str) -> str:
    return f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = os.path.join(BASE_RAW_DIR, f"dt={dt}")
    tickers_dir = os.path.join(out_dir, "tickers")
    os.makedirs(tickers_dir, exist_ok=True)

    run_started_utc = datetime.now(timezone.utc).isoformat()
    successes = []
    failures = []

    for ticker in TICKERS:
        url = stooq_url(ticker)
        logging.info(f"Fetching {ticker} from {url}")

        try:
            last_error = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    r = requests.get(url, timeout=TIMEOUT)
                    r.raise_for_status()
                    break
                except Exception as e:
                    last_error = e
                    logging.warning(f"{ticker}: attempt {attempt}/{MAX_ATTEMPTS} failed: {e}")
                    time.sleep(2 * attempt)
            else:
                raise last_error

            df = pd.read_csv(io.StringIO(r.text))

            missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
            if missing:
                raise ValueError(f"{ticker}: Missing columns: {missing}")
            if df.empty:
                raise ValueError(f"{ticker}: Empty dataset")

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            if df["Date"].isna().all():
                raise ValueError(f"{ticker}: Date parsing failed")

            for col in NUMERIC_COLUMNS:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            df["Ticker"] = ticker

            out_path = os.path.join(tickers_dir, f"{ticker.lower()}.csv")
            df.to_csv(out_path, index=False)

            successes.append(
                {"ticker": ticker, "rows": int(len(df)), "columns": list(df.columns), "file": out_path.replace("\\", "/")}
            )
            logging.info(f"Saved {ticker}: {len(df)} rows -> {out_path}")

        except Exception as e:
            failures.append({"ticker": ticker, "error": str(e)})
            logging.error(f"FAILED {ticker}: {e}")

    run_finished_utc = datetime.now(timezone.utc).isoformat()

    manifest = {
        "run_started_utc": run_started_utc,
        "run_finished_utc": run_finished_utc,
        "partition_dt": dt,
        "source": "stooq",
        "tickers_requested": TICKERS,
        "success_count": len(successes),
        "failure_count": len(failures),
        "successes": successes,
        "failures": failures,
        "expected_columns": EXPECTED_COLUMNS,
    }

    manifest_path = os.path.join(out_dir, "run_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    logging.info(f"Run manifest saved -> {manifest_path}")

    if len(successes) == 0:
        raise RuntimeError("All tickers failed. See run_manifest.json for details.")


if __name__ == "__main__":
    main()