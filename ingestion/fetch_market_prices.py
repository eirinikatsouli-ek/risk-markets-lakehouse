import os
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
import pandas as pd

TICKERS = ["SPY", "QQQ", "IWM", "EFA", "TLT", "IEF", "GLD", "VNQ"]

EXPECTED_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]
NUMERIC_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

BASE_RAW_DIR = os.path.join("data", "raw", "market_prices")
REQUEST_TIMEOUT_SECONDS = 30


def setup_logging() -> None:
    """
    Ρυθμίζει logging ώστε να βλέπεις καθαρά τι κάνει το script.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

def stooq_url(ticker: str) -> str:
    """
    Φτιάχνει το URL για daily δεδομένα από Stooq.
    Π.χ. SPY -> https://stooq.com/q/d/l/?s=spy.us&i=d
    """
    return f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"


def ensure_dir(path: str) -> None:
    """
    Φτιάχνει folder αν δεν υπάρχει. Αν υπάρχει ήδη, δεν σκάει.
    """
    os.makedirs(path, exist_ok=True)


def validate_dataframe(df: pd.DataFrame, ticker: str) -> Tuple[bool, List[str]]:
    """
    Ελέγχει ότι το dataset έχει το schema που περιμένουμε και βασική ποιότητα.
    Επιστρέφει (ok, errors).
    """
    errors: List[str] = []

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"{ticker}: Missing columns: {missing}")
    if df.empty:
        errors.append(f"{ticker}: Dataset is empty")

    if "Date" in df.columns:
        parsed = pd.to_datetime(df["Date"], errors="coerce")
        if parsed.isna().all():
            errors.append(f"{ticker}: Could not parse any Date values")
        df["Date"] = parsed.dt.date 

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            coerced = pd.to_numeric(df[col], errors="coerce")
            if coerced.isna().all():
                errors.append(f"{ticker}: Column {col} could not be parsed as numeric")
            df[col] = coerced

    ok = len(errors) == 0
    return ok, errors


def fetch_one(ticker: str) -> pd.DataFrame:
    """
    Κατεβάζει το CSV για 1 ticker και το επιστρέφει ως DataFrame.
    """
    url = stooq_url(ticker)
    logging.info(f"Fetching {ticker} from {url}")
    r = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return df


def write_ticker_file(df: pd.DataFrame, out_dir: str, ticker: str) -> str:
    """
    Γράφει ένα CSV ανά ticker μέσα σε tickers/ folder.
    Επιστρέφει το path που γράφτηκε.
    """
    tickers_dir = os.path.join(out_dir, "tickers")
    ensure_dir(tickers_dir)

    out_path = os.path.join(tickers_dir, f"{ticker.lower()}.csv")
    df.to_csv(out_path, index=False)
    return out_path


def write_manifest(out_dir: str, manifest: Dict) -> str:
    """
    Γράφει run manifest (metadata) σε JSON.
    """
    out_path = os.path.join(out_dir, "run_manifest.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    return out_path


def main() -> None:
    setup_logging()
    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = os.path.join(BASE_RAW_DIR, f"dt={dt}")
    ensure_dir(out_dir)

    run_started_utc = datetime.now(timezone.utc).isoformat()

    successes = []
    failures = []

    for ticker in TICKERS:
        try:
            df = fetch_one(ticker)

            df["Ticker"] = ticker

            ok, errors = validate_dataframe(df, ticker)
            if not ok:
                raise ValueError(" | ".join(errors))

            out_path = write_ticker_file(df, out_dir, ticker)

            successes.append(
                {
                    "ticker": ticker,
                    "rows": int(len(df)),
                    "columns": list(df.columns),
                    "file": out_path.replace("\\", "/"),
                }
            )
            logging.info(f"Saved {ticker}: {len(df)} rows -> {out_path}")

        except Exception as e:
            msg = str(e)
            failures.append({"ticker": ticker, "error": msg})
            logging.error(f"FAILED {ticker}: {msg}")

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

    manifest_path = write_manifest(out_dir, manifest)
    logging.info(f"Run manifest saved -> {manifest_path}")

    if len(successes) == 0:
        raise RuntimeError("All tickers failed. See run_manifest.json for details.")


if __name__ == "__main__":
    main()