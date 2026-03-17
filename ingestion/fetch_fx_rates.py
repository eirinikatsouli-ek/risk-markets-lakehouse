import os
import io
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import requests
import pandas as pd

PAIRS = ["EURUSD", "EURGBP", "USDJPY"]
EXPECTED_COLUMNS = ["Date", "Open", "High", "Low", "Close"]
NUMERIC_COLUMNS = ["Open", "High", "Low", "Close"]

BASE_RAW_DIR = os.path.join("data", "raw", "fx_rates")
TIMEOUT = 30


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def stooq_fx_url(pair: str) -> str:
    return f"https://stooq.com/q/d/l/?s={pair.lower()}&i=d"


def validate_df(df: pd.DataFrame, pair: str) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"{pair}: Missing columns: {missing}")

    if df.empty:
        errors.append(f"{pair}: Empty dataset")

    if "Date" in df.columns:
        parsed = pd.to_datetime(df["Date"], errors="coerce")
        if parsed.isna().all():
            errors.append(f"{pair}: Could not parse any Date values")
        df["Date"] = parsed.dt.date

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            coerced = pd.to_numeric(df[col], errors="coerce")
            if coerced.isna().all():
                errors.append(f"{pair}: Column {col} could not be parsed as numeric")
            df[col] = coerced

    return (len(errors) == 0), errors


def fetch_one(pair: str) -> pd.DataFrame:
    url = stooq_fx_url(pair)
    logging.info(f"Fetching {pair} from {url}")
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text))


def write_pair_file(df: pd.DataFrame, out_dir: str, pair: str) -> str:
    pairs_dir = os.path.join(out_dir, "pairs")
    ensure_dir(pairs_dir)
    out_path = os.path.join(pairs_dir, f"{pair.lower()}.csv")
    df.to_csv(out_path, index=False)
    return out_path


def write_manifest(out_dir: str, manifest: Dict) -> str:
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
    successes, failures = [], []

    for pair in PAIRS:
        try:
            df = fetch_one(pair)
            df["Pair"] = pair

            ok, errs = validate_df(df, pair)
            if not ok:
                raise ValueError(" | ".join(errs))

            out_path = write_pair_file(df, out_dir, pair)
            successes.append({"pair": pair, "rows": int(len(df)), "columns": list(df.columns), "file": out_path.replace("\\", "/")})
            logging.info(f"Saved {pair}: {len(df)} rows -> {out_path}")

        except Exception as e:
            failures.append({"pair": pair, "error": str(e)})
            logging.error(f"FAILED {pair}: {e}")

    run_finished_utc = datetime.now(timezone.utc).isoformat()

    manifest = {
        "run_started_utc": run_started_utc,
        "run_finished_utc": run_finished_utc,
        "partition_dt": dt,
        "source": "stooq",
        "pairs_requested": PAIRS,
        "success_count": len(successes),
        "failure_count": len(failures),
        "successes": successes,
        "failures": failures,
        "expected_columns": EXPECTED_COLUMNS,
    }

    manifest_path = write_manifest(out_dir, manifest)
    logging.info(f"Run manifest saved -> {manifest_path}")

    if len(successes) == 0:
        raise RuntimeError("All FX pairs failed. See run_manifest.json for details.")


if __name__ == "__main__":
    main()