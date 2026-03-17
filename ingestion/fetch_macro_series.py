import os
import io
import json
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import requests

SERIES = ["CPIAUCSL", "FEDFUNDS", "UNRATE", "DGS10", "VIXCLS"]

BASE_RAW_DIR = os.path.join("data", "raw", "macro")
TIMEOUT = 120
MAX_ATTEMPTS = 5

def fred_csv_url(series_id: str) -> str:
    return f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = os.path.join(BASE_RAW_DIR, f"dt={dt}")
    series_dir = os.path.join(out_dir, "series")

    os.makedirs(series_dir, exist_ok=True)

    run_started_utc = datetime.now(timezone.utc).isoformat()
    successes = []
    failures = []

    for sid in SERIES:
        url = fred_csv_url(sid)
        logging.info(f"Fetching {sid} from {url}")

        try:
            last_error = None
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    r = requests.get(
                        url,
                        timeout=TIMEOUT,
                        headers={"User-Agent": "risk-markets-lakehouse/1.0", "Accept": "text/csv"},
                    )
                    r.raise_for_status()
                    break
                except Exception as e:
                    last_error = e
                    logging.warning(f"{sid}: attempt {attempt}/{MAX_ATTEMPTS} failed: {e}")
                    time.sleep(2 * attempt)  # simple backoff
            else:
                raise last_error
            
            df = pd.read_csv(io.StringIO(r.text))

            df.columns = [c.strip() for c in df.columns]

            if "observation_date" in df.columns and "DATE" not in df.columns:
                df.rename(columns={"observation_date": "DATE"}, inplace=True)

            if "DATE" not in df.columns:
                raise ValueError(f"{sid}: Missing DATE column. Columns={list(df.columns)}")

            other_cols = [c for c in df.columns if c != "DATE"]
            if len(other_cols) == 0:
                raise ValueError(f"{sid}: Missing value column. Columns={list(df.columns)}")
            if "VALUE" not in df.columns:
                df.rename(columns={other_cols[0]: "VALUE"}, inplace=True)

            df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
            df["VALUE"] = pd.to_numeric(df["VALUE"].replace(".", None), errors="coerce")

            if df["DATE"].isna().all():
                raise ValueError(f"{sid}: DATE parsing failed (all NaT)")
            if df.empty:
                raise ValueError(f"{sid}: empty dataset")

            out_path = os.path.join(series_dir, f"{sid.lower()}.csv")
            df.to_csv(out_path, index=False)

            successes.append(
                {"series": sid, "rows": int(len(df)), "columns": list(df.columns), "file": out_path.replace("\\", "/")}
            )
            logging.info(f"Saved {sid}: {len(df)} rows -> {out_path}")

        except Exception as e:
            failures.append({"series": sid, "error": str(e)})
            logging.error(f"FAILED {sid}: {e}")

    run_finished_utc = datetime.now(timezone.utc).isoformat()

    manifest = {
        "run_started_utc": run_started_utc,
        "run_finished_utc": run_finished_utc,
        "partition_dt": dt,
        "source": "fred",
        "series_requested": SERIES,
        "success_count": len(successes),
        "failure_count": len(failures),
        "successes": successes,
        "failures": failures,
    }

    manifest_path = os.path.join(out_dir, "run_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    logging.info(f"Run manifest saved -> {manifest_path}")

    if len(successes) == 0:
        raise RuntimeError("All macro series failed. See run_manifest.json for details.")


if __name__ == "__main__":
    main()