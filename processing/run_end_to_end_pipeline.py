import logging
import subprocess
import sys
import time
from pathlib import Path

PIPELINE_STEPS = [
    # ---------------------------
    # Raw ingestion
    # ---------------------------
    "ingestion/fetch_market_prices.py",
    "ingestion/fetch_fx_rates.py",
    "ingestion/fetch_macro_series.py",

    # ---------------------------
    # Raw loaders
    # ---------------------------
    "ingestion/load_raw_market_to_postgres.py",
    "ingestion/load_raw_fx_to_postgres.py",
    "ingestion/load_raw_macro_to_postgres.py",

    # ---------------------------
    # Silver builds
    # ---------------------------
    "processing/build_silver_market_prices.py",
    "processing/build_silver_fx_macro.py",

    # ---------------------------
    # Curated layer
    # ---------------------------
    "processing/run_curated_pipeline.py",

    # ---------------------------
    # Validation
    # ---------------------------
    "processing/validate_curated_pipeline.py",
]


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def run_step(repo_root: Path, script_relative_path: str) -> None:
    script_path = repo_root / script_relative_path

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    logging.info("Starting step: %s", script_relative_path)
    step_started = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=repo_root,
        check=False
    )

    duration_seconds = round(time.time() - step_started, 2)

    if result.returncode != 0:
        raise RuntimeError(
            f"Step failed: {script_relative_path} | exit_code={result.returncode} | duration_seconds={duration_seconds}"
        )

    logging.info("Completed step: %s | duration_seconds=%s", script_relative_path, duration_seconds)


def main():
    setup_logging()

    repo_root = Path(__file__).resolve().parent.parent
    pipeline_started = time.time()

    logging.info("Starting end-to-end pipeline runner...")
    logging.info("Repository root: %s", repo_root)

    try:
        for script_relative_path in PIPELINE_STEPS:
            run_step(repo_root, script_relative_path)

        total_duration = round(time.time() - pipeline_started, 2)
        logging.info("End-to-end pipeline completed successfully.")
        logging.info("Total duration_seconds=%s", total_duration)

    except Exception:
        total_duration = round(time.time() - pipeline_started, 2)
        logging.exception("End-to-end pipeline failed after %s seconds.", total_duration)
        raise SystemExit(1)


if __name__ == "__main__":
    main()