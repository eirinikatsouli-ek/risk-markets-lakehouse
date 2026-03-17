import logging
import subprocess
import sys
import time
from pathlib import Path

PIPELINE_STEPS = [
    "build_curated_market_features.py",
    "build_curated_pair_correlations.py",
    "build_curated_market_macro.py",
    "build_curated_fx_features.py",
    "build_curated_asset_risk_snapshot_latest.py",
    "build_curated_asset_pair_correlation_snapshot_latest.py",
    "build_curated_fx_risk_snapshot_latest.py",
    "build_curated_market_regime_daily.py",
    "build_curated_asset_regime_context_daily.py",
    "build_curated_asset_performance_by_regime.py",
    "build_curated_pair_correlation_by_regime.py",
]


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def run_step(processing_dir: Path, script_name: str) -> None:
    script_path = processing_dir / script_name

    if not script_path.exists():
        raise FileNotFoundError(f"Script not found: {script_path}")

    logging.info("Starting step: %s", script_name)
    step_started = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=processing_dir.parent,
        check=False
    )

    duration_seconds = round(time.time() - step_started, 2)

    if result.returncode != 0:
        raise RuntimeError(
            f"Step failed: {script_name} | exit_code={result.returncode} | duration_seconds={duration_seconds}"
        )

    logging.info("Completed step: %s | duration_seconds=%s", script_name, duration_seconds)


def main():
    setup_logging()

    processing_dir = Path(__file__).resolve().parent
    pipeline_started = time.time()

    logging.info("Starting curated pipeline runner...")
    logging.info("Processing directory: %s", processing_dir)

    try:
        for script_name in PIPELINE_STEPS:
            run_step(processing_dir, script_name)

        total_duration = round(time.time() - pipeline_started, 2)
        logging.info("Curated pipeline completed successfully.")
        logging.info("Total duration_seconds=%s", total_duration)

    except Exception as exc:
        total_duration = round(time.time() - pipeline_started, 2)
        logging.exception("Curated pipeline failed after %s seconds.", total_duration)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()