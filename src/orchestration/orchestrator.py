"""
orchestration/orchestrator.py
------------------------------
Standalone pipeline runner — executes all three phases in sequence
without requiring Airflow. Useful for local development and Docker runs.
"""

import logging
import sys
import os

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.ingestion.ingest import ingest_all
from src.transformation.transform import transform_all
from src.validation.validate import validate_all
from src.warehouse.warehouse import load_to_warehouse


def setup_logging():
    """Configure logging for the pipeline."""
    log_dir = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(os.path.join(log_dir, "pipeline.log")),
        ],
    )


def run_pipeline():
    """
    Execute the full ETL pipeline:
      1. Ingest raw data from CSV / API
      2. Transform and compute metrics
      3. Validate data quality
      4. Load into the warehouse
    """
    setup_logging()
    logger = logging.getLogger("orchestrator")

    logger.info("╔══════════════════════════════════════╗")
    logger.info("║  GCP Security Monitoring Pipeline    ║")
    logger.info("╚══════════════════════════════════════╝")

    try:
        # PHASE 1: Ingest
        logger.info("PHASE 1 ▶ Ingestion")
        raw_data = ingest_all()

        # PHASE 2: Transform
        logger.info("PHASE 2 ▶ Transformation")
        transformed_data = transform_all(raw_data)

        # PHASE 3: Validate
        logger.info("PHASE 3 ▶ Validation")
        validation_report = validate_all(transformed_data["cleaned"])

        if not validation_report["all_passed"]:
            logger.warning(
                f"⚠️  {validation_report['failed']} validation check(s) failed. "
                "Proceeding to load with warnings."
            )

        # PHASE 4: Load
        logger.info("PHASE 4 ▶ Warehouse Load")
        load_to_warehouse(raw_data, transformed_data)

        logger.info("✅ Pipeline completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
