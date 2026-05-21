"""
Seattle Housing ETL Pipeline
Fetches the 50 newest housing listings from Seattle Open Data,
transforms them, and loads to Amazon S3.
"""

import logging
import sys
from datetime import datetime

from extractor.seattle_extractor import SeattleHousingExtractor
from transformer.housing_transformer import HousingTransformer
from loader.s3_loader import S3Loader
from utils.logger import setup_logger


def run_pipeline(config: dict) -> dict:
    """
    Run the full ETL pipeline.

    Args:
        config: Pipeline configuration dictionary

    Returns:
        Summary dict with run metadata
    """
    logger = setup_logger("seattle_housing_etl")
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    logger.info(f"Starting Seattle Housing ETL pipeline | run_id={run_id}")

    summary = {
        "run_id": run_id,
        "status": "failed",
        "records_extracted": 0,
        "records_transformed": 0,
        "records_loaded": 0,
        "s3_path": None,
    }

    try:
        # ── EXTRACT ──────────────────────────────────────────────────────────
        logger.info("EXTRACT: Fetching 50 newest Seattle housing listings...")
        extractor = SeattleHousingExtractor(
            limit=config.get("record_limit", 50),
            timeout=config.get("request_timeout", 30),
        )
        raw_records = extractor.fetch()
        summary["records_extracted"] = len(raw_records)
        logger.info(f"EXTRACT: Retrieved {len(raw_records)} records")

        # ── TRANSFORM ─────────────────────────────────────────────────────────
        logger.info("TRANSFORM: Cleaning and enriching records...")
        transformer = HousingTransformer()
        transformed_df = transformer.transform(raw_records)
        summary["records_transformed"] = len(transformed_df)
        logger.info(f"TRANSFORM: {len(transformed_df)} records ready for load")

        # ── LOAD ──────────────────────────────────────────────────────────────
        logger.info("LOAD: Writing to S3...")
        loader = S3Loader(
            bucket=config["s3_bucket"],
            prefix=config.get("s3_prefix", "seattle-housing/raw"),
            region=config.get("aws_region", "us-west-2"),
        )
        s3_path = loader.load(transformed_df, run_id=run_id)
        summary["records_loaded"] = len(transformed_df)
        summary["s3_path"] = s3_path
        summary["status"] = "success"
        logger.info(f"LOAD: Data written to s3://{config['s3_bucket']}/{s3_path}")

    except Exception as exc:
        logger.exception(f"Pipeline failed: {exc}")
        summary["error"] = str(exc)
        sys.exit(1)

    logger.info(f"Pipeline complete | summary={summary}")
    return summary


if __name__ == "__main__":
    import os

    pipeline_config = {
        "record_limit": 50,
        "request_timeout": 30,
        # ── S3 Settings ──────────────────────────────────────────────────────
        # Set these via environment variables or replace directly.
        "s3_bucket": os.environ.get("S3_BUCKET", "your-bucket-name"),
        "s3_prefix": os.environ.get("S3_PREFIX", "seattle-housing/raw"),
        "aws_region": os.environ.get("AWS_REGION", "us-west-2"),
    }

    result = run_pipeline(pipeline_config)
    print(result)
