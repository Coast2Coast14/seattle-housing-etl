"""
Airflow DAG: Seattle Housing ETL
Runs every Monday at 07:00 UTC, fetches the 50 newest Seattle housing
permits, transforms them, loads the result to Amazon S3, and generates
a neighborhood heat map for the weekly newsletter.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Allow imports from the project root inside the Airflow container ──────────
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="seattle_housing_weekly_etl",
    description="Weekly ingest of Seattle housing permits → S3 (Parquet) + newsletter heat map",
    schedule="0 7 * * MON",
    start_date=datetime(2025, 1, 6),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["seattle", "housing", "s3", "etl", "weekly", "newsletter"],
    doc_md=__doc__,
)
def seattle_housing_weekly_etl():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # ── EXTRACT ───────────────────────────────────────────────────────────────
    @task(task_id="extract_seattle_housing")
    def extract(**context) -> list[dict]:
        """Pull the 50 newest residential permits from Seattle Open Data."""
        from extractor.seattle_extractor import SeattleHousingExtractor

        limit = int(Variable.get("seattle_housing_record_limit", default_var=50))
        extractor = SeattleHousingExtractor(limit=limit, timeout=30)
        records = extractor.fetch()

        log.info(f"Extracted {len(records)} records")
        context["ti"].xcom_push(key="records_extracted", value=len(records))
        return records

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    @task(task_id="transform_housing_data")
    def transform(raw_records: list[dict], **context) -> list[dict]:
        """Clean, cast types, and enrich raw permit records."""
        from transformer.housing_transformer import HousingTransformer

        if not raw_records:
            raise ValueError("No records to transform.")

        transformer = HousingTransformer()
        df = transformer.transform(raw_records)

        log.info(f"Transformed {len(df)} records")
        context["ti"].xcom_push(key="records_transformed", value=len(df))

        # Convert Timestamps to strings for XCom JSON serialisation
        df["issueddate"] = df["issueddate"].astype(str)
        df["expiresdate"] = df["expiresdate"].astype(str)
        df["etl_ingested_at"] = df["etl_ingested_at"].astype(str)
        return df.to_dict(orient="records")

    # ── LOAD ──────────────────────────────────────────────────────────────────
    @task(task_id="load_to_s3")
    def load(transformed_records: list[dict], **context) -> str:
        """Serialize to Parquet and upload to S3."""
        import pandas as pd
        from loader.s3_loader import S3Loader

        s3_bucket = Variable.get("seattle_housing_s3_bucket")
        s3_prefix = Variable.get(
            "seattle_housing_s3_prefix", default_var="seattle-housing/raw"
        )
        aws_region = Variable.get("seattle_housing_aws_region", default_var="us-west-2")
        run_id = context["run_id"]

        df = pd.DataFrame(transformed_records)
        loader = S3Loader(bucket=s3_bucket, prefix=s3_prefix, region=aws_region)
        s3_key = loader.load(df, run_id=run_id)

        log.info(f"Loaded data to s3://{s3_bucket}/{s3_key}")
        context["ti"].xcom_push(key="s3_path", value=f"s3://{s3_bucket}/{s3_key}")
        return s3_key

    # ── GENERATE HEAT MAP ─────────────────────────────────────────────────────
    @task(task_id="generate_heatmap")
    def generate_heatmap(**context) -> str:
        """Generate neighborhood heat map HTML from latest S3 Parquet file."""
        from newsletter.heatmap import generate

        s3_bucket = Variable.get("seattle_housing_s3_bucket")
        s3_prefix = Variable.get(
            "seattle_housing_s3_prefix", default_var="seattle-housing/raw"
        )
        aws_region = Variable.get("seattle_housing_aws_region", default_var="us-west-2")

        result = generate(
            bucket=s3_bucket,
            prefix=s3_prefix,
            region=aws_region,
            output_dir="/opt/airflow/heatmap_output",
        )

        log.info(
            f"Heat map generated | top_zip={result['top_zip']} | "
            f"total_permits={result['total_permits']} | week={result['week_label']}"
        )
        context["ti"].xcom_push(key="heatmap_top_zip", value=result["top_zip"])
        context["ti"].xcom_push(key="heatmap_week_label", value=result["week_label"])
        return result["full_html_path"]

    # ── SUMMARY ───────────────────────────────────────────────────────────────
    @task(
        task_id="log_pipeline_summary",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def log_summary(**context):
        """Log a run summary."""
        ti = context["ti"]
        extracted = ti.xcom_pull(
            task_ids="extract_seattle_housing", key="records_extracted"
        )
        transformed = ti.xcom_pull(
            task_ids="transform_housing_data", key="records_transformed"
        )
        s3_path = ti.xcom_pull(task_ids="load_to_s3", key="s3_path")
        top_zip = ti.xcom_pull(task_ids="generate_heatmap", key="heatmap_top_zip")
        week_label = ti.xcom_pull(task_ids="generate_heatmap", key="heatmap_week_label")

        log.info(
            f"Seattle Housing ETL complete | "
            f"extracted={extracted} | transformed={transformed} | "
            f"s3_path={s3_path} | top_zip={top_zip} | week={week_label}"
        )

    # ── Wire the DAG ──────────────────────────────────────────────────────────
    raw = extract()
    clean = transform(raw)
    s3_key = load(clean)
    heatmap = generate_heatmap()
    summary = log_summary()

    start >> raw >> clean >> s3_key >> heatmap >> summary >> end


seattle_housing_weekly_etl()
