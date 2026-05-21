"""
Loader: Writes the transformed DataFrame to Amazon S3 as a
partitioned Parquet file (date-based Hive-style partitioning).

Output path pattern:
    s3://<bucket>/<prefix>/year=YYYY/month=MM/day=DD/<run_id>.parquet
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger("seattle_housing_etl.loader")


class S3Loader:
    """Loads a DataFrame to S3 as Parquet with date-based partitioning."""

    def __init__(self, bucket: str, prefix: str, region: str = "us-west-2") -> None:
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.region = region
        self._client = boto3.client("s3", region_name=region)

    def load(self, df: pd.DataFrame, run_id: str | None = None) -> str:
        """
        Serialize DataFrame to Parquet and upload to S3.

        Args:
            df:     Transformed DataFrame to upload.
            run_id: Unique run identifier used as the filename.

        Returns:
            The S3 object key (path without bucket name).

        Raises:
            ValueError:   If the DataFrame is empty.
            ClientError:  On S3 API errors.
        """
        if df.empty:
            raise ValueError("Cannot load an empty DataFrame to S3.")

        run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        s3_key = self._build_key(run_id)

        parquet_buffer = self._to_parquet(df)
        self._upload(parquet_buffer, s3_key)

        return s3_key

    # ── Private helpers ───────────────────────────────────────────────────────

    def _build_key(self, run_id: str) -> str:
        """Build a Hive-style partitioned S3 key."""
        now = datetime.now(timezone.utc)
        return (
            f"{self.prefix}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"{run_id}.parquet"
        )

    @staticmethod
    def _to_parquet(df: pd.DataFrame) -> io.BytesIO:
        """Serialize DataFrame to an in-memory Parquet buffer."""
        # Cast problematic types before serialisation
        for col in df.select_dtypes(include=["category"]).columns:
            df[col] = df[col].astype(str)

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
        buffer.seek(0)
        logger.debug(f"Parquet buffer size: {buffer.getbuffer().nbytes / 1024:.1f} KB")
        return buffer

    def _upload(self, buffer: io.BytesIO, s3_key: str) -> None:
        """Upload the in-memory buffer to S3."""
        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=buffer,
                ContentType="application/octet-stream",
                Metadata={
                    "pipeline": "seattle-housing-etl",
                    "source": "seattle-open-data",
                },
            )
            logger.info(f"Uploaded s3://{self.bucket}/{s3_key}")
        except (BotoCoreError, ClientError) as exc:
            logger.error(f"S3 upload failed for key '{s3_key}': {exc}")
            raise
