"""
Transformer: Cleans, casts, and enriches raw Seattle housing records.
Outputs a pandas DataFrame ready for S3 load.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger("seattle_housing_etl.transformer")


class HousingTransformer:
    """Applies cleaning and enrichment rules to raw permit records."""

    # Columns that should be numeric
    NUMERIC_COLS = ["estprojectcost", "latitude", "longitude", "housingunitsadded"]

    # Columns that should be datetime
    DATE_COLS = ["issueddate", "expiresdate"]

    def transform(self, raw_records: list[dict[str, Any]]) -> pd.DataFrame:
        """
        Full transformation pipeline.

        Steps:
            1. Load into DataFrame
            2. Normalise column names
            3. Cast data types
            4. Derive helper columns
            5. Drop duplicates & null-permit rows
            6. Sort newest-first

        Args:
            raw_records: List of dicts from the extractor.

        Returns:
            Cleaned, enriched DataFrame.
        """
        if not raw_records:
            logger.warning("Transformer received 0 records — returning empty DataFrame")
            return pd.DataFrame()

        df = pd.DataFrame(raw_records)
        logger.info(f"Transformer starting with {len(df)} rows, {len(df.columns)} columns")

        df = self._normalize_columns(df)
        df = self._cast_types(df)
        df = self._derive_columns(df)
        df = self._clean(df)

        logger.info(f"Transformer finished: {len(df)} rows after cleaning")
        return df

    # ── Private helpers ───────────────────────────────────────────────────────

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase and strip whitespace from all column names."""
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        return df

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast numeric and datetime columns; coerce errors to NaN/NaT."""
        for col in self.NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in self.DATE_COLS:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

        return df

    def _derive_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add enrichment columns useful for downstream analysis."""
        # Project value bucket
        if "estprojectcost" in df.columns:
            df["value_bucket"] = pd.cut(
                df["estprojectcost"],
                bins=[0, 500_000, 1_000_000, 2_000_000, 5_000_000, float("inf")],
                labels=["<500K", "500K–1M", "1M–2M", "2M–5M", "5M+"],
                right=False,
            ).astype(str)

        # Days since permit issued
        if "issueddate" in df.columns:
            now = pd.Timestamp.utcnow()
            df["days_since_issued"] = (now - df["issueddate"]).dt.days

        # ETL metadata
        df["etl_ingested_at"] = pd.Timestamp.utcnow().isoformat()
        df["data_source"] = "seattle_open_data_building_permits"

        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop rows missing a permit number and remove duplicates."""
        before = len(df)
        df = df.dropna(subset=["permitnum"])
        df = df.drop_duplicates(subset=["permitnum"])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows (missing permit number or duplicates)")
        return df.sort_values("issueddate", ascending=False).reset_index(drop=True)
