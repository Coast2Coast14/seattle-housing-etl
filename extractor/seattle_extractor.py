"""
Extractor: Pulls the newest housing permit/listing records
from the City of Seattle's Open Data portal (Socrata API).

Dataset used: Seattle Building Permits
Endpoint: https://data.seattle.gov/resource/76t5-zqzr.json
"""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger("seattle_housing_etl.extractor")

# Seattle Open Data — Building Permits dataset (publicly available, no key required)
SEATTLE_OPEN_DATA_URL = "https://data.seattle.gov/resource/76t5-zqzr.json"

# Fields we care about; $select keeps the payload lean
# Field names match the actual Socrata API schema for dataset 76t5-zqzr
SELECTED_FIELDS = ",".join([
    "PermitNum",
    "PermitClass",
    "PermitClassMapped",
    "PermitTypeMapped",
    "PermitTypeDesc",
    "Description",
    "EstProjectCost",
    "IssuedDate",
    "ExpiresDate",
    "StatusCurrent",
    "ContractorCompanyName",
    "OriginalAddress1",
    "OriginalCity",
    "OriginalZip",
    "Latitude",
    "Longitude",
    "HousingUnitsAdded",
    "HousingCategory",
])


class SeattleHousingExtractor:
    """Fetches the N newest housing-related building permits from Seattle Open Data."""

    def __init__(self, limit: int = 50, timeout: int = 30) -> None:
        self.limit = limit
        self.timeout = timeout

    def fetch(self) -> list[dict[str, Any]]:
        """
        Query the Socrata API for the newest residential permits.

        Returns:
            List of raw record dicts.

        Raises:
            requests.HTTPError: On non-2xx responses.
            requests.Timeout:   If the request exceeds `self.timeout` seconds.
        """
        # Filter to residential/housing permits only and sort newest-first
        params = {
            "$limit": self.limit,
            "$order": "IssuedDate DESC",
            "$where": (
                "PermitClassMapped='Residential' AND "
                "IssuedDate IS NOT NULL"
            ),
            "$select": SELECTED_FIELDS,
        }

        logger.debug(f"GET {SEATTLE_OPEN_DATA_URL} params={params}")

        response = requests.get(
            SEATTLE_OPEN_DATA_URL,
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json"},
        )
        response.raise_for_status()

        records: list[dict[str, Any]] = response.json()
        logger.info(f"Extractor received {len(records)} raw records")
        return records
