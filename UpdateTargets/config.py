"""
Central configuration for the INFOLINK Targets scripts.

Share the code safely by setting credentials via environment variables:
  - DHIS2_USERNAME
  - DHIS2_PASSWORD
  - DHIS2_BASE_URL (optional; defaults to infolink.fhi360.org)
"""

from __future__ import annotations

import os

USERNAME="aejakhegbe"
PASSWORD=""
BASE_URL="https://infolink.fhi360.org"

def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


# === DHIS2 connection ===
DHIS2_BASE_URL = _env("DHIS2_BASE_URL", BASE_URL).rstrip("/")
DHIS2_API_VERSION = _env("DHIS2_API_VERSION", "29")
DHIS2_TIMEOUT_SECONDS = int(_env("DHIS2_TIMEOUT_SECONDS", "15") or "15")

DHIS2_USERNAME = _env("DHIS2_USERNAME", USERNAME)
DHIS2_PASSWORD = _env("DHIS2_PASSWORD", PASSWORD)


def dhis2_auth() -> tuple[str, str] | None:
    """Return (username, password) if present; otherwise None."""
    if not DHIS2_USERNAME or not DHIS2_PASSWORD:
        return None
    return (DHIS2_USERNAME, DHIS2_PASSWORD)


def api_url(path: str) -> str:
    path = path.lstrip("/")
    return f"{DHIS2_BASE_URL}/api/{DHIS2_API_VERSION}/{path}"


# === API endpoints used by scripts ===
DATASET_ELEMENTS_URL = api_url(
    "dataSets/KVx804ANUJW.json?fields=dataSetElements[dataElement[id,name],dataSet[id]]"
)
ORG_UNITS_L2_URL = api_url(
    "organisationUnits.json?filter=level:eq:2&fields=id,displayName&paging=false"
)


# === File names ===
INDICATOR_XLSX = "indicator.xlsx"
DATASET_FALLBACK_JSON = "data.json"
ORG_UNITS_FALLBACK_JSON = "orgUnits.json"

PART1_OUTPUT_XLSX = "transposed_with_fixedRoot.xlsx"
PART2_OUTPUT_XLSX = "transposed_with_orgUnitID.xlsx"
PART3_OUTPUT_CSV = "dhis2_import_ready.csv"


# === DHIS2 import constants (Part 3) ===
DHIS2_PERIOD = _env("DHIS2_PERIOD", "2025Oct")
CATEGORY_OPTION_COMBO_GP = _env("CATEGORY_OPTION_COMBO_GP", "qGHHBM7Kq0P")
ATTRIBUTE_OPTION_COMBO_DSD = _env("ATTRIBUTE_OPTION_COMBO_DSD", "BUsfBRCZeRF")
ATTRIBUTE_OPTION_COMBO_TA = _env("ATTRIBUTE_OPTION_COMBO_TA", "j19cE0qW6UH")

