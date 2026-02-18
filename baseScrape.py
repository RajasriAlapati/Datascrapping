"""
WTO Data Scraper → MinIO (S3-Compatible) Storage
=================================================
Scrapes multiple WTO Timeseries API endpoints and stores
raw JSON + converted CSV files in an S3 bucket.

Requires a FREE WTO API key from: https://apiportal.wto.org/

Usage:
    # One-shot run
    python wto_scraper.py

    # Keep running on cron schedule (default: daily at 02:00)
    python wto_scraper.py --cron

Dependencies:
    pip install requests boto3 pandas schedule
"""

# =====================================================
# STDLIB
# =====================================================
import io
import json
import logging
import time
import argparse
from datetime import datetime, timezone

# =====================================================
# THIRD-PARTY
# =====================================================
import requests
import boto3
import pandas as pd
import schedule

# =====================================================
# CONFIGURATION – ADAPT TO YOUR SETUP
# =====================================================
# ── WTO API ──────────────────────────────────────────
WTO_API_KEY    = "YOUR_WTO_API_KEY"           # get free key at https://apiportal.wto.org/
WTO_BASE_URL   = "https://api.wto.org/timeseries/v1"

# ── MinIO (S3-compatible) settings ───────────────────
MINIO_ENDPOINT = "https://s3.us-east-005.oriobjects.cloud"
MINIO_BUCKET   = "holacracydata"
ACCESS_KEY     = "005775aede18e2e0000000023"
SECRET_KEY     = "K005GD3X7YxPdbUEtP9mfYwatqf/ugg"
PREFIX         = "wits/trains"                # base path in bucket

# ── Scrape settings ───────────────────────────────────
# Indicator codes to download (WTO Timeseries codes).
# Common ones – add/remove as needed.
INDICATOR_CODES = [
    "ITS_MTV_AX",   # Merchandise exports – value (annual)
    "ITS_MTV_AM",   # Merchandise imports – value (annual)
    "ITS_CS_AX5",   # Commercial services exports (annual)
    "ITS_CS_AM5",   # Commercial services imports (annual)
    "HS_M_0010",    # Imports by HS2 product (if tariff data needed)
    "TA_1_MFN_BD",  # Bound MFN tariff rates
    "TA_2_AD_AHS",  # Applied ad-valorem tariffs (HS)
]

# Reporting economies: use "all" or a comma-separated list of ISO3 codes
# e.g.  "USA,CHN,IND,DEU,BRA"
REPORTING_ECONOMIES = "all"

# Partner economies (for bilateral data). Use "all" or codes.
PARTNER_ECONOMIES   = "all"

# Year range
YEAR_START = 2018
YEAR_END   = datetime.now().year

# Max rows per API call (WTO limit: 1 000 000)
MAX_ROWS   = 500_000

# ── Cron schedule ────────────────────────────────────
CRON_TIME  = "02:00"   # HH:MM daily (server local time)

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# =====================================================
# S3 CLIENT (MINIO)
# =====================================================
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="us-east-1",   # may be ignored by MinIO
)


# =====================================================
# HELPERS
# =====================================================

def s3_key(indicator: str, suffix: str) -> str:
    """Build a deterministic S3 object key."""
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    return f"{PREFIX}/{indicator}/{date_str}/{indicator}{suffix}"


def upload_bytes(data: bytes, key: str, content_type: str = "application/octet-stream") -> None:
    """Upload raw bytes to MinIO bucket."""
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
    )
    log.info("  ✔  Uploaded  s3://%s/%s  (%d bytes)", MINIO_BUCKET, key, len(data))


def upload_json(payload: dict | list, key: str) -> None:
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    upload_bytes(raw, key, "application/json")


def upload_dataframe_csv(df: pd.DataFrame, key: str) -> None:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    upload_bytes(buf.getvalue(), key, "text/csv")


# =====================================================
# WTO API HELPERS
# =====================================================

def wto_get(path: str, params: dict) -> dict:
    """
    Perform a GET request against the WTO Timeseries API.
    Raises RuntimeError on non-200 responses.
    """
    params["subscription-key"] = WTO_API_KEY
    url = f"{WTO_BASE_URL}{path}"

    log.debug("GET %s  params=%s", url, {k: v for k, v in params.items() if k != "subscription-key"})
    resp = requests.get(url, params=params, timeout=120)

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 60))
        log.warning("  Rate-limited. Sleeping %ds …", retry_after)
        time.sleep(retry_after)
        return wto_get(path, params)   # retry once

    if not resp.ok:
        raise RuntimeError(f"WTO API error {resp.status_code}: {resp.text[:300]}")

    return resp.json()


# =====================================================
# SCRAPE FUNCTIONS
# =====================================================

def fetch_metadata() -> None:
    """Download and store reference metadata: indicators, reporters, partners."""
    log.info("── Fetching metadata ──")

    endpoints = {
        "indicators":   ("/indicators",          {}),
        "reporters":    ("/reporters",            {}),
        "partners":     ("/partners",             {}),
        "topics":       ("/topics",               {}),
        "frequencies":  ("/frequencies",          {}),
        "periods":      ("/periods",              {"f": "A"}),
    }

    for name, (path, params) in endpoints.items():
        try:
            data = wto_get(path, params)
            key  = f"{PREFIX}/metadata/{name}.json"
            upload_json(data, key)
        except Exception as exc:
            log.error("  Metadata '%s' failed: %s", name, exc)


def fetch_indicator(indicator_code: str) -> None:
    """
    Download one indicator's data, upload as JSON + CSV.
    Handles pagination automatically (page param).
    """
    log.info("── Fetching indicator: %s ──", indicator_code)

    all_rows: list[dict] = []
    page = 1

    while True:
        params = {
            "i":    indicator_code,
            "r":    REPORTING_ECONOMIES,
            "p":    PARTNER_ECONOMIES,
            "ps":   f"{YEAR_START}-{YEAR_END}",
            "freq": "A",            # Annual; change to Q or M for quarterly/monthly
            "fmt":  "json",
            "max":  MAX_ROWS,
            "page": page,
        }

        try:
            payload = wto_get("/data", params)
        except Exception as exc:
            log.error("  Page %d fetch failed: %s", page, exc)
            break

        dataset = payload.get("Dataset", [])
        if not dataset:
            log.info("  No more data at page %d.", page)
            break

        all_rows.extend(dataset)
        log.info("  Page %d → %d rows (total so far: %d)", page, len(dataset), len(all_rows))

        # If we received fewer rows than the max, there are no more pages
        if len(dataset) < MAX_ROWS:
            break

        page += 1
        time.sleep(1)   # polite delay between pages

    if not all_rows:
        log.warning("  No data returned for %s.", indicator_code)
        return

    # ── Upload raw JSON ───────────────────────────────
    raw_key = s3_key(indicator_code, "_raw.json")
    upload_json(all_rows, raw_key)

    # ── Convert to DataFrame & upload CSV ────────────
    try:
        df  = pd.DataFrame(all_rows)
        csv_key = s3_key(indicator_code, "_data.csv")
        upload_dataframe_csv(df, csv_key)
        log.info("  Indicator %s: %d total rows stored.", indicator_code, len(df))
    except Exception as exc:
        log.error("  CSV conversion failed for %s: %s", indicator_code, exc)


def fetch_indicator_metadata(indicator_code: str) -> None:
    """Fetch and store metadata specific to one indicator."""
    try:
        data = wto_get("/indicators", {"i": indicator_code})
        key  = s3_key(indicator_code, "_metadata.json")
        upload_json(data, key)
    except Exception as exc:
        log.error("  Indicator metadata for %s failed: %s", indicator_code, exc)


# =====================================================
# MAIN SCRAPE JOB
# =====================================================

def run_scrape() -> None:
    """Full scrape job: metadata + all configured indicators."""
    log.info("========================================")
    log.info("WTO Scrape Job Started  –  %s UTC", datetime.now(timezone.utc).isoformat())
    log.info("========================================")

    # 1. Global metadata
    fetch_metadata()

    # 2. Each indicator
    for code in INDICATOR_CODES:
        try:
            fetch_indicator_metadata(code)
            fetch_indicator(code)
        except Exception as exc:
            log.error("Indicator %s failed unexpectedly: %s", code, exc)
        time.sleep(2)   # polite delay between indicators

    log.info("========================================")
    log.info("WTO Scrape Job Finished  –  %s UTC", datetime.now(timezone.utc).isoformat())
    log.info("========================================")


# =====================================================
# ENTRY POINT
# =====================================================

def main():
    parser = argparse.ArgumentParser(description="WTO data scraper → MinIO S3")
    parser.add_argument(
        "--cron",
        action="store_true",
        help=f"Run on a daily cron schedule at {CRON_TIME} (keeps process alive)",
    )
    args = parser.parse_args()

    if args.cron:
        log.info("Cron mode: scheduled daily at %s (server local time).", CRON_TIME)
        log.info("Running an immediate first pass …")
        run_scrape()

        schedule.every().day.at(CRON_TIME).do(run_scrape)
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        run_scrape()


if __name__ == "__main__":
    main()