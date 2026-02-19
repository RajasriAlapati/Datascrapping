"""
WTO Complete Dynamic Scraper
============================
Uses the WTO base URL to AUTO-DISCOVER every indicator, every reporter,
and every frequency — then downloads ALL of them to S3.

No hardcoded indicator lists. Everything is fetched live from:
  https://api.wto.org/timeseries/v1

Flow:
  Step 1  → GET /indicators        → discover ALL indicator codes + their frequencies
  Step 2  → GET /reporters         → discover ALL reporting economies
  Step 3  → GET /frequencies       → discover all valid frequency codes
  Step 4  → GET /topics            → store topic/category metadata
  Step 5  → For every indicator × reporter → GET /data → upload CSV only

USAGE
─────
  # Discover + scrape EVERYTHING (all indicators × all reporters)
  python wto_dynamic_scraper.py --api-key YOUR_KEY

  # Preview what would be scraped (dry run - no download)
  python wto_dynamic_scraper.py --api-key YOUR_KEY --dry-run

  # Scrape all indicators, only specific reporters
  python wto_dynamic_scraper.py --api-key YOUR_KEY --reporters USA,CHN,IND

  # Filter by topic category
  python wto_dynamic_scraper.py --api-key YOUR_KEY --topic "Merchandise trade"

  # Custom year range
  python wto_dynamic_scraper.py --api-key YOUR_KEY --year-start 2010 --year-end 2024

  # Run on daily cron
  python wto_dynamic_scraper.py --api-key YOUR_KEY --cron

  # Just print everything the API has (no scraping)
  python wto_dynamic_scraper.py --api-key YOUR_KEY --list-all
"""

import re
import time
import logging
import argparse
import schedule
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
import boto3
import pandas as pd
from botocore.config import Config


# =====================================================
# CONFIGURATION
# =====================================================

# ──── WTO API ────────────────────────────────────────
WTO_API_KEY  = "f65870b910744ac2875b62998a59dbf3"          # https://apiportal.wto.org/
WTO_BASE_URL = "https://api.wto.org/timeseries/v1"

# ──── S3 / MinIO ─────────────────────────────────────
AWS_ACCESS_KEY_ID     = "005775aede18e2e0000000023"
AWS_SECRET_ACCESS_KEY = "K005GD3X7YxPdbUEtP9mfYwatqf/ugg"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "holacracydata"
S3_PREFIX             = "wto/indicators"
S3_ENDPOINT           = "https://s3.us-east-005.oriobjects.cloud"

# ──── Scraping defaults ──────────────────────────────
DEFAULT_REPORTERS  = "all"        # "all" or "USA,CHN,IND,..."
DEFAULT_YEAR_START = 2000
DEFAULT_YEAR_END   = datetime.now(timezone.utc).year
DEFAULT_CRON_TIME  = "02:00"
MAX_ROWS           = 500_000      # WTO hard limit = 1,000,000
REQUEST_DELAY      = 0.5          # seconds between calls

RUN_DATE      = datetime.now(timezone.utc).strftime("%Y-%m-%d")
RUN_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# ──── Local output paths ──────────────────────────────
LOG_DIR   = Path("logs")
STATS_DIR = Path("stats")
LOG_DIR.mkdir(exist_ok=True)
STATS_DIR.mkdir(exist_ok=True)

LOG_FILE   = LOG_DIR   / f"wto_scraper_{RUN_TIMESTAMP}.log"
STATS_FILE = STATS_DIR / f"wto_stats_{RUN_TIMESTAMP}.json"


# =====================================================
# LOGGING SETUP  (file + console)
# =====================================================

def setup_logger() -> logging.Logger:
    """
    Returns a logger that writes to both the console (INFO)
    and a rotating log file (DEBUG).
    """
    logger = logging.getLogger("wto_scraper")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler ──────────────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # ── File handler ─────────────────────────────────
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info(f"Log file  : {LOG_FILE.resolve()}")
    logger.info(f"Stats file: {STATS_FILE.resolve()}")
    return logger


logger = setup_logger()


# =====================================================
# STATS TRACKER
# =====================================================

class StatsTracker:
    """
    Tracks per-run metrics and persists them to a JSON file after
    every update so you always have a recoverable snapshot.
    """

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.data: dict = {
            "run_id":            RUN_TIMESTAMP,
            "run_date":          RUN_DATE,
            "started_at":        datetime.now(timezone.utc).isoformat(),
            "finished_at":       None,
            "duration_seconds":  None,
            "config": {},
            "discovery": {
                "total_indicators": 0,
                "total_reporters":  0,
                "total_tasks":      0,
            },
            "progress": {
                "tasks_attempted": 0,
                "tasks_succeeded": 0,
                "tasks_failed":    0,
                "tasks_skipped":   0,
                "rows_downloaded": 0,
                "bytes_uploaded":  0,
            },
            "indicators": {},   # per-indicator breakdown
            "errors":     [],   # list of {indicator, reporter, error}
        }
        self._save()

    # ── helpers ──────────────────────────────────────

    def set_config(self, **kwargs):
        self.data["config"].update(kwargs)
        self._save()

    def set_discovery(self, indicators: int, reporters: int):
        self.data["discovery"]["total_indicators"] = indicators
        self.data["discovery"]["total_reporters"]  = reporters
        self.data["discovery"]["total_tasks"]      = indicators * reporters
        self._save()

    def record_success(self, indicator_code: str, reporter_iso3: str,
                       rows: int, bytes_uploaded: int):
        p = self.data["progress"]
        p["tasks_attempted"] += 1
        p["tasks_succeeded"] += 1
        p["rows_downloaded"] += rows
        p["bytes_uploaded"]  += bytes_uploaded

        ind = self._get_indicator(indicator_code)
        ind["succeeded"] += 1
        ind["total_rows"] += rows
        ind["reporters_done"].append(reporter_iso3)
        self._save()

    def record_failure(self, indicator_code: str, reporter_iso3: str, error: str):
        p = self.data["progress"]
        p["tasks_attempted"] += 1
        p["tasks_failed"]    += 1

        ind = self._get_indicator(indicator_code)
        ind["failed"] += 1
        ind["reporters_failed"].append(reporter_iso3)

        self.data["errors"].append({
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "indicator":  indicator_code,
            "reporter":   reporter_iso3,
            "error":      error,
        })
        self._save()

    def record_skip(self, indicator_code: str, reporter_iso3: str):
        self.data["progress"]["tasks_attempted"] += 1
        self.data["progress"]["tasks_skipped"]   += 1
        ind = self._get_indicator(indicator_code)
        ind["skipped"] += 1
        self._save()

    def finalize(self):
        self.data["finished_at"] = datetime.now(timezone.utc).isoformat()
        started  = datetime.fromisoformat(self.data["started_at"])
        finished = datetime.fromisoformat(self.data["finished_at"])
        self.data["duration_seconds"] = round((finished - started).total_seconds(), 2)
        self._save()
        logger.info(f"Stats saved → {self.filepath.resolve()}")

    def print_summary(self):
        p = self.data["progress"]
        d = self.data["discovery"]
        logger.info("=" * 70)
        logger.info("STATS SUMMARY")
        logger.info("=" * 70)
        logger.info(f"  Run ID            : {self.data['run_id']}")
        logger.info(f"  Started           : {self.data['started_at']}")
        logger.info(f"  Finished          : {self.data['finished_at']}")
        logger.info(f"  Duration          : {self.data['duration_seconds']}s "
                    f"({(self.data['duration_seconds'] or 0) / 60:.1f} min)")
        logger.info(f"  Indicators        : {d['total_indicators']}")
        logger.info(f"  Reporters         : {d['total_reporters']}")
        logger.info(f"  Total tasks       : {d['total_tasks']:,}")
        logger.info(f"  ✅ Succeeded      : {p['tasks_succeeded']:,}")
        logger.info(f"  ❌ Failed         : {p['tasks_failed']:,}")
        logger.info(f"  ⏭  Skipped        : {p['tasks_skipped']:,}")
        logger.info(f"  Rows downloaded   : {p['rows_downloaded']:,}")
        logger.info(f"  Bytes uploaded    : {p['bytes_uploaded']:,} "
                    f"({p['bytes_uploaded'] / 1_048_576:.1f} MB)")
        logger.info(f"  Stats file        : {self.filepath.resolve()}")
        logger.info(f"  Log file          : {LOG_FILE.resolve()}")
        logger.info("=" * 70)

    # ── internal ──────────────────────────────────────

    def _get_indicator(self, code: str) -> dict:
        if code not in self.data["indicators"]:
            self.data["indicators"][code] = {
                "succeeded":       0,
                "failed":          0,
                "skipped":         0,
                "total_rows":      0,
                "reporters_done":  [],
                "reporters_failed":[],
            }
        return self.data["indicators"][code]

    def _save(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, default=str)


# =====================================================
# S3 CLIENT
# =====================================================

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT,
    config=Config(signature_version="s3v4"),
)
logger.info("✓ S3 client initialized")


# =====================================================
# CLEAN NAME  (same pattern as SEC scraper)
# =====================================================

def clean_name(raw: str) -> str:
    """
    APPLE_INC style: UPPERCASE + underscores, no special chars.
    "China (People's Rep. of)" → "CHINA_PEOPLES_REP_OF"
    """
    name = raw.upper()
    name = re.sub(r'[.,/\\:*?"<>|()\[\]{}\'\`~!@#$%^&+=\-]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    name = name.replace(' ', '_')
    name = re.sub(r'_+', '_', name)
    return name.strip('_')


# =====================================================
# WTO API WRAPPER
# =====================================================

def wto_get(path: str, params: dict, api_key: str) -> dict:
    """
    GET any WTO Timeseries endpoint.
    Auto-retries on HTTP 429 (rate limit).
    Raises RuntimeError on any other failure.
    """
    p = dict(params)
    p["subscription-key"] = api_key

    url  = f"{WTO_BASE_URL}{path}"
    resp = requests.get(url, params=p, timeout=120)

    if resp.status_code == 429:
        wait = int(resp.headers.get("Retry-After", 60))
        logger.warning(f"Rate-limited. Sleeping {wait}s …")
        time.sleep(wait)
        return wto_get(path, params, api_key)   # retry

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} [{path}]: {resp.text[:300]}")

    return resp.json()


# =====================================================
# S3 UPLOAD HELPER  ─  CSV ONLY
# =====================================================

def upload_csv(csv_str: str, key: str) -> int:
    """Upload CSV string to S3. Returns number of bytes uploaded."""
    body = csv_str.encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="text/csv")
    logger.info(f"  ✓ Uploaded → s3://{S3_BUCKET}/{key}  ({len(body):,} bytes)")
    return len(body)


# =====================================================
# STEP 1 — DISCOVER ALL INDICATORS FROM BASE URL
# =====================================================

def discover_indicators(api_key: str) -> list[dict]:
    """
    Calls GET /indicators from the base URL.
    Returns full list of all indicator dicts:
      { "code": "ITS_MTV_AX", "name": "...", "frequencies": ["A"], ... }
    """
    logger.info(f"Discovering all indicators from WTO API …  [{WTO_BASE_URL}/indicators]")
    raw = wto_get("/indicators", {"lang": 1}, api_key)
    indicators = raw if isinstance(raw, list) else raw.get("indicators", [])
    logger.info(f"✓ Discovered {len(indicators)} indicators from WTO base URL")
    return indicators


# =====================================================
# STEP 2 — DISCOVER ALL REPORTERS FROM BASE URL
# =====================================================

def discover_reporters(api_key: str) -> list[dict]:
    """
    Calls GET /reporters from the base URL.
    Returns full list: [{ "code": "840", "iso3": "USA", "name": "United States", ... }]
    """
    logger.info(f"Discovering all reporters from WTO API …  [{WTO_BASE_URL}/reporters]")
    raw = wto_get("/reporters", {"lang": 1}, api_key)
    reporters = raw if isinstance(raw, list) else raw.get("reporters", [])
    logger.info(f"✓ Discovered {len(reporters)} reporters from WTO base URL")
    return reporters


# =====================================================
# STEP 3 — DISCOVER ALL FREQUENCIES FROM BASE URL
# =====================================================

def discover_frequencies(api_key: str) -> list[dict]:
    """
    Calls GET /frequencies from the base URL.
    Returns: [{ "code": "A", "name": "Annual" }, { "code": "Q", ... }, ...]
    """
    logger.info("Discovering frequencies …")
    raw   = wto_get("/frequencies", {"lang": 1}, api_key)
    freqs = raw if isinstance(raw, list) else []
    logger.info(f"✓ Frequencies: {[f.get('code') for f in freqs]}")
    return freqs


# =====================================================
# STEP 4 — SAVE ALL REFERENCE METADATA TO S3
# =====================================================

def save_all_metadata(api_key: str) -> dict:
    """
    Calls every reference endpoint from the base URL and stores
    results to S3. Returns a dict of all metadata for local use.
    """
    logger.info("=" * 70)
    logger.info("STEP 4 – Saving all reference metadata to S3")
    logger.info("=" * 70)

    endpoints = {
        "indicators":          ("/indicators",          {"lang": 1}),
        "reporters":           ("/reporters",           {"lang": 1}),
        "partners":            ("/partners",            {"lang": 1}),
        "topics":              ("/topics",              {"lang": 1}),
        "frequencies":         ("/frequencies",         {"lang": 1}),
        "periods_annual":      ("/periods",             {"freq": "A", "lang": 1}),
        "periods_quarterly":   ("/periods",             {"freq": "Q", "lang": 1}),
        "periods_monthly":     ("/periods",             {"freq": "M", "lang": 1}),
        "units":               ("/units",               {"lang": 1}),
        "indicator_categories":("/indicator_categories",{"lang": 1}),
    }

    metadata = {}
    for name, (path, params) in endpoints.items():
        try:
            logger.info(f"  → GET {WTO_BASE_URL}{path}  (params: {params})")
            data  = wto_get(path, params, api_key)
            rows  = data if isinstance(data, list) else [data]
            df    = pd.DataFrame(rows)
            s3key = f"{S3_PREFIX}/metadata/{name}.csv"
            upload_csv(df.to_csv(index=False), s3key)
            metadata[name] = data
        except Exception as e:
            logger.error(f"  ❌ {name} failed: {e}")
        time.sleep(REQUEST_DELAY)

    logger.info(f"✓ All metadata stored under s3://{S3_BUCKET}/{S3_PREFIX}/metadata/")
    return metadata


# =====================================================
# STEP 5 — DOWNLOAD ONE INDICATOR × ONE REPORTER
# =====================================================

def fetch_and_upload_data(indicator_code: str, indicator_name: str,
                           reporter_iso3: str, reporter_name: str,
                           freq: str, year_start: int, year_end: int,
                           api_key: str,
                           stats: StatsTracker) -> bool:
    """
    GET /data for one (indicator × reporter) combination.
    Handles multi-page responses automatically.
    Uploads CSV only to S3.
    Returns True on success.
    """
    all_rows = []
    page = 1

    while True:
        params = {
            "i":    indicator_code,
            "r":    reporter_iso3,
            "ps":   f"{year_start}-{year_end}",
            "freq": freq,
            "fmt":  "json",
            "max":  MAX_ROWS,
            "page": page,
            "lang": 1,
        }

        try:
            payload = wto_get("/data", params, api_key)
        except Exception as e:
            err_msg = str(e)
            logger.error(f"    ❌ Page {page} error: {err_msg}")
            stats.record_failure(indicator_code, reporter_iso3, err_msg)
            return False

        dataset = payload.get("Dataset", [])
        if not dataset:
            break

        all_rows.extend(dataset)
        logger.debug(f"    Page {page} → {len(dataset):,} rows  (cumulative: {len(all_rows):,})")

        if len(dataset) < MAX_ROWS:
            break     # no more pages
        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        logger.warning(f"    ⚠  No data returned for {indicator_code} × {reporter_iso3}")
        stats.record_skip(indicator_code, reporter_iso3)
        return False

    try:
        ind_clean = clean_name(indicator_code)
        rep_clean = clean_name(reporter_iso3)
        csv_key   = (f"{S3_PREFIX}/data/{ind_clean}"
                     f"/dt={RUN_DATE}/{ind_clean}_{rep_clean}_data.csv")

        df           = pd.DataFrame(all_rows)
        csv_str      = df.to_csv(index=False)
        bytes_up     = upload_csv(csv_str, csv_key)

        stats.record_success(indicator_code, reporter_iso3, len(df), bytes_up)
        logger.info(f"    ✅ {indicator_code} × {reporter_iso3}  ({len(df):,} rows)\n")
        return True

    except Exception as e:
        err_msg = str(e)
        logger.error(f"    ❌ Upload error: {err_msg}")
        stats.record_failure(indicator_code, reporter_iso3, err_msg)
        return False


# =====================================================
# EXTRACT FREQUENCY CODES FOR AN INDICATOR
# =====================================================

def get_indicator_freqs(indicator: dict, requested_freq: str) -> list[str]:
    raw_freqs = (
        indicator.get("frequencies")
        or indicator.get("frequency")
        or []
    )

    if isinstance(raw_freqs, list) and raw_freqs:
        codes = []
        for f in raw_freqs:
            code = f.get("code") if isinstance(f, dict) else str(f)
            if code:
                codes.append(code)
        return codes if codes else [requested_freq]

    return [requested_freq]


# =====================================================
# MAIN PIPELINE
# =====================================================

def run_pipeline(api_key: str, reporters_arg: str,
                 requested_freq: str, year_start: int, year_end: int,
                 topic_filter: str = None, dry_run: bool = False) -> None:

    stats = StatsTracker(STATS_FILE)
    stats.set_config(
        api_key_masked   = f"{api_key[:4]}****{api_key[-4:]}",
        reporters_arg    = reporters_arg,
        requested_freq   = requested_freq,
        year_start       = year_start,
        year_end         = year_end,
        topic_filter     = topic_filter,
        dry_run          = dry_run,
        s3_bucket        = S3_BUCKET,
        s3_prefix        = S3_PREFIX,
    )

    logger.info("=" * 70)
    logger.info("WTO DYNAMIC SCRAPER  ─  FULL PIPELINE")
    logger.info(f"Base URL : {WTO_BASE_URL}")
    logger.info("=" * 70)

    # ── Step 1: Discover indicators ───────────────────
    all_indicators = discover_indicators(api_key)

    if topic_filter:
        before = len(all_indicators)
        all_indicators = [
            i for i in all_indicators
            if topic_filter.lower() in (
                i.get("topic", {}).get("name", "")
                or i.get("topicName", "")
                or ""
            ).lower()
        ]
        logger.info(f"Topic filter '{topic_filter}': {before} → {len(all_indicators)} indicators")

    # ── Step 2: Discover reporters ────────────────────
    all_reporters_raw = discover_reporters(api_key)

    if reporters_arg.lower() == "all":
        reporters = [
            (r.get("iso3") or r.get("code", ""), r.get("name", ""))
            for r in all_reporters_raw
        ]
    else:
        codes    = [c.strip().upper() for c in reporters_arg.split(",") if c.strip()]
        name_map = {
            (r.get("iso3") or r.get("code", "")).upper(): r.get("name", "")
            for r in all_reporters_raw
        }
        reporters = [(c, name_map.get(c, c)) for c in codes]

    stats.set_discovery(len(all_indicators), len(reporters))

    # ── Step 3: Save metadata ─────────────────────────
    if not dry_run:
        save_all_metadata(api_key)

    # ── Summary ──────────────────────────────────────
    total_tasks = len(all_indicators) * len(reporters)

    logger.info("=" * 70)
    logger.info(f"  Indicators discovered : {len(all_indicators)}")
    logger.info(f"  Reporters resolved    : {len(reporters)}")
    logger.info(f"  Year range            : {year_start}–{year_end}")
    logger.info(f"  Frequency (fallback)  : {requested_freq}")
    logger.info(f"  Total tasks           : {total_tasks:,}")
    logger.info(f"  S3 destination        : s3://{S3_BUCKET}/{S3_PREFIX}/data/")
    logger.info(f"  Log file              : {LOG_FILE.resolve()}")
    logger.info(f"  Stats file            : {STATS_FILE.resolve()}")
    if dry_run:
        logger.info("  ⚠️  DRY RUN MODE  — No data will be downloaded or uploaded")
    logger.info("=" * 70)

    if dry_run:
        logger.info("DRY RUN — Indicators that would be scraped:")
        for ind in all_indicators:
            code  = ind.get("code", "")
            name  = ind.get("name", "")
            freqs = get_indicator_freqs(ind, requested_freq)
            logger.info(f"  {code:<20} {','.join(freqs):<12} {name[:45]}")
        logger.info(f"Total: {len(all_indicators)} indicators × {len(reporters)} reporters"
                    f" = {total_tasks:,} tasks")
        logger.info("Remove --dry-run to start actual download.")
        stats.finalize()
        stats.print_summary()
        return

    # ── Step 4: Main download loop ────────────────────
    task = 0

    for ind in all_indicators:
        ind_code  = ind.get("code", "")
        ind_name  = ind.get("name", ind_code)
        ind_freqs = get_indicator_freqs(ind, requested_freq)

        logger.info(f"{'=' * 70}")
        logger.info(f"INDICATOR [{ind_code}]  ─  {ind_name}")
        logger.info(f"Frequencies: {ind_freqs}")

        freq_to_use = (
            requested_freq if requested_freq in ind_freqs
            else ind_freqs[0]
        )

        for iso3, rep_name in reporters:
            task += 1
            clean_rep = clean_name(rep_name or iso3)

            logger.info(f"[{task}/{total_tasks}]  "
                        f"{ind_code} [{freq_to_use}] × {iso3:6s}  "
                        f"({rep_name[:35]})  → {clean_rep}")

            fetch_and_upload_data(
                ind_code, ind_name,
                iso3, rep_name,
                freq_to_use, year_start, year_end,
                api_key,
                stats,
            )

            time.sleep(REQUEST_DELAY)

            # Progress report every 100 tasks
            if task % 100 == 0:
                p       = stats.data["progress"]
                elapsed = (datetime.now() -
                           datetime.fromisoformat(stats.data["started_at"].replace("Z", "+00:00"))
                           ).total_seconds()
                rate    = task / elapsed if elapsed > 0 else 0
                pct     = task / total_tasks * 100
                logger.info(f"PROGRESS  {task:,}/{total_tasks:,}  ({pct:.1f}%)  |  "
                            f"✅ {p['tasks_succeeded']}  ❌ {p['tasks_failed']}  |  "
                            f"{rate:.2f} tasks/sec  |  "
                            f"ETA: {((total_tasks - task) / rate / 60):.0f} min")

    # ── Finalize ──────────────────────────────────────
    stats.finalize()
    stats.print_summary()

    logger.info(f"\n  📦 Data at   : s3://{S3_BUCKET}/{S3_PREFIX}/data/")
    logger.info(f"  📄 Log file  : {LOG_FILE.resolve()}")
    logger.info(f"  📊 Stats file: {STATS_FILE.resolve()}")


# =====================================================
# LIST ALL  (--list-all flag)
# =====================================================

def list_all(api_key: str) -> None:
    indicators = discover_indicators(api_key)
    reporters  = discover_reporters(api_key)
    freqs      = discover_frequencies(api_key)

    logger.info("═" * 70)
    logger.info(f"WTO API BASE: {WTO_BASE_URL}")
    logger.info("═" * 70)

    logger.info(f"FREQUENCIES  ({len(freqs)} total)")
    for f in freqs:
        logger.info(f"  {f.get('code', ''):<5}  {f.get('name', '')}")

    logger.info(f"INDICATORS  ({len(indicators)} total)")
    for ind in indicators:
        code      = ind.get("code", "")
        name      = ind.get("name", "")
        freqs_raw = ind.get("frequencies") or ind.get("frequency") or []
        freq_codes = [
            (f.get("code") if isinstance(f, dict) else str(f))
            for f in freqs_raw
        ] if isinstance(freqs_raw, list) else []
        logger.info(f"  {code:<22} {','.join(freq_codes):<12} {name[:40]}")

    logger.info(f"REPORTERS  ({len(reporters)} total)")
    for r in reporters:
        iso3 = r.get("iso3", "")
        code = str(r.get("code", ""))
        name = r.get("name", "")
        logger.info(f"  {iso3:<8} {code:<10} {name}")

    logger.info(f"Total: {len(indicators)} indicators × {len(reporters)} reporters"
                f" = {len(indicators) * len(reporters):,} data tasks if scraped fully")


# =====================================================
# ARGUMENT PARSER
# =====================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="wto_dynamic_scraper.py",
        description="WTO Complete Dynamic Scraper → MinIO S3  (auto-discovers all datasets from base URL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  python wto_dynamic_scraper.py --api-key KEY --list-all
  python wto_dynamic_scraper.py --api-key KEY --dry-run
  python wto_dynamic_scraper.py --api-key KEY
  python wto_dynamic_scraper.py --api-key KEY --reporters USA,CHN,IND,DEU,BRA,GBR
  python wto_dynamic_scraper.py --api-key KEY --topic "Merchandise trade"
  python wto_dynamic_scraper.py --api-key KEY --year-start 2015 --year-end 2024
  python wto_dynamic_scraper.py --api-key KEY --freq A
  python wto_dynamic_scraper.py --api-key KEY --cron
""",
    )

    p.add_argument("--api-key", default=WTO_API_KEY, metavar="KEY",
                   help="WTO API key from https://apiportal.wto.org/")

    g = p.add_argument_group("DISCOVERY")
    g.add_argument("--list-all", action="store_true",
                   help="Print ALL indicators + reporters from base URL and exit")
    g.add_argument("--dry-run", action="store_true",
                   help="Show what would be scraped without downloading anything")

    g2 = p.add_argument_group("FILTERS")
    g2.add_argument("--reporters", default=DEFAULT_REPORTERS, metavar="ISO3,...",
                    help=f'Comma-separated ISO3 codes, or "all" (default: {DEFAULT_REPORTERS})')
    g2.add_argument("--topic", metavar="NAME",
                    help='Filter indicators by topic name (e.g. "Merchandise trade")')
    g2.add_argument("--freq", default=None, metavar="A|Q|M",
                    help="Force frequency: A=Annual, Q=Quarterly, M=Monthly "
                         "(default: use each indicator's own frequency)")
    g2.add_argument("--year-start", type=int, default=DEFAULT_YEAR_START,
                    help=f"Start year (default: {DEFAULT_YEAR_START})")
    g2.add_argument("--year-end", type=int, default=DEFAULT_YEAR_END,
                    help=f"End year (default: {DEFAULT_YEAR_END})")

    g3 = p.add_argument_group("SCHEDULING")
    g3.add_argument("--cron", action="store_true",
                    help=f"Keep process alive; run daily at --cron-time")
    g3.add_argument("--cron-time", default=DEFAULT_CRON_TIME, metavar="HH:MM",
                    help=f"Cron time (default: {DEFAULT_CRON_TIME})")

    return p


# =====================================================
# ENTRY POINT
# =====================================================

def main():
    parser = build_parser()
    args   = parser.parse_args()

    logger.info("=" * 70)
    logger.info("WTO DYNAMIC SCRAPER  ─  AUTO-DISCOVERS ALL DATASETS FROM BASE URL")
    logger.info(f"  {WTO_BASE_URL}")
    logger.info("=" * 70)

    if args.list_all:
        list_all(args.api_key)
        return

    freq = args.freq or "A"

    def job():
        run_pipeline(
            api_key        = args.api_key,
            reporters_arg  = args.reporters,
            requested_freq = freq,
            year_start     = args.year_start,
            year_end       = args.year_end,
            topic_filter   = args.topic,
            dry_run        = args.dry_run,
        )

    if args.cron:
        logger.info(f"⏰  Cron mode: daily at {args.cron_time}")
        logger.info("   Running immediate first pass …")
        job()
        schedule.every().day.at(args.cron_time).do(job)
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        try:
            job()
        except KeyboardInterrupt:
            logger.warning("Interrupted by user")
        except Exception as e:
            logger.exception(f"FATAL ERROR: {e}")


if __name__ == "__main__":
    main()
