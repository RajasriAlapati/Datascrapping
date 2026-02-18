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

import os
import logging
import re
import time
import argparse
import schedule
from datetime import datetime, timezone

import requests
import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.local")

# =====================================================
# LOGGING SETUP
# =====================================================
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "wto_scraper.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("WTO-Scraper")

# =====================================================
# CONFIGURATION
# =====================================================

# ──── WTO API ────────────────────────────────────────
WTO_API_KEY  = os.getenv("WTO_API_KEY", "f65870b910744ac2875b62998a59dbf3")
WTO_BASE_URL = "https://api.wto.org/timeseries/v1"

# ──── S3 / MinIO ─────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "holacracydata")
S3_PREFIX             = os.getenv("S3_PREFIX", "wto/indicators")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT", "https://s3.us-east-005.oriobjects.cloud")


# ──── Scraping defaults ──────────────────────────────
DEFAULT_REPORTERS  = "all"        # "all" or "USA,CHN,IND,..."
DEFAULT_YEAR_START = 2000
DEFAULT_YEAR_END   = datetime.now(timezone.utc).year
DEFAULT_CRON_TIME  = "02:00"
MAX_ROWS           = 500_000      # WTO hard limit = 1,000,000
REQUEST_DELAY      = 0.5          # seconds between calls

RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
# S3 UTILS
# =====================================================

def s3_file_exists(bucket: str, key: str) -> bool:
    """Check if a file exists in S3."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            logger.error(f"Error checking S3 file {key}: {e}")
            raise


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

def wto_get(path: str, params: dict, api_key: str, retries: int = 3) -> dict:
    """
    GET any WTO Timeseries endpoint.
    Auto-retries on HTTP 429 (rate limit) and 5xx errors.
    Raises RuntimeError on any other failure.
    """
    p = dict(params)
    p["subscription-key"] = api_key

    url  = f"{WTO_BASE_URL}{path}"
    
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=p, timeout=120)
            
            if resp.status_code == 200:
                return resp.json()
            
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                logger.warning(f"  ⚠  Rate-limited (429) at {path}. Sleeping {wait}s … (Attempt {attempt+1}/{retries+1})")
                time.sleep(wait)
                continue

            if resp.status_code in [500, 502, 503, 504]:
                wait = 2 ** attempt * 5  # Exponential backoff
                logger.warning(f"  ⚠  Server error ({resp.status_code}) at {path}. Retrying in {wait}s … (Attempt {attempt+1}/{retries+1})")
                time.sleep(wait)
                continue

            logger.error(f"HTTP {resp.status_code} [{path}]: {resp.text[:300]}")
            raise RuntimeError(f"HTTP {resp.status_code} [{path}]: {resp.text[:300]}")

        except requests.exceptions.RequestException as e:
            if attempt < retries:
                wait = 2 ** attempt * 5
                logger.warning(f"  ⚠  Connection error at {path}: {e}. Retrying in {wait}s … (Attempt {attempt+1}/{retries+1})")
                time.sleep(wait)
                continue
            logger.error(f"Request failed: {url} - {e}")
            raise RuntimeError(f"Connection error: {e}")

    raise RuntimeError(f"Max retries exceeded for {url}")


# =====================================================
# S3 UPLOAD HELPER  ─  CSV ONLY
# =====================================================

def upload_csv(csv_str: str, key: str) -> None:
    body = csv_str.encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="text/csv")
    logger.info(f"  ✓ Uploaded → s3://{S3_BUCKET}/{key}  ({len(body):,} bytes)")


# =====================================================
# STEP 1 — DISCOVER ALL INDICATORS FROM BASE URL
# =====================================================

def discover_indicators(api_key: str) -> list[dict]:
    """
    Calls GET /indicators from the base URL.
    Returns full list of all indicator dicts:
      { "code": "ITS_MTV_AX", "name": "...", "frequencies": ["A"], ... }
    """
    logger.info("📡 Discovering all indicators from WTO API …")
    logger.debug(f"   URL: {WTO_BASE_URL}/indicators")

    raw = wto_get("/indicators", {"lang": 1}, api_key)

    # API returns a list directly
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
    logger.info("📡 Discovering all reporters from WTO API …")
    logger.debug(f"   URL: {WTO_BASE_URL}/reporters")

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
    logger.info("📡 Discovering frequencies …")
    raw  = wto_get("/frequencies", {"lang": 1}, api_key)
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
    logger.info("📋  STEP 4 – Saving all reference metadata to S3")

    # Every reference endpoint available under the base URL
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

            # Normalise to a flat list so we can write CSV
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
                           api_key: str) -> bool:
    """
    GET /data for one (indicator × reporter) combination.
    Handles multi-page responses automatically.
    Uploads CSV only to S3.
    Returns True on success.
    """
    ind_clean = clean_name(indicator_code)
    rep_clean = clean_name(reporter_iso3)
    csv_key   = (f"{S3_PREFIX}/data/{ind_clean}"
                 f"/dt={RUN_DATE}/{ind_clean}_{rep_clean}_data.csv")

    # Duplicate check
    if s3_file_exists(S3_BUCKET, csv_key):
        logger.info(f"    ⏭️  Skipping (already exists): {csv_key}")
        return True

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
            logger.error(f"    ❌ Page {page} error: {e}")
            break

        dataset = payload.get("Dataset", [])
        if not dataset:
            break

        all_rows.extend(dataset)
        logger.info(f"    Page {page} → {len(dataset):,} rows  (cumulative: {len(all_rows):,})")

        if len(dataset) < MAX_ROWS:
            break     # no more pages
        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        logger.warning(f"    ⚠  No data returned")
        return False

    try:
        # Upload CSV only
        df = pd.DataFrame(all_rows)
        upload_csv(df.to_csv(index=False), csv_key)

        logger.info(f"    ✅ {indicator_code} × {reporter_iso3}  ({len(df):,} rows)")
        return True

    except Exception as e:
        logger.error(f"    ❌ Upload error: {e}")
        return False


# =====================================================
# EXTRACT FREQUENCY CODES FOR AN INDICATOR
# =====================================================

def get_indicator_freqs(indicator: dict, requested_freq: str) -> list[str]:
    """
    Determine which frequency codes to use for an indicator.
    WTO indicators carry a 'frequencies' or 'frequency' field.
    Falls back to the user-requested frequency.
    """
    raw_freqs = (
        indicator.get("frequencies")
        or indicator.get("frequency")
        or []
    )

    if isinstance(raw_freqs, list) and raw_freqs:
        # Each element may be a dict {"code": "A"} or a plain string
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
    """
    Full dynamic pipeline:
      1. Discover ALL indicators from base URL
      2. Discover ALL reporters from base URL
      3. Save all reference metadata to S3
      4. Loop every indicator × every reporter → download & upload
    """

    logger.info("WTO DYNAMIC SCRAPER  ─  FULL PIPELINE")
    logger.info(f"Base URL : {WTO_BASE_URL}")

    # ── Step 1: Discover indicators ───────────────────
    all_indicators = discover_indicators(api_key)

    # Optional: filter by topic name
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
        logger.info(f"  Topic filter '{topic_filter}': {before} → {len(all_indicators)} indicators")

    # ── Step 2: Discover reporters ────────────────────
    all_reporters_raw = discover_reporters(api_key)

    if reporters_arg.lower() == "all":
        reporters = [
            (r.get("iso3") or r.get("code", ""), r.get("name", ""))
            for r in all_reporters_raw
        ]
    else:
        codes     = [c.strip().upper() for c in reporters_arg.split(",") if c.strip()]
        # Build name map from discovered reporters
        name_map  = {
            (r.get("iso3") or r.get("code", "")).upper(): r.get("name", "")
            for r in all_reporters_raw
        }
        reporters = [(c, name_map.get(c, c)) for c in codes]

    # ── Step 3: Save metadata ─────────────────────────
    if not dry_run:
        save_all_metadata(api_key)

    # ── Summary ──────────────────────────────────────
    total_tasks = len(all_indicators) * len(reporters)

    logger.info(f"  Indicators discovered : {len(all_indicators)}")
    logger.info(f"  Reporters resolved    : {len(reporters)}")
    logger.info(f"  Year range            : {year_start}–{year_end}")
    logger.info(f"  Frequency (fallback)  : {requested_freq}")
    logger.info(f"  Total tasks           : {total_tasks:,}")
    logger.info(f"  S3 destination        : s3://{S3_BUCKET}/{S3_PREFIX}/data/")
    if dry_run:
        logger.info(f"  ⚠️  DRY RUN MODE  — No data will be downloaded or uploaded")

    if dry_run:
        logger.info("📋 DRY RUN — Indicators that would be scraped:\n")
        logger.info(f"  {'CODE':<20} {'FREQUENCIES':<15} NAME")
        logger.info("  " + "─" * 65)
        for ind in all_indicators:
            code  = ind.get("code", "")
            name  = ind.get("name", "")
            freqs = get_indicator_freqs(ind, requested_freq)
            logger.info(f"  {code:<20} {','.join(freqs):<15} {name[:45]}")
        logger.info(f"\n  Total: {len(all_indicators)} indicators × {len(reporters)} reporters"
                    f" = {total_tasks:,} tasks")
        logger.info("\n  Remove --dry-run to start actual download.")
        return

    # ── Step 4: Main download loop ────────────────────
    stats  = {"success": 0, "failed": 0, "skipped": 0, "start": datetime.now()}
    task   = 0

    for ind in all_indicators:
        ind_code  = ind.get("code", "")
        ind_name  = ind.get("name", ind_code)
        ind_freqs = get_indicator_freqs(ind, requested_freq)

        logger.info(f"📈  INDICATOR [{ind_code}]  ─  {ind_name}")
        logger.info(f"    Frequencies: {ind_freqs}")

        # Use the first matching frequency for this indicator
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

            ok = fetch_and_upload_data(
                ind_code, ind_name,
                iso3, rep_name,
                freq_to_use, year_start, year_end,
                api_key
            )

            if ok:
                stats["success"] += 1
            else:
                stats["failed"]  += 1

            time.sleep(REQUEST_DELAY)

            # Progress report every 100 tasks
            if task % 100 == 0:
                elapsed = (datetime.now() - stats["start"]).total_seconds()
                rate    = task / elapsed if elapsed > 0 else 0
                pct     = task / total_tasks * 100
                logger.info(f"  PROGRESS  {task:,}/{total_tasks:,}  ({pct:.1f}%)  |  "
                            f"✅ {stats['success']}  ❌ {stats['failed']}  |  "
                            f"{rate:.2f} tasks/sec  |  "
                            f"ETA: {((total_tasks - task) / rate / 60):.0f} min")

    elapsed = (datetime.now() - stats["start"]).total_seconds()

    logger.info("🎉  SCRAPING COMPLETE")
    logger.info(f"  Indicators   : {len(all_indicators)}")
    logger.info(f"  Reporters    : {len(reporters)}")
    logger.info(f"  Total tasks  : {total_tasks:,}")
    logger.info(f"  ✅ Success   : {stats['success']:,}")
    logger.info(f"  ❌ Failed    : {stats['failed']:,}")
    logger.info(f"  Duration     : {elapsed:.0f}s  ({elapsed/60:.1f} min)")
    logger.info(f"  📦 Data at  : s3://{S3_BUCKET}/{S3_PREFIX}/data/")

    # S3 layout preview
    logger.info(f"""
📁 S3 LAYOUT:

  s3://{S3_BUCKET}/{S3_PREFIX}/
  │
  ├── metadata/                     ← reference data (CSV)
  │   ├── indicators.csv            (all {len(all_indicators)} discovered indicators)
  │   ├── reporters.csv             (all {len(reporters)} reporters)
  │   ├── partners.csv
  │   ├── topics.csv
  │   ├── frequencies.csv
  │   ├── periods_annual.csv
  │   ├── periods_quarterly.csv
  │   ├── periods_monthly.csv
  │   ├── units.csv
  │   └── indicator_categories.csv
  │
  └── data/                         ← trade data (CSV only)
      └── <INDICATOR_CODE>/
          └── dt={RUN_DATE}/
              └── <INDICATOR_CODE>_<REPORTER>_data.csv
""")


# =====================================================
# LIST ALL  (--list-all flag)
# =====================================================

def list_all(api_key: str) -> None:
    """
    Print every indicator and reporter the WTO API currently exposes.
    No data is downloaded.
    """
    indicators = discover_indicators(api_key)
    reporters  = discover_reporters(api_key)
    freqs      = discover_frequencies(api_key)

    logger.info(f" WTO API BASE: {WTO_BASE_URL}")

    logger.info(f"  FREQUENCIES  ({len(freqs)} total)")
    for f in freqs:
        code = f.get("code", "")
        name = f.get("name", "")
        logger.info(f"  {code:<5}  {name}")

    logger.info(f"  INDICATORS  ({len(indicators)} total)")
    logger.info(f"  {'CODE':<22} {'FREQ':<12} NAME")
    for ind in indicators:
        code  = ind.get("code", "")
        name  = ind.get("name", "")
        freqs_raw = (
            ind.get("frequencies") or ind.get("frequency") or []
        )
        freq_codes = [
            (f.get("code") if isinstance(f, dict) else str(f))
            for f in freqs_raw
        ] if isinstance(freqs_raw, list) else []
        logger.info(f"  {code:<22} {','.join(freq_codes):<12} {name[:40]}")

    logger.info(f"  REPORTERS  ({len(reporters)} total)")
    logger.info(f"  {'ISO3':<8} {'NUMERIC':<10} NAME")
    for r in reporters:
        iso3 = r.get("iso3", "")
        code = str(r.get("code", ""))
        name = r.get("name", "")
        logger.info(f"  {iso3:<8} {code:<10} {name}")

    logger.info(f"  Total: {len(indicators)} indicators × {len(reporters)} reporters")
    logger.info(f"         = {len(indicators) * len(reporters):,} data tasks if scraped fully")
    logger.info("  Run without --list-all to start downloading.")


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
  # Discover everything available (no download)
  python wto_dynamic_scraper.py --api-key KEY --list-all

  # See what would be downloaded (no actual download)
  python wto_dynamic_scraper.py --api-key KEY --dry-run

  # Download ALL indicators × ALL reporters
  python wto_dynamic_scraper.py --api-key KEY

  # Download ALL indicators, specific reporters only
  python wto_dynamic_scraper.py --api-key KEY --reporters USA,CHN,IND,DEU,BRA,GBR

  # Filter by topic category
  python wto_dynamic_scraper.py --api-key KEY --topic "Merchandise trade"

  # Custom year range
  python wto_dynamic_scraper.py --api-key KEY --year-start 2015 --year-end 2024

  # Force a specific frequency for all indicators
  python wto_dynamic_scraper.py --api-key KEY --freq A

  # Daily cron (runs immediately, then every day at 02:00)
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

    logger.info("WTO DYNAMIC SCRAPER  ─  AUTO-DISCOVERS ALL DATASETS FROM BASE URL")
    logger.info(f"  {WTO_BASE_URL}")

    if args.list_all:
        list_all(args.api_key)
        return

    freq = args.freq or "A"   # fallback freq if indicator doesn't specify

    def job():
        run_pipeline(
            api_key       = args.api_key,
            reporters_arg = args.reporters,
            requested_freq= freq,
            year_start    = args.year_start,
            year_end      = args.year_end,
            topic_filter  = args.topic,
            dry_run       = args.dry_run,
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
            print("\n⚠️  Interrupted by user")
        except Exception as e:
            print(f"\n❌ ERROR: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()
