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
import argparse
import schedule
from datetime import datetime, timezone

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
print("✓ S3 client initialized")


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
        print(f"  ⚠  Rate-limited. Sleeping {wait}s …")
        time.sleep(wait)
        return wto_get(path, params, api_key)   # retry

    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} [{path}]: {resp.text[:300]}")

    return resp.json()


# =====================================================
# S3 UPLOAD HELPER  ─  CSV ONLY
# =====================================================

def upload_csv(csv_str: str, key: str) -> None:
    body = csv_str.encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="text/csv")
    print(f"  ✓ Uploaded → s3://{S3_BUCKET}/{key}  ({len(body):,} bytes)")


# =====================================================
# STEP 1 — DISCOVER ALL INDICATORS FROM BASE URL
# =====================================================

def discover_indicators(api_key: str) -> list[dict]:
    """
    Calls GET /indicators from the base URL.
    Returns full list of all indicator dicts:
      { "code": "ITS_MTV_AX", "name": "...", "frequencies": ["A"], ... }
    """
    print("\n📡 Discovering all indicators from WTO API …")
    print(f"   URL: {WTO_BASE_URL}/indicators")

    raw = wto_get("/indicators", {"lang": 1}, api_key)

    # API returns a list directly
    indicators = raw if isinstance(raw, list) else raw.get("indicators", [])

    print(f"✓ Discovered {len(indicators)} indicators from WTO base URL")
    return indicators


# =====================================================
# STEP 2 — DISCOVER ALL REPORTERS FROM BASE URL
# =====================================================

def discover_reporters(api_key: str) -> list[dict]:
    """
    Calls GET /reporters from the base URL.
    Returns full list: [{ "code": "840", "iso3": "USA", "name": "United States", ... }]
    """
    print("\n📡 Discovering all reporters from WTO API …")
    print(f"   URL: {WTO_BASE_URL}/reporters")

    raw = wto_get("/reporters", {"lang": 1}, api_key)

    reporters = raw if isinstance(raw, list) else raw.get("reporters", [])

    print(f"✓ Discovered {len(reporters)} reporters from WTO base URL")
    return reporters


# =====================================================
# STEP 3 — DISCOVER ALL FREQUENCIES FROM BASE URL
# =====================================================

def discover_frequencies(api_key: str) -> list[dict]:
    """
    Calls GET /frequencies from the base URL.
    Returns: [{ "code": "A", "name": "Annual" }, { "code": "Q", ... }, ...]
    """
    print("\n📡 Discovering frequencies …")
    raw  = wto_get("/frequencies", {"lang": 1}, api_key)
    freqs = raw if isinstance(raw, list) else []
    print(f"✓ Frequencies: {[f.get('code') for f in freqs]}")
    return freqs


# =====================================================
# STEP 4 — SAVE ALL REFERENCE METADATA TO S3
# =====================================================

def save_all_metadata(api_key: str) -> dict:
    """
    Calls every reference endpoint from the base URL and stores
    results to S3. Returns a dict of all metadata for local use.
    """
    print("\n" + "=" * 70)
    print("📋  STEP 4 – Saving all reference metadata to S3")
    print("=" * 70)

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
            print(f"  → GET {WTO_BASE_URL}{path}  (params: {params})")
            data  = wto_get(path, params, api_key)

            # Normalise to a flat list so we can write CSV
            rows  = data if isinstance(data, list) else [data]
            df    = pd.DataFrame(rows)
            s3key = f"{S3_PREFIX}/metadata/{name}.csv"
            upload_csv(df.to_csv(index=False), s3key)
            metadata[name] = data
        except Exception as e:
            print(f"  ❌ {name} failed: {e}")
        time.sleep(REQUEST_DELAY)

    print(f"\n✓ All metadata stored under s3://{S3_BUCKET}/{S3_PREFIX}/metadata/")
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
            print(f"    ❌ Page {page} error: {e}")
            break

        dataset = payload.get("Dataset", [])
        if not dataset:
            break

        all_rows.extend(dataset)
        print(f"    Page {page} → {len(dataset):,} rows  (cumulative: {len(all_rows):,})")

        if len(dataset) < MAX_ROWS:
            break     # no more pages
        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        print(f"    ⚠  No data returned")
        return False

    try:
        ind_clean = clean_name(indicator_code)
        rep_clean = clean_name(reporter_iso3)
        csv_key   = (f"{S3_PREFIX}/data/{ind_clean}"
                     f"/dt={RUN_DATE}/{ind_clean}_{rep_clean}_data.csv")

        # Upload CSV only
        df = pd.DataFrame(all_rows)
        upload_csv(df.to_csv(index=False), csv_key)

        print(f"    ✅ {indicator_code} × {reporter_iso3}  ({len(df):,} rows)\n")
        return True

    except Exception as e:
        print(f"    ❌ Upload error: {e}")
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

    print("\n" + "=" * 70)
    print("WTO DYNAMIC SCRAPER  ─  FULL PIPELINE")
    print(f"Base URL : {WTO_BASE_URL}")
    print("=" * 70)

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
        print(f"  Topic filter '{topic_filter}': {before} → {len(all_indicators)} indicators")

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

    print("\n" + "=" * 70)
    print(f"  Indicators discovered : {len(all_indicators)}")
    print(f"  Reporters resolved    : {len(reporters)}")
    print(f"  Year range            : {year_start}–{year_end}")
    print(f"  Frequency (fallback)  : {requested_freq}")
    print(f"  Total tasks           : {total_tasks:,}")
    print(f"  S3 destination        : s3://{S3_BUCKET}/{S3_PREFIX}/data/")
    if dry_run:
        print(f"\n  ⚠️  DRY RUN MODE  — No data will be downloaded or uploaded")
    print("=" * 70)

    if dry_run:
        print("\n📋 DRY RUN — Indicators that would be scraped:\n")
        print(f"  {'CODE':<20} {'FREQUENCIES':<15} NAME")
        print("  " + "─" * 65)
        for ind in all_indicators:
            code  = ind.get("code", "")
            name  = ind.get("name", "")
            freqs = get_indicator_freqs(ind, requested_freq)
            print(f"  {code:<20} {','.join(freqs):<15} {name[:45]}")
        print(f"\n  Total: {len(all_indicators)} indicators × {len(reporters)} reporters"
              f" = {total_tasks:,} tasks")
        print("\n  Remove --dry-run to start actual download.")
        return

    # ── Step 4: Main download loop ────────────────────
    stats  = {"success": 0, "failed": 0, "skipped": 0, "start": datetime.now()}
    task   = 0

    for ind in all_indicators:
        ind_code  = ind.get("code", "")
        ind_name  = ind.get("name", ind_code)
        ind_freqs = get_indicator_freqs(ind, requested_freq)

        print(f"\n{'=' * 70}")
        print(f"📈  INDICATOR [{ind_code}]  ─  {ind_name}")
        print(f"    Frequencies: {ind_freqs}")
        print("=" * 70)

        # Use the first matching frequency for this indicator
        freq_to_use = (
            requested_freq if requested_freq in ind_freqs
            else ind_freqs[0]
        )

        for iso3, rep_name in reporters:
            task += 1
            clean_rep = clean_name(rep_name or iso3)

            print(f"\n[{task}/{total_tasks}]  "
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
                print(f"\n{'─' * 70}")
                print(f"  PROGRESS  {task:,}/{total_tasks:,}  ({pct:.1f}%)  |  "
                      f"✅ {stats['success']}  ❌ {stats['failed']}  |  "
                      f"{rate:.2f} tasks/sec  |  "
                      f"ETA: {((total_tasks - task) / rate / 60):.0f} min")
                print(f"{'─' * 70}")

    # ── Final summary ──────────────────────────────────
    elapsed = (datetime.now() - stats["start"]).total_seconds()

    print("\n" + "=" * 70)
    print("🎉  SCRAPING COMPLETE")
    print("=" * 70)
    print(f"  Indicators   : {len(all_indicators)}")
    print(f"  Reporters    : {len(reporters)}")
    print(f"  Total tasks  : {total_tasks:,}")
    print(f"  ✅ Success   : {stats['success']:,}")
    print(f"  ❌ Failed    : {stats['failed']:,}")
    print(f"  Duration     : {elapsed:.0f}s  ({elapsed/60:.1f} min)")
    print(f"\n  📦 Data at  : s3://{S3_BUCKET}/{S3_PREFIX}/data/")
    print("=" * 70)

    # S3 layout preview
    print(f"""
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

    print("\n" + "═" * 70)
    print(f"  WTO API BASE: {WTO_BASE_URL}")
    print("═" * 70)

    print(f"\n{'─' * 70}")
    print(f"  FREQUENCIES  ({len(freqs)} total)")
    print(f"{'─' * 70}")
    for f in freqs:
        code = f.get("code", "")
        name = f.get("name", "")
        print(f"  {code:<5}  {name}")

    print(f"\n{'─' * 70}")
    print(f"  INDICATORS  ({len(indicators)} total)")
    print(f"{'─' * 70}")
    print(f"  {'CODE':<22} {'FREQ':<12} NAME")
    print(f"  {'─' * 65}")
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
        print(f"  {code:<22} {','.join(freq_codes):<12} {name[:40]}")

    print(f"\n{'─' * 70}")
    print(f"  REPORTERS  ({len(reporters)} total)")
    print(f"{'─' * 70}")
    print(f"  {'ISO3':<8} {'NUMERIC':<10} NAME")
    print(f"  {'─' * 55}")
    for r in reporters:
        iso3 = r.get("iso3", "")
        code = str(r.get("code", ""))
        name = r.get("name", "")
        print(f"  {iso3:<8} {code:<10} {name}")

    print(f"\n{'═' * 70}")
    print(f"  Total: {len(indicators)} indicators × {len(reporters)} reporters")
    print(f"         = {len(indicators) * len(reporters):,} data tasks if scraped fully")
    print(f"{'═' * 70}")
    print("\n  Run without --list-all to start downloading.\n")


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

    print("\n" + "=" * 70)
    print("WTO DYNAMIC SCRAPER  ─  AUTO-DISCOVERS ALL DATASETS FROM BASE URL")
    print(f"  {WTO_BASE_URL}")
    print("=" * 70)

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
        print(f"⏰  Cron mode: daily at {args.cron_time}")
        print("   Running immediate first pass …\n")
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