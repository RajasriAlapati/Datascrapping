import requests
import time
import zipfile
import io
from datetime import datetime

# -------- WTO API CONFIG --------
WTO_API_KEY  = "f65870b910744ac2875b62998a59dbf3"  # ← get free key at https://apiportal.wto.org/
WTO_BASE_URL = "https://api.wto.org/timeseries/v1"

# -------- SCRAPING CONFIG --------
INDICATOR_CODE = "ITS_MTV_AX"  # Example: Merchandise exports – total value (annual)
REPORTER_CODE = "USA"  # We will use the reporter code for USA
YEAR_START = 2018
YEAR_END = datetime.now().year
MAX_ROWS = 500_000   # Rows per API page (WTO hard limit = 1,000,000)
REQUEST_DELAY = 0.5  # Seconds between requests (WTO allows ~6 req/sec)
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

# =====================================================
# WTO API HELPERS
# =====================================================

def wto_get(path: str, params: dict, is_csv=False) -> requests.Response:
    """
    Perform a GET request against the WTO Timeseries API.
    Automatically retries once on HTTP 429 (rate limit).
    If `is_csv=True`, returns the raw CSV data.
    """
    params["subscription-key"] = WTO_API_KEY

    url = f"{WTO_BASE_URL}{path}"
    headers = {"Accept": "text/csv" if is_csv else "application/json"}
    response = requests.get(url, params=params, headers=headers, timeout=120)

    # Print status code and first part of response for debugging
    print(f"HTTP Status Code: {response.status_code}")
    print(f"Response Text (First 200 chars): {response.text[:200]}")

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"  ⚠  Rate-limited. Sleeping {retry_after}s …")
        time.sleep(retry_after)
        return wto_get(path, params, is_csv)  # Retry once

    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} on {path} → {response.text[:300]}")

    return response

def handle_compressed_data(response: requests.Response) -> str:
    """
    If the response is a compressed file (e.g., ZIP), extract the contents.
    Returns the CSV content as a string.
    """
    # Check if the response is compressed
    if 'application/zip' in response.headers.get('Content-Type', ''):
        print("  ⚠  ZIP file detected. Extracting …")
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        
        # Extract the first file in the zip
        zip_file_name = zip_file.namelist()[0]
        print(f"  Extracted file: {zip_file_name}")

        # Save the raw file as is (even if there are encoding issues)
        with zip_file.open(zip_file_name) as csvfile:
            raw_data = csvfile.read()

        return raw_data
    else:
        raise ValueError("  ⚠  Unexpected file format: Not a ZIP file.")

# =====================================================
# DOWNLOAD DATA FOR ONE INDICATOR × ONE REPORTER
# =====================================================

def download_indicator_reporter(indicator_code: str, reporter_code: str) -> None:
    """
    Download the data for the specified indicator and reporter,
    then save it as CSV.
    """
    page = 1
    all_data = []

    while True:
        params = {
            "i": indicator_code,
            "r": reporter_code,
            "ps": f"{YEAR_START}-{YEAR_END}",
            "freq": "A",  # Annual data; change to Q or M if needed for quarterly or monthly
            "fmt": "csv",  # Request data in CSV format
            "max": MAX_ROWS,
            "page": page,
            "lang": 1,  # English
        }

        try:
            response = wto_get("/data", params, is_csv=True)
        except Exception as exc:
            print(f"    ❌ Page {page} failed: {exc}")
            break

        if response.text.strip() == "":  # No data in the response
            print(f"    ⚠ No more data.")
            break

        # If the response is a compressed file (ZIP), handle it
        try:
            raw_data = handle_compressed_data(response)
            all_data.append(raw_data)
            print(f"    Page {page} → Data fetched.")
        except ValueError as ve:
            print(ve)
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    if not all_data:
        print(f"    ⚠  No data returned")
        return

    # Save the raw data to CSV files
    csv_file_name = f"{indicator_code}_{reporter_code}_data_{RUN_DATE}_raw.csv"

    with open(csv_file_name, 'wb') as f:
        for page_data in all_data:
            f.write(page_data)

    print(f"    ✅ SUCCESS  Data saved to {csv_file_name}")


# =====================================================
# ENTRY POINT
# =====================================================

def main():
    try:
        # Fetch available reporter codes and names
        print("=" * 70)
        print(f"📊 Fetching data for reporter {REPORTER_CODE}")
        print("=" * 70)

        download_indicator_reporter(INDICATOR_CODE, '840')  # Use the correct code for USA (840)

    except Exception as exc:
        print(f"❌ ERROR: {exc}")


if __name__ == "__main__":
    main()
