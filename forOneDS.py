import requests
import time
import zipfile
import io
from datetime import datetime

# WTO API configuration
WTO_API_KEY  = "f65870b910744ac2875b62998a59dbf3"  # Replace with your key
WTO_BASE_URL = "https://api.wto.org/timeseries/v1"

# Scraping configuration
INDICATOR_CODE = "TP_A_0010"  # Example indicator code for merchandise exports total value
REPORTER_CODE = "000"  # Reporter code for USA
YEAR_START = 2018
YEAR_END = datetime.now().year
MAX_ROWS = 500_000  # Max rows per API page (WTO allows up to 1 million)
REQUEST_DELAY = 0.5  # Seconds between requests

# Helper function to make the GET request
def wto_get(path: str, params: dict, is_csv=False) -> requests.Response:
    """ Perform a GET request to the WTO API """
    params["subscription-key"] = WTO_API_KEY
    url = f"{WTO_BASE_URL}{path}"
    headers = {"Accept": "text/csv" if is_csv else "application/json"}
    response = requests.get(url, params=params, headers=headers, timeout=120)
    
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"  ⚠  Rate-limited. Sleeping {retry_after}s …")
        time.sleep(retry_after)
        return wto_get(path, params, is_csv)  # Retry once

    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} on {path} → {response.text[:300]}")
    
    return response

# Handle ZIP file and extract data
def handle_compressed_data(response: requests.Response) -> str:
    """ Handle ZIP file and extract CSV data """
    if 'application/zip' in response.headers.get('Content-Type', ''):
        print("  ⚠  ZIP file detected. Extracting …")
        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
        zip_file_name = zip_file.namelist()[0]
        print(f"  Extracted file: {zip_file_name}")

        with zip_file.open(zip_file_name) as csvfile:
            raw_data = csvfile.read()
        
        return raw_data
    else:
        raise ValueError("  ⚠  Unexpected file format: Not a ZIP file.")

# Function to fetch data for one indicator and reporter
def download_data_for_indicator(indicator_code: str, reporter_code: str) -> None:
    """ Download data for one specific indicator and reporter and save it to a CSV file """
    params = {
        "i": indicator_code,
        "r": reporter_code,
        "ps": f"{YEAR_START}-{YEAR_END}",
        "freq": "A",  # "A" for annual data
        "fmt": "csv",  # Request data in CSV format
        "max": MAX_ROWS,
        "page": 1,  # Fetching the first page
        "lang": 1,  # Language: English
    }

    try:
        response = wto_get("/data", params, is_csv=True)
    except Exception as exc:
        print(f"    ❌ Error fetching data: {exc}")
        return

    if response.text.strip() == "":  # If no data is returned
        print(f"    ⚠ No data found for {indicator_code} and {reporter_code}.")
        return

    try:
        # Handle ZIP file and extract the CSV content
        raw_data = handle_compressed_data(response)
        
        # Save the extracted raw data to a CSV file
        csv_file_name = f"{indicator_code}_{reporter_code}_data_{YEAR_START}_{YEAR_END}_raw.csv"
        with open(csv_file_name, 'wb') as f:
            f.write(raw_data)

        print(f"    ✅ Data saved successfully to {csv_file_name}")

    except ValueError as ve:
        print(ve)
        print(f"    ⚠ Unable to process the data correctly.")

# Main function to download data for one indicator and reporter
def main():
    print("=" * 70)
    print(f"📊 Fetching data for {REPORTER_CODE} (USA) on Indicator {INDICATOR_CODE}")
    print("=" * 70)
    
    download_data_for_indicator(INDICATOR_CODE, "840")  # "840" is the code for the USA in WTO

if __name__ == "__main__":
    main()
