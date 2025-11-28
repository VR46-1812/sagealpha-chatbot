import os
import random
from datetime import datetime
import pandas as pd
import requests
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings
from requests.adapters import HTTPAdapter, Retry
from io import BytesIO

# -------------------------------------
# Load environment variables
# -------------------------------------
load_dotenv()
AZURE_CONN_STR = os.getenv("AZURE_CONN_STR")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "nse-data-raw")

# CSV columns
URL_COLUMN = "attchmntFile"
SYMBOL_COLUMN = "symbol"
DATE_COLUMN = "exchdisstime"

# Random user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605 Safari/605.1.15",
]


# ----------------------------------------------------
# 1. Get latest CSV directly from Azure Blob Storage
# ----------------------------------------------------
def get_latest_csv_from_blob():
    service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container = service.get_container_client(CONTAINER_NAME)

    all_blobs = list(container.list_blobs(name_starts_with="metadata/"))

    if not all_blobs:
        print("❌ No CSV files found in metadata/ folder in Azure Blob.")
        return None, None

    # Sort by last modified
    all_blobs.sort(key=lambda b: b.last_modified, reverse=True)

    latest_blob = all_blobs[0]
    blob_name = latest_blob.name

    print(f"📌 Using latest CSV from Azure → {blob_name}")

    csv_bytes = container.download_blob(blob_name).readall()

    return blob_name, csv_bytes


# ----------------------------------------------------
# 2. Azure client helper
# ----------------------------------------------------
def get_blob_service():
    return BlobServiceClient.from_connection_string(AZURE_CONN_STR)


# ----------------------------------------------------
# 3. Requests session with retry
# ----------------------------------------------------
def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def is_pdf(data: bytes):
    return data.startswith(b"%PDF")


# ----------------------------------------------------
# 4. Date parsing + Quarter calculation
# ----------------------------------------------------
def parse_dt(s):
    try:
        return datetime.strptime(s, "%d-%b-%Y %H:%M:%S")
    except:
        return datetime.now()


def get_quarter(dt):
    m = dt.month
    if 4 <= m <= 6:
        return "Q1"
    if 7 <= m <= 9:
        return "Q2"
    if 10 <= m <= 12:
        return "Q3"
    return "Q4"


def build_blob_path(symbol, dt, filename):
    q = get_quarter(dt)
    year = dt.year
    return f"documents/{symbol}/{year}/{q}/{filename}"


# ----------------------------------------------------
# 5. PDF Downloader
# ----------------------------------------------------
def download_pdf(url):
    session = create_session()
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.nseindia.com/",
    }

    r = session.get(url, headers=headers, timeout=15, stream=True)

    if r.status_code != 200:
        print(f"❌ HTTP Error {r.status_code} → {url}")
        return None

    data = b"".join(chunk for chunk in r.iter_content(4096) if chunk)

    if not is_pdf(data):
        print("❌ Not a PDF file (NSE returned HTML/XML)")
        return None

    return data


# ----------------------------------------------------
# 6. Process CSV → Download + Upload PDFs
# ----------------------------------------------------
def process_csv_from_azure(csv_bytes):
    df = pd.read_csv(BytesIO(csv_bytes), dtype=str, keep_default_na=False)

    print(f"📄 CSV contains {len(df)} rows")

    blob_service = get_blob_service()
    container_client = blob_service.get_container_client(CONTAINER_NAME)

    if not container_client.exists():
        container_client.create_container()

    for _, row in df.iterrows():

        url = (row.get(URL_COLUMN) or "").strip()
        symbol = row.get(SYMBOL_COLUMN, "UNKNOWN")
        dt = parse_dt(row.get(DATE_COLUMN, ""))

        if not url:
            continue

        filename = url.split("/")[-1]
        blob_path = build_blob_path(symbol, dt, filename)
        blob_client = container_client.get_blob_client(blob_path)

        if blob_client.exists():
            print(f"⏭ SKIP (Already exists): {blob_path}")
            continue

        print(f"⬇ Downloading PDF: {filename}")

        pdf_bytes = download_pdf(url)
        if not pdf_bytes:
            print(f"❌ Download failed → {url}")
            continue

        blob_client.upload_blob(
            pdf_bytes,
            overwrite=True,
            content_settings=ContentSettings(content_type="application/pdf")
        )

        print(f"✔ Uploaded → {blob_path}")


# ----------------------------------------------------
# 7. Main function
# ----------------------------------------------------
def main():
    print("-------------------------------------------------------")
    print("🔎 Fetching latest CSV from Azure Blob...")
    print("-------------------------------------------------------")

    blob_name, csv_bytes = get_latest_csv_from_blob()
    if not blob_name:
        return

    print(f"📌 Processing: {blob_name}")
    process_csv_from_azure(csv_bytes)

    print("-------------------------------------------------------")
    print("🎉 PDF Download + Azure Upload COMPLETE")
    print("-------------------------------------------------------")


if __name__ == "__main__":
    main()
