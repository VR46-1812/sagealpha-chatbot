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
load_dotenv()
AZURE_CONN_STR = os.getenv("AZURE_CONN_STR")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "nse-data-raw")

URL_COLUMN = "attchmntFile"
SYMBOL_COLUMN = "symbol"
DATE_COLUMN = "exchdisstime"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605 Safari/605.1.15",
]


def get_latest_csv_from_blob():
    service = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    container = service.get_container_client(CONTAINER_NAME)

    all_blobs = list(container.list_blobs(name_starts_with="metadata/"))
    if not all_blobs:
        print("No CSV found.")
        return None, None

    all_blobs.sort(key=lambda b: b.last_modified, reverse=True)
    newest = all_blobs[0]

    print(f"Using latest CSV → {newest.name}")
    csv = container.download_blob(newest.name).readall()
    return newest.name, csv


def get_blob_service():
    return BlobServiceClient.from_connection_string(AZURE_CONN_STR)


def create_session():
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


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


def build_path(symbol, dt, filename):
    q = get_quarter(dt)
    return f"documents/{symbol}/{dt.year}/{q}/{filename}"


def download_pdf(url):
    session = create_session()
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://www.nseindia.com/"
    }

    try:
        r = session.get(url, headers=headers, timeout=(10, 20), stream=True)
    except Exception as e:
        print(f"Request failed: {e}")
        return None

    if r.status_code != 200:
        print(f"HTTP {r.status_code}: {url}")
        return None

    # Stream instead of full read
    data = bytearray()
    max_size = 60 * 1024 * 1024  # 60MB

    try:
        for chunk in r.iter_content(4096):
            if not chunk:
                break
            data.extend(chunk)

            if len(data) > max_size:
                print("File too large, skipping.")
                return None

    except Exception as e:
        print(f"Stream error: {e}")
        return None

    if not data[:4] == b"%PDF":
        print("Invalid PDF")
        return None

    return bytes(data)


def upload_large_pdf(blob_client, data):
    BLOCK_SIZE = 4 * 1024 * 1024  # 4MB
    blocks = []
    index = 0

    for i in range(0, len(data), BLOCK_SIZE):
        block_id = f"block-{index:05}".encode("utf-8")
        chunk = data[i:i + BLOCK_SIZE]

        blob_client.stage_block(block_id=block_id, data=chunk)
        blocks.append(dict(id=block_id))
        index += 1

    blob_client.commit_block_list(
        [b["id"] for b in blocks],
        content_settings=ContentSettings(content_type="application/pdf")
    )


def process(csv_bytes):
    df = pd.read_csv(BytesIO(csv_bytes), dtype=str, keep_default_na=False)
    print(f"CSV Rows: {len(df)}")

    service = get_blob_service()
    container = service.get_container_client(CONTAINER_NAME)

    if not container.exists():
        container.create_container()

    for _, row in df.iterrows():

        url = (row.get(URL_COLUMN) or "").strip()
        symbol = row.get(SYMBOL_COLUMN, "UNKNOWN")
        dt = parse_dt(row.get(DATE_COLUMN, ""))
        if not url:
            continue

        filename = url.split("/")[-1]
        blob_path = build_path(symbol, dt, filename)
        blob_client = container.get_blob_client(blob_path)

        if blob_client.exists():
            print(f"Skip (exists): {blob_path}")
            continue

        print(f"Downloading {filename} ...")

        pdf = download_pdf(url)
        if not pdf:
            print(f"Failed: {url}")
            continue

        # block upload
        try:
            upload_large_pdf(blob_client, pdf)
            print(f"Uploaded → {blob_path}")
        except Exception as e:
            print(f"Azure upload error: {e}")


def main():
    print("Fetching latest CSV...")
    name, csv_bytes = get_latest_csv_from_blob()
    if not name:
        return

    print(f"Processing CSV → {name}")
    process(csv_bytes)

    print("DONE")


if __name__ == "__main__":
    main()
