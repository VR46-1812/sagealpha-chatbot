import requests
import os
import time
from datetime import datetime
import urllib3
import json
from dotenv import load_dotenv

# Azure Blob Storage
from azure.storage.blob import BlobServiceClient, ContentSettings

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load .env file
load_dotenv()

class NSEScraper:
    def __init__(self):

        # ------------ LOAD ENV VARIABLES ------------
        self.azure_conn_str = os.getenv("AZURE_CONN_STR")
        self.container_name = os.getenv("AZURE_CONTAINER_NAME")
        self.cupid_filings_url = os.getenv("cupid_file_url")

        if not self.azure_conn_str:
            print("❌ Missing AZURE_CONN_STR in .env")
        if not self.container_name:
            print("❌ Missing AZURE_CONTAINER_NAME in .env")
        if not self.cupid_filings_url:
            print("⚠ WARNING: cupid_file_url missing in .env, using default")

        # ------------ NSE API ------------
        self.base_url = "https://www.nseindia.com"
        self.api_url = "https://www.nseindia.com/api/corporates-financial-results"

        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": self.cupid_filings_url or "https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
            "X-Requested-With": "XMLHttpRequest",
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # ------------ Local Folders ------------
        self.xbrl_folder = "Cupid_XBRL_Files"
        self.pdf_folder = "Cupid_PDF_Filings"
        os.makedirs(self.xbrl_folder, exist_ok=True)
        os.makedirs(self.pdf_folder, exist_ok=True)

        # ------------ Azure Blob Init ------------
        try:
            self.blob_service = BlobServiceClient.from_connection_string(self.azure_conn_str)
            print("✓ Azure Blob connected")
        except Exception as e:
            print(f"❌ Azure connection failed: {e}")

    # ------------------------------
    def initialize_session(self):
        try:
            print("Initializing session...")
            self.session.get(self.base_url, timeout=10)
            self.session.get(
                self.cupid_filings_url or "https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
                timeout=10
            )
            print("✓ Session initialized")
        except Exception as e:
            print(f"Error initializing session: {e}")

    # ------------------------------
    def fetch_financial_results(self, symbol):
        params = {"index": "equities", "symbol": symbol, "period": "Quarterly"}

        try:
            print(f"Fetching NSE data for {symbol}...")
            response = self.session.get(self.api_url, params=params, timeout=15)

            if response.status_code == 200:
                print("✓ Data received from NSE")
                return response.json()
            else:
                print(f"❌ API returned {response.status_code}")
                return None

        except Exception as e:
            print(f"Error fetching API: {e}")
            return None

    # ------------------------------
    def upload_to_blob(self, local_path, blob_path):
        try:
            blob_client = self.blob_service.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )

            content_type = "application/xml" if local_path.endswith(".xml") else "application/pdf"

            with open(local_path, "rb") as data:
                blob_client.upload_blob(
                    data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type=content_type)
                )

            print(f"✓ Uploaded to Azure: {blob_path}")

        except Exception as e:
            print(f"❌ Blob upload failed: {e}")

    # ------------------------------
    def download_and_upload(self, file_url, local_folder, local_file, azure_blob_path):
        if not file_url:
            print("⚠ No file URL found")
            return

        # Fix relative URLs
        if not file_url.startswith("http"):
            file_url = f"https://nsearchives.nseindia.com/corporate/{file_url}"

        local_path = os.path.join(local_folder, local_file)

        print(f"Downloading: {local_file}")
        response = self.session.get(file_url, timeout=20)

        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)

            print(f"✓ Saved locally: {local_path}")

            # Upload to Azure
            self.upload_to_blob(local_path, azure_blob_path)
        else:
            print(f"❌ Download failed ({response.status_code})")

    # ------------------------------
    def process_data(self, data):
        if not data:
            print("No data returned.")
            return

        items = data.get("data") if isinstance(data, dict) else data

        if not items:
            print("Empty results.")
            return

        print(f"Total records received: {len(items)}")

        # --- Removed cutoff filtering (download ALL filings) ---

        for item in items:

            date_str = item.get("reBroadcastDate") or item.get("broadcastDate") or item.get("fromDate")
            if not date_str:
                continue

            date_clean = date_str.split(" ")[0]

            parsed = None
            for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"]:
                try:
                    parsed = datetime.strptime(date_clean, fmt)
                    break
                except:
                    pass

            if not parsed:
                print(f"⚠ Could not parse date: {date_str}")
                continue

            print(f"\nRecord found: {parsed.strftime('%Y-%m-%d')}")

            # Determine quarter
            quarter = (parsed.month - 1) // 3 + 1
            blob_base = f"cupid_data/Quarter{quarter}/{parsed.strftime('%Y-%m-%d')}"

            # XBRL
            xbrl = item.get("xbrl") or item.get("xbrllink")
            if xbrl:
                local_name = f"{parsed.strftime('%Y-%m-%d')}_Cupid_XBRL.xml"
                blob_name = f"{blob_base}_Cupid_XBRL.xml"
                self.download_and_upload(xbrl, self.xbrl_folder, local_name, blob_name)

            # PDF
            pdf = item.get("attachableFile") or item.get("pdflink")
            if pdf:
                local_name = f"{parsed.strftime('%Y-%m-%d')}_Cupid_Results.pdf"
                blob_name = f"{blob_base}_Cupid_Results.pdf"
                self.download_and_upload(pdf, self.pdf_folder, local_name, blob_name)


# ----------------------------------------------------------
if __name__ == "__main__":
    scraper = NSEScraper()
    scraper.initialize_session()
    time.sleep(2)
    data = scraper.fetch_financial_results("CUPID")
    scraper.process_data(data)
