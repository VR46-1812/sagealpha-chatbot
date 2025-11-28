import os
import time
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContentSettings

# ============================================================
# ENV SETUP
# ============================================================
load_dotenv()

AZURE_CONN = os.getenv("AZURE_CONN_STR")
AZURE_CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "cupid-financials")

if not AZURE_CONN:
    raise ValueError("Missing Azure connection string! Set AZURE_CONN_STR in .env")

blob_service = BlobServiceClient.from_connection_string(AZURE_CONN)

if not blob_service.get_container_client(AZURE_CONTAINER).exists():
    blob_service.create_container(AZURE_CONTAINER)
    print(f"✔ Created Azure Container: {AZURE_CONTAINER}")


# ============================================================
# NSE Utility Class
# ============================================================
class NSEUtils:
    BASE_URL = "https://www.nseindia.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/plain, */*",
            "Connection": "keep-alive",
            "Referer": self.BASE_URL,
        })
        self.init_cookies()

    def init_cookies(self):
        """Touch a few NSE pages to get proper cookies."""
        seed_urls = [
            self.BASE_URL,
            f"{self.BASE_URL}/get-quaterly-results?symbol=CUPID",
            f"{self.BASE_URL}/get-financial-information?symbol=CUPID",
        ]
        for u in seed_urls:
            try:
                self.session.get(u, timeout=10)
            except Exception:
                # We don't hard-fail on cookie warmup
                pass

    # --------------------------------------------------------
    # Corporate Announcements (multi-year)
    # --------------------------------------------------------
    def get_announcements(self, symbol: str = "CUPID") -> pd.DataFrame | None:
        """
        Fetch corporate announcements for the given symbol over multiple years.
        """
        start = "01-01-2019"
        end = datetime.now().strftime("%d-%m-%Y")

        url = (
            "https://www.nseindia.com/api/corporate-announcements"
            f"?index=equities&symbol={symbol}&from_date={start}&to_date={end}"
        )

        print(f"\n📌 Fetching Corporate Announcements → {url}")

        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                print(f"⚠ Announcement HTTP Error: {resp.status_code}")
                return None

            try:
                data = resp.json()
            except ValueError:
                print("⚠ Announcement response not JSON (maybe blocked by NSE)")
                return None

            if isinstance(data, dict) and "data" in data:
                data = data["data"]
            elif isinstance(data, list):
                # Already a list of announcements
                pass
            else:
                print(f"⚠ Unknown announcement JSON format: {type(data)}")
                return None

            if not data:
                print("⚠ No announcements returned.")
                return None

            df = pd.DataFrame(data)
            print(f"✔ Found {len(df)} announcements for {symbol}")
            return df

        except Exception as e:
            print("❌ Announcement Error:", e)
            return None

    # --------------------------------------------------------
    # Quarterly Results
    # --------------------------------------------------------
    def get_quarterly(self, symbol: str = "CUPID") -> pd.DataFrame | None:
        """
        Fetch quarterly financial results from NSE.
        """
        self.init_cookies()  # ensure fresh cookies

        url = f"https://www.nseindia.com/api/corporates-financial-results?symbol={symbol}"
        print(f"\n📌 Fetching Quarterly Results → {url}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"https://www.nseindia.com/get-quaterly-results?symbol={symbol}",
            "Connection": "keep-alive",
        }

        try:
            resp = self.session.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f"⚠ Quarterly HTTP Error: {resp.status_code}")
                return None

            try:
                json_data = resp.json()
            except ValueError:
                print("⚠ Quarterly response not JSON (maybe blocked by NSE)")
                return None

            if not json_data:
                print("⚠ Quarterly API returned empty JSON")
                return None

            df = pd.DataFrame(json_data)
            print(f"✔ Quarterly results fetched: {len(df)} rows")
            return df

        except Exception as e:
            print("❌ Quarterly Error:", e)
            return None

    # --------------------------------------------------------
    # Download attachment (PDF/XML/etc.)
    # --------------------------------------------------------
    def download(self, url: str) -> bytes | None:
        """
        Download an attachment (PDF, XML, etc.) from NSE.
        """
        try:
            print(f"⬇ Downloading file → {url}")
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.content
            print(f"⚠ Download failed: HTTP {resp.status_code}")
            return None
        except Exception as e:
            print("❌ Download Error:", e)
            return None


# ============================================================
# Azure Upload Helper
# ============================================================
def upload_blob(path: str, content: bytes | str, content_type: str = "application/octet-stream") -> None:
    try:
        blob = blob_service.get_blob_client(AZURE_CONTAINER, path)
        blob.upload_blob(
            content,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )
        print(f"✔ Uploaded → {path}")
    except Exception as e:
        print("❌ Upload Error:", e)


# ============================================================
# MAIN PIPELINE
# ============================================================
def run_pipeline():
    nse = NSEUtils()

    # --------------------------------------------------------
    # 1️⃣ Corporate Announcements + PDFs + XBRL + Annual
    # --------------------------------------------------------
    df_anno = nse.get_announcements("CUPID")
    if df_anno is not None:
        # Save announcements metadata
        csv_buf = StringIO()
        df_anno.to_csv(csv_buf, index=False)
        upload_blob(
            "cupid/announcements/announcements.csv",
            csv_buf.getvalue(),
            "text/csv",
        )

        for _, row in df_anno.iterrows():
            attachment_url = row.get("attchmntFile")
            if not attachment_url:
                continue

            desc = str(row.get("desc", "")).lower()
            symbol = row.get("symbol", "CUPID")
            filename = os.path.basename(attachment_url)

            # Decide folder: XBRL, Annual, or generic announcement
            lower_name = filename.lower()
            if lower_name.endswith(".xml") or "xbrl" in lower_name:
                # Treat as XBRL file
                blob_path = f"cupid/xbrl/{filename}"
            elif any(
                kw in desc
                for kw in [
                    "annual report",
                    "financial year",
                    "audited",
                    "annual financial",
                ]
            ):
                # Treat as Annual report PDF
                blob_path = f"cupid/annual/{filename}"
            else:
                # Generic announcement attachment
                blob_path = f"cupid/announcements/{filename}"

            file_content = nse.download(attachment_url)
            if file_content:
                content_type = (
                    "application/pdf"
                    if lower_name.endswith(".pdf")
                    else "text/xml"
                    if lower_name.endswith(".xml")
                    else "application/octet-stream"
                )
                upload_blob(blob_path, file_content, content_type)
                time.sleep(1)  # be nice to NSE

    else:
        print("⚠ Skipping announcement downloads: None returned")

    # --------------------------------------------------------
    # 2️⃣ Quarterly Results CSV
    # --------------------------------------------------------
    df_quarter = nse.get_quarterly("CUPID")
    if df_quarter is not None:
        csv_buf = StringIO()
        df_quarter.to_csv(csv_buf, index=False)
        upload_blob(
            "cupid/quarterly/quarterly_results.csv",
            csv_buf.getvalue(),
            "text/csv",
        )
    else:
        print("⚠ Skipping quarterly upload: No data returned")

    print("\n🎉 CUPID Full Financial Data Pipeline Completed Successfully!")


# ============================================================
if __name__ == "__main__":
    run_pipeline()
