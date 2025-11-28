import requests
import pandas as pd
from datetime import datetime

class NseUtils:

    BASE_URL = "https://www.nseindia.com"
    ANNOUNCEMENTS_API = "https://www.nseindia.com/api/corporate-announcements"

    def __init__(self):
        """Initialize NSE session with cookies and required headers"""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive"
        })

        # Touch homepage to get cookies
        try:
            self.session.get(self.BASE_URL, timeout=10)
        except:
            print("⚠ Warning: Could not fetch initial cookies from NSE")

    def get_corporate_announcement(self):
        """
        Fetch corporate announcements from NSE API.
        Handles both JSON dict and JSON list formats.
        Returns a pandas DataFrame.
        """

        today = datetime.now().strftime("%d-%m-%Y")
        url = (
            f"{self.ANNOUNCEMENTS_API}"
            f"?index=equities&from_date={today}&to_date={today}&random=1"
        )

        print(f"CALLING NSE WITH URL → {url}")

        try:
            response = self.session.get(
                url,
                headers={"Referer": self.BASE_URL},
                timeout=20
            )

            if response.status_code != 200:
                print(f"⚠ Failed NSE API → HTTP {response.status_code}")
                return None

            json_data = response.json()

            # Case 1: Response is dict
            if isinstance(json_data, dict) and "data" in json_data:
                data = json_data["data"]

            # Case 2: Response is list
            elif isinstance(json_data, list):
                data = json_data

            else:
                print(f"⚠ Unknown NSE response format: {type(json_data)}")
                return None

            if not data:
                print("⚠ No announcements found.")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(data)

            # Clean description field
            if "desc" in df.columns:
                df["desc"] = df["desc"].astype(str)

            return df

        except Exception as e:
            print(f"❌ Error fetching announcements: {e}")
            return None

    def download_document(self, attachment_url):
        """
        Download NSE Corporate Announcement PDFs.
        """

        try:
            print(f"Downloading → {attachment_url}")

            response = self.session.get(
                attachment_url,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": self.BASE_URL
                },
                timeout=20
            )

            if response.status_code == 200:
                return response.content
            else:
                print(f"⚠ Failed PDF Download → HTTP {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ PDF Download Error: {e}")
            return None
