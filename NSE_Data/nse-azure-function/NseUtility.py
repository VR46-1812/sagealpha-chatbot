import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import time
import random

class NseUtils:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.nseindia.com/'
        })

        # Initialize cookies properly
        try:
            self.session.get("https://www.nseindia.com", timeout=10)
            time.sleep(1.2)
            self.session.get("https://www.nseindia.com/api/corporate-announcements", timeout=10)
        except:
            pass

    def get_corporate_announcement(self, from_date_str=None, to_date_str=None):
        # Default = TODAY ONLY!
        if from_date_str is None:
            from_date_str = datetime.now().strftime("%d-%m-%Y")
        if to_date_str is None:
            to_date_str = datetime.now().strftime("%d-%m-%Y")

        # Add anti-cache timestamp
        ts = int(time.time() * 1000)

        url = (
            f"https://www.nseindia.com/api/corporate-announcements?"
            f"index=equities&from_date={from_date_str}&to_date={to_date_str}&random={ts}"
        )

        try:
            print("CALLING NSE WITH URL →", url)
            r = self.session.get(url, timeout=15)

            if r.status_code == 200:
                data = r.json()

                # Filter only today's announcements properly
                return pd.DataFrame(data)
            else:
                print(f"NSE Error {r.status_code}")
                return None
        except Exception as e:
            print("Error:", e)
            return None
