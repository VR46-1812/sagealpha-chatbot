import json
from playwright.sync_api import sync_playwright
import time
import logging

logging.basicConfig(level=logging.INFO)

COOKIE_FILE = "nse_cookies.json"

def save_nse_cookies():
    with sync_playwright() as p:
        logging.info("🌐 Launching Firefox browser...")
        browser = p.firefox.launch(headless=False)

        context = browser.new_context(
            ignore_https_errors=True,
            bypass_csp=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:112.0) Gecko/20100101 Firefox/112.0"
        )

        page = context.new_page()

        logging.info("🌐 Opening NSE homepage...")

        # More human-like navigation (NSE firewall requires this)
        page.goto("https://www.nseindia.com", wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)
        page.evaluate("window.scrollBy(0, 500)")
        page.wait_for_timeout(3000)

        logging.info("⏳ Waiting for cookies to stabilize...")
        time.sleep(2)

        cookies = context.cookies()

        with open(COOKIE_FILE, "w") as f:
            json.dump(cookies, f, indent=2)

        logging.info(f"✅ Cookies saved to {COOKIE_FILE}")

        browser.close()


if __name__ == "__main__":
    save_nse_cookies()
