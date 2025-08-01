"""
FBref Match Report Scraper - Complete Team Statistics
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from typing import Dict, Optional
from urllib.parse import urljoin
import time
import logging

logger = logging.getLogger(__name__)


class MatchReportScraper:
    """
    Scraper that extracts complete statistics from both team_stats and team_stats_extra divs.
    """

    def __init__(
        self,
        headless: bool = True,
        base_url: str = "https://fbref.com",
        request_delay: int = 3,
        timeout: int = 30,
    ):
        self.base_url = base_url
        self.request_delay = request_delay
        self.timeout = timeout
        self.driver = self._init_webdriver(headless)
        logger.info("MatchReportScraper initialized")

    def _init_webdriver(self, headless: bool) -> webdriver.Chrome:
        """Initialize and configure Chrome WebDriver."""
        options = Options()
        if headless:
            options.add_argument("--headless=new")

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(self.timeout)
        return driver

    def scrape_report(self, report_url: str) -> Optional[Dict]:
        """
        Scrape complete statistics from a match report page.
        """
        if not report_url:
            return None

        full_url = urljoin(self.base_url, report_url)

        try:
            logger.debug(f"Loading: {full_url}")
            self.driver.get(full_url)

            # Wait for both stats sections to load separately
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.ID, "team_stats"))
            )
            WebDriverWait(self.driver, self.timeout).until(
                EC.presence_of_element_located((By.ID, "team_stats_extra"))
            )
            time.sleep(self.request_delay)

            soup = BeautifulSoup(self.driver.page_source, "html.parser")

            # Extract from both sections
            stats = {}
            stats.update(self._extract_main_stats(soup))
            stats.update(self._extract_extra_stats(soup))

            return stats

        except TimeoutException:
            logger.error(f"Timeout waiting for stats sections on: {full_url}")
            return None
        except Exception as e:
            logger.error(f"Error scraping report {full_url}: {str(e)}")
            return None

    def _extract_main_stats(self, soup: BeautifulSoup) -> Dict:
        """
        Extract statistics from the main team_stats div.
        """
        stats = {}
        stats_div = soup.find("div", id="team_stats")

        if not stats_div:
            logger.warning("Team stats div not found")
            return stats

        stats_table = stats_div.find("table", {"cellpadding": "0", "cellspacing": "8"})

        if not stats_table:
            logger.warning("Main stats table not found")
            return stats

        current_stat = None

        for row in stats_table.find_all("tr"):
            # Skip card-related rows
            if row.th and "Cards" in row.th.get_text():
                continue

            # Detect stat category rows
            if row.th and row.th.get("colspan") == "2":
                current_stat = row.th.get_text(strip=True).lower()
                continue

            # Process data rows
            if current_stat and len(row.find_all("td")) == 2:
                home_td, away_td = row.find_all("td")

                # Extract complete text content
                home_value = home_td.get_text(strip=True)
                away_value = away_td.get_text(strip=True)

                if home_value or away_value:
                    stats[f"{current_stat}_home"] = home_value
                    stats[f"{current_stat}_away"] = away_value

        return stats

    def _extract_extra_stats(self, soup: BeautifulSoup) -> Dict:
        """
        Extract all non-header stats from the team_stats_extra div.
        Returns a dictionary with raw stat values grouped by their parent div index.
        """
        stats = {}
        extra_div = soup.find("div", id="team_stats_extra")

        if not extra_div:
            logger.warning("Team stats extra div not found")
            return stats

        # Get the 3 main stat group divs
        stat_groups = extra_div.find_all("div", recursive=False)

        for group_idx, group in enumerate(stat_groups, 1):
            # Find all non-header divs in this group
            stat_divs = [
                div
                for div in group.find_all("div", recursive=False)
                if "th" not in div.get("class", [])
            ]

            # Store all values in order for this group
            group_stats = []
            for div in stat_divs:
                group_stats.append(div.get_text(strip=True))

            stats[f"extra_stats_group_{group_idx}"] = group_stats

        return stats

    def close(self):
        """Clean up resources."""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
        except Exception as e:
            logger.error(f"Error closing WebDriver: {str(e)}")


if __name__ == "__main__":
    import json

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Example usage
    scraper = MatchReportScraper(headless=False)  # Visible for debugging
    try:
        test_url = "https://fbref.com/en/matches/42463e83/Flamengo-Sport-Recife-May-14-2016-Serie-A"
        match_stats = scraper.scrape_report(test_url)

        print("Complete Match Statistics:")
        print(json.dumps(match_stats, indent=2))

        # Expected output now includes both main and extra stats:
        # {
        #   "possession_home": "56%",
        #   "possession_away": "44%",
        #   "passing accuracy_home": "483 of 553 — 87%",
        #   "passing accuracy_away": "83% — 362 of 437",
        #   "shots on target_home": "5 of 11 — 45%",
        #   "shots on target_away": "100% — 1 of 1",
        #   "saves_home": "1 of 1 — 100%",
        #   "saves_away": "80% — 4 of 5",
        #   "fouls_home": "18",
        #   "fouls_away": "17",
        #   "corners_home": "4",
        #   "corners_away": "4",
        #   ... (all other extra stats)
        # }
    finally:
        scraper.close()
