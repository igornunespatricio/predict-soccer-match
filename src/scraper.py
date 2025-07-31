"""
FBref Multi-Season Scraper with Per-Season CSV Saving
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from src.logger import setup_logger
import sys
import time
import os

# Initialize logger
logger = setup_logger()

# Constants
BASE_URL = "https://fbref.com"
PAGE_LOAD_TIMEOUT = 30
WEBDRIVER_TIMEOUT = 10
REQUEST_DELAY = 5  # seconds between requests
COMPETITION_CODE = "24"  # Serie A competition code
OUTPUT_DIR = "data"  # Directory for output files


@dataclass
class MatchData:
    """Data class to store match information using data-stat attributes"""

    date: str
    home_team: str
    score: str
    away_team: str
    attendance: str
    match_report_link: Optional[str]
    season: str


class FBrefSeasonalScraper:
    """Professional FBref scraper with per-season CSV output"""

    def __init__(self, headless: bool = True):
        self.driver = self._init_webdriver(headless)
        self._ensure_output_dir()
        logger.info("WebDriver initialized with per-season CSV saving")

    def _ensure_output_dir(self):
        """Create output directory if it doesn't exist"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def _init_webdriver(self, headless: bool) -> webdriver.Chrome:
        """Configure Chrome WebDriver with anti-detection measures"""
        options = Options()
        if headless:
            options.add_argument("--headless=new")

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument(
            f"user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            driver = webdriver.Chrome(options=options)
            driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
            return driver
        except WebDriverException as e:
            logger.error(f"WebDriver initialization failed: {str(e)}")
            raise

    def _get_season_urls(self, start_year: int, end_year: int) -> List[Tuple[str, str]]:
        """Generate list of (season_year, url) tuples for the range"""
        return [
            (
                str(year),
                f"{BASE_URL}/en/comps/{COMPETITION_CODE}/{year}/schedule/{year}-Serie-A-Scores-and-Fixtures",
            )
            for year in range(start_year, end_year + 1)
        ]

    def _load_page(self, url: str) -> bool:
        """Load page and wait for table to be present"""
        try:
            logger.info(f"Loading: {url}")
            self.driver.get(url)
            WebDriverWait(self.driver, WEBDRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table[id^='sched_']"))
            )
            time.sleep(REQUEST_DELAY)
            return True
        except TimeoutException:
            logger.error(f"Timeout waiting for table on {url}")
            return False

    def _extract_from_cell(self, cell, attribute: str) -> str:
        """Helper to extract text from cell based on data-stat attribute"""
        element = cell.find(attrs={"data-stat": attribute})
        return element.get_text(strip=True) if element else ""

    def _extract_link_from_cell(self, cell, attribute: str) -> Optional[str]:
        """Helper to extract link from cell based on data-stat attribute"""
        element = cell.find(attrs={"data-stat": attribute})
        if element and element.find("a", href=True):
            return f"{BASE_URL}{element.find('a')['href']}"
        return None

    def _parse_row(self, row, season: str) -> Optional[MatchData]:
        """Parse a single match row using data-stat attributes"""
        try:
            return MatchData(
                date=self._extract_from_cell(row, "date"),
                home_team=self._extract_from_cell(row, "home_team"),
                score=self._extract_from_cell(row, "score"),
                away_team=self._extract_from_cell(row, "away_team"),
                attendance=self._extract_from_cell(row, "attendance"),
                match_report_link=self._extract_link_from_cell(row, "match_report"),
                season=season,
            )
        except Exception as e:
            logger.warning(f"Error parsing row: {str(e)}")
            return None

    def _save_season_data(self, season: str, df: pd.DataFrame) -> bool:
        """Save a season's data to CSV with error handling"""
        try:
            filename = f"{OUTPUT_DIR}/serie_a_{season}_matches.csv"
            df.to_csv(filename, index=False)
            logger.info(f"Successfully saved season {season} data to {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save season {season} data: {str(e)}")
            return False

    def scrape_season(self, season: str, url: str) -> Optional[pd.DataFrame]:
        """Scrape and save a single season"""
        if not self._load_page(url):
            return None

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        table = soup.find("table", {"id": lambda x: x and x.startswith("sched_")})

        if not table:
            logger.error(f"Schedule table not found for season {season}")
            return None

        matches = []
        for row in table.find_all("tr")[1:]:
            match_data = self._parse_row(row, season)
            if match_data and match_data.score and match_data.score != "Score":
                matches.append(vars(match_data))

        if not matches:
            logger.error(f"No valid matches found for season {season}")
            return None

        season_df = pd.DataFrame(matches)
        self._save_season_data(season, season_df)
        return season_df

    def scrape_seasons(self, start_year: int, end_year: int) -> pd.DataFrame:
        """Scrape multiple seasons, saving each as CSV and combining results"""
        season_urls = self._get_season_urls(start_year, end_year)
        all_matches = []

        for season, url in season_urls:
            logger.info(f"Starting scrape for season {season}")
            season_df = self.scrape_season(season, url)
            if season_df is not None:
                all_matches.append(season_df)
            time.sleep(REQUEST_DELAY)

        if not all_matches:
            logger.error("No data was scraped from any season")
            return pd.DataFrame()

        combined_df = pd.concat(all_matches, ignore_index=True)

        # Save combined data as well
        combined_filename = (
            f"{OUTPUT_DIR}/serie_a_combined_{start_year}_to_{end_year}.csv"
        )
        combined_df.to_csv(combined_filename, index=False)
        logger.info(f"Saved combined data to {combined_filename}")

        return combined_df

    def close(self):
        """Clean up resources"""
        try:
            self.driver.quit()
            logger.info("WebDriver closed")
        except Exception as e:
            logger.error(f"Error closing WebDriver: {str(e)}")


def main():
    """Execution workflow"""
    scraper = None
    try:
        scraper = FBrefSeasonalScraper(headless=True)

        # Scrape from 2016 to 2025
        matches_df = scraper.scrape_seasons(2016, 2021)

        if not matches_df.empty:
            logger.info(f"Scraping complete. Total matches: {len(matches_df)}")
            logger.info(f"Seasons included: {matches_df['season'].unique()}")
            return 0
        return 1
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")
        return 1
    finally:
        if scraper:
            scraper.close()


if __name__ == "__main__":
    sys.exit(main())
