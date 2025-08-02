"""
FBref Match Report Scraper - Complete Team Statistics
"""

from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from typing import Dict, List, Optional
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

    def process_csv(
        self, input_file: str, output_file: str, batch_size: int = 5
    ) -> bool:
        """
        Process a CSV file containing match_report_link column.
        Adds extracted stats as new columns and saves to output file.

        Args:
            input_file: Path to input CSV file
            output_file: Path to save enhanced CSV file
            batch_size: Number of matches to process between saves

        Returns:
            True if successful, False otherwise
        """
        try:
            # Read input CSV
            df = pd.read_csv(input_file)

            if "match_report_link" not in df.columns:
                logger.error("Input CSV missing 'match_report_link' column")
                return False

            # Prepare output directory
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Process matches in batches
            processed_count = 0
            results = []

            for idx, row in df.iterrows():
                if pd.isna(row["match_report_link"]):
                    logger.warning(f"Skipping row {idx} - empty match_report_link")
                    results.append({})
                    continue

                try:
                    logger.info(f"Processing match {idx+1}/{len(df)}")
                    match_stats = self.scrape_report(row["match_report_link"])

                    if match_stats:
                        # Flatten the extra stats groups into individual columns
                        flat_stats = self._flatten_stats(match_stats)
                        results.append(flat_stats)
                    else:
                        results.append({})

                    # Save progress periodically
                    if (idx + 1) % batch_size == 0 or (idx + 1) == len(df):
                        self._save_intermediate_results(df, results, output_file)
                        logger.info(f"Saved progress after {idx+1} matches")

                except Exception as e:
                    logger.error(f"Error processing row {idx}: {str(e)}")
                    results.append({})

            # Merge results with original dataframe
            final_df = self._merge_results(df, results)
            final_df.to_csv(output_file, index=False)
            logger.info(
                f"Successfully processed {len(df)} matches. Saved to {output_file}"
            )
            return True

        except Exception as e:
            logger.error(f"Error processing CSV: {str(e)}")
            return False

    def _flatten_stats(self, stats: Dict) -> Dict:
        """
        Flatten the nested stats structure into individual columns.
        """
        flat_stats = {}
        # Process main stats
        for key, value in stats.items():
            if not key.startswith("extra_stats_group"):
                flat_stats[key] = value
        # Process extra stats
        for group_key, group_values in stats.items():
            if group_key.startswith("extra_stats_group"):
                # Process each triplet (home, stat_name, away)
                for i in range(0, len(group_values), 3):
                    if i + 2 < len(group_values):
                        stat_name = group_values[i + 1].lower().replace(" ", "_")
                        flat_stats[f"{stat_name}_home"] = group_values[i]
                        flat_stats[f"{stat_name}_away"] = group_values[i + 2]
        return flat_stats

    def _save_intermediate_results(
        self, df: pd.DataFrame, results: List[Dict], output_file: str
    ):
        """
        Save intermediate results to avoid losing progress.
        """
        temp_df = df.iloc[: len(results)].copy()
        for i, result in enumerate(results):
            for key, value in result.items():
                temp_df.at[i, key] = value
        temp_df.to_csv(output_file, index=False)

    def _merge_results(self, df: pd.DataFrame, results: List[Dict]) -> pd.DataFrame:
        """
        Merge original dataframe with scraped results.
        """
        result_df = pd.DataFrame(results)
        return pd.concat([df, result_df], axis=1)


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Hardcoded file paths (change these as needed)
    INPUT_CSV = (
        "data/serie_a_2016_matches.csv"  # Input file with match_report_link column
    )
    OUTPUT_CSV = (
        "data/serie_a_2016_matches_enhanced.csv"  # Output file for enhanced data
    )

    # Run the scraper
    scraper = MatchReportScraper(headless=True)  # Set to False for debugging
    try:
        success = scraper.process_csv(
            input_file=INPUT_CSV,
            output_file=OUTPUT_CSV,
            batch_size=5,  # Process 5 matches between saves
        )
        if not success:
            logger.error("Failed to process CSV file")
            exit(1)
    finally:
        scraper.close()
