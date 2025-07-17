import requests
from bs4 import BeautifulSoup
from typing import Optional, Tuple, List, Dict
from dataclasses import dataclass
from urllib.parse import urljoin
from src.db import get_db, insert_match
from src.logger import setup_logger
import time
from random import uniform

logger = setup_logger()


@dataclass
class MatchData:
    round: str
    match_date: Optional[str]
    home_team: Optional[str]
    score: Optional[str]
    guest_team: Optional[str]
    stadium: Optional[str]


class ScraperConfig:
    BASE_URL = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"
    REQUEST_TIMEOUT = 10
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    HEADERS = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }


class Scraper:
    def __init__(self, base_url: str = None):
        self.base_url = base_url or ScraperConfig.BASE_URL
        self.session = requests.Session()
        self.session.headers.update(ScraperConfig.HEADERS)

    def get_round_url(self, year: int, round_number: int) -> str:
        """Construct the correct URL format: /campeonato/campeonato-brasileiro/YEAR/rodada/ROUND_NUMBER"""
        return f"{self.base_url}/{year}/rodada/{round_number}"

    def fetch_content(self, url: str) -> Tuple[int, Optional[bytes]]:
        """Fetch content from a URL with error handling and polite delays"""
        try:
            # Add random delay to be polite to the server
            time.sleep(uniform(1, 2))

            response = self.session.get(
                url, timeout=ScraperConfig.REQUEST_TIMEOUT, allow_redirects=True
            )
            response.raise_for_status()
            return response.status_code, response.content
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for {url}: {e.response.status_code}")
            return e.response.status_code, None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            return 500, None

    def parse_content(self, content: bytes) -> List[MatchData]:
        """Parse HTML content and extract match data"""
        soup = BeautifulSoup(content, "html.parser")

        # Safely extract round number
        round_element = soup.find("h6", class_="mb-0 mt-1")
        round_number = round_element.text if round_element else "Unknown"

        # Find all match cards
        cards = soup.find_all(
            "div",
            class_="card p-1 border-top-0 border-right border-left border-bottom rounded-0 bg-white text-center",
        )

        return [self._parse_card(card, round_number) for card in cards]

    def _parse_card(self, card, round_number: str) -> MatchData:
        """Parse individual match card"""
        headings = card.find_all("div", class_="text-center text-uppercase")
        date = headings[0].get_text(strip=True) if len(headings) > 0 else None
        stadium = headings[1].get_text(strip=True) if len(headings) > 1 else None

        row = card.find("div", class_="row small")
        if row:
            teams = row.find_all("div", class_="p-0")
            home_team = teams[0].get_text(strip=True) if len(teams) > 0 else None
            score = teams[1].get_text(strip=True) if len(teams) > 1 else None
            guest_team = teams[2].get_text(strip=True) if len(teams) > 2 else None
        else:
            home_team = score = guest_team = None

        return MatchData(
            round=round_number,
            match_date=date,
            home_team=home_team,
            score=score,
            guest_team=guest_team,
            stadium=stadium,
        )

    def scrape_round(
        self, year: int, round_number: int, db_path: str = "data/matches_db.json"
    ) -> bool:
        """Main method to scrape and save data for a specific round"""
        url = self.get_round_url(year, round_number)
        logger.info(f"Scraping URL: {url}")

        status_code, content = self.fetch_content(url)

        if status_code != 200 or not content:
            logger.error(f"Failed to fetch data for {year} round {round_number}")
            return False

        matches = self.parse_content(content)
        if not matches:
            logger.warning(f"No matches found for {year} round {round_number}")
            return False

        db, db_table = get_db(db_path)
        try:
            for match in matches:
                insert_match(db_table, match.__dict__)
            logger.info(
                f"Successfully saved {len(matches)} matches for {year} round {round_number}"
            )
            return True
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            return False
        finally:
            db.close()


def main():
    scraper = Scraper()

    # Define the years and rounds you want to scrape
    years_to_scrape = [2023, 2024, 2025]
    rounds_per_year = [1, 2]  # Typically 38 rounds in Brazilian league

    for year in years_to_scrape:
        for round_num in rounds_per_year:
            logger.info(f"Scraping year {year}, round {round_num}")
            success = scraper.scrape_round(year=year, round_number=round_num)

            if not success:
                logger.warning(f"Failed to scrape year {year}, round {round_num}")
                # Continue with next round instead of exiting
                continue


if __name__ == "__main__":
    main()
