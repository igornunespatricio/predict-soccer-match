import datetime
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
    home_formation: Optional[str] = None  # New field
    away_formation: Optional[str] = None  # New field
    home_lineup: Optional[List[Dict]] = None  # New field (list of players)
    away_lineup: Optional[List[Dict]] = None  # New field (list of players)


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

    def scrape_match_details(
        self, relative_url: str
    ) -> Tuple[
        Optional[str], Optional[str], Optional[List[Dict]], Optional[List[Dict]]
    ]:
        """Scrape formations and lineups from a match details page."""
        url = urljoin(self.base_url, relative_url)
        status_code, content = self.fetch_content(url)

        if status_code != 200 or not content:
            logger.error(f"Failed to fetch match details: {url}")
            return None, None, None, None

        soup = BeautifulSoup(content, "html.parser")

        # Clean extraction with minimal processing
        def extract_formation(div):
            if not div:
                return None
            text = div.get_text(strip=True)  # Basic cleanup
            return text.split("(")[-1].split(")")[0] if "(" in text else None

        home_formation = extract_formation(
            soup.find("div", class_="col-sm-2 col-12 text-sm-left text-center")
        )

        away_formation = extract_formation(
            soup.find("div", class_="col-sm-2 col-12 text-sm-right text-center")
        )

        # Extract lineups
        lineup_tables = soup.find_all("table", class_="table")
        home_lineup = away_lineup = None

        if len(lineup_tables) >= 2:
            home_lineup = self._parse_lineup_table(lineup_tables[0])
            away_lineup = self._parse_lineup_table(lineup_tables[1])

        return home_formation, away_formation, home_lineup, away_lineup

    def _parse_lineup_table(self, table) -> List[Dict]:
        """Parse lineup table, tracking substitutions (arrows), yellow/red cards, and goals."""
        players = []
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            number = cells[0].get_text(strip=True)
            position = cells[1].get_text(strip=True)
            name = cells[2].get_text(strip=True)

            # Check substitution arrows
            substitution_status = None
            svg_icons = row.find_all("svg")

            for icon in svg_icons:
                path = icon.find("path")
                if (
                    path and path.get("fill") == "#FA1200"
                ):  # Red arrow (substituted OUT)
                    substitution_status = "substituted_out"
                    break
                elif (
                    path and path.get("fill") == "#399C00"
                ):  # Green arrow (substituted IN)
                    substitution_status = "substituted_in"
                    break

            # Check cards and goals
            yellow_card = bool(row.find("img", class_="cartao-amarelo-icon"))
            red_card = bool(
                row.find("img", class_="cartao-vermelho-icon")
            )  # New: Red card detection
            goals = len(
                row.find_all("img", class_="gol-bola-icon")
            )  # Count goals (optional)

            players.append(
                {
                    "number": number,
                    "position": position,
                    "name": name,
                    "substitution_status": substitution_status,
                    "yellow_card": yellow_card,
                    "red_card": red_card,
                    "goals": goals,  # Optional: Track goals scored
                }
            )

        return players

    def _parse_card(self, card, round_number: str) -> MatchData:
        """Parse individual match card including all new details"""
        # Extract basic match info
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

        # Extract match details link
        details_link = None
        details_btn = card.find("a", class_="btn btn-sm btn-primary-3 smaller p-1")
        if details_btn and details_btn.get("href"):
            details_link = details_btn["href"]

        # Create match data object with basic info
        match_data = MatchData(
            round=round_number,
            match_date=date,
            home_team=home_team,
            score=score,
            guest_team=guest_team,
            stadium=stadium,
        )

        # If details link exists, fetch additional data
        if details_link:
            try:
                (
                    match_data.home_formation,
                    match_data.away_formation,
                    match_data.home_lineup,
                    match_data.away_lineup,
                ) = self.scrape_match_details(details_link)

                # Log sample lineup data for debugging
                if match_data.home_lineup:
                    logger.debug(
                        f"Home lineup for {home_team}: {[p['name'] for p in match_data.home_lineup[:3]]}..."
                    )
                if match_data.away_lineup:
                    logger.debug(
                        f"Away lineup for {guest_team}: {[p['name'] for p in match_data.away_lineup[:3]]}..."
                    )

            except Exception as e:
                logger.error(f"Failed to parse match details: {str(e)}")

        return match_data

    def scrape_round(
        self, year: int, round_number: int, db_path: str = "data/matches_db.json"
    ) -> bool:
        """Scrape and store raw match data including formations and player details"""
        url = self.get_round_url(year, round_number)
        logger.info(f"Scraping round {round_number} ({year}) from: {url}")

        status_code, content = self.fetch_content(url)
        if status_code != 200 or not content:
            logger.error(f"Failed to fetch round content (HTTP {status_code})")
            return False

        matches = self.parse_content(content)
        if not matches:
            logger.warning(f"No matches found in round content")
            return False

        db, db_table = get_db(db_path)
        try:
            for match in matches:
                match_data = {
                    # Core match info
                    "round": match.round,
                    "match_date": match.match_date,
                    "home_team": match.home_team,
                    "score": match.score,
                    "guest_team": match.guest_team,
                    "stadium": match.stadium,
                    # Extracted details
                    "home_formation": match.home_formation,
                    "away_formation": match.away_formation,
                    "home_lineup": match.home_lineup if match.home_lineup else [],
                    "away_lineup": match.away_lineup if match.away_lineup else [],
                    # Metadata
                    "scraped_at": datetime.datetime.utcnow().isoformat(),
                    "data_source": url,
                }

                db_table.insert(match_data)
                logger.debug(f"Stored match: {match.home_team} vs {match.guest_team}")

            logger.info(f"Saved {len(matches)} matches (round {round_number}, {year})")
            return True

        except Exception as e:
            logger.error(f"Database insertion failed: {str(e)}")
            return False
        finally:
            db.close()


def main():
    scraper = Scraper()

    # Define the years and rounds you want to scrape
    years_to_scrape = [2025]
    rounds_per_year = [14]  # Typically 38 rounds in Brazilian league

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
