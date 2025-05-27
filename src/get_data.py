import requests
from bs4 import BeautifulSoup

from src.db import get_db, insert_match  # Import your TinyDB helpers
from src.logger import setup_logger

logger = setup_logger()


def get_round_link(link: str, year: int, round_number: int) -> str:
    round_link = f"{link}/{year}/rodada/{round_number}"
    # print(round_link)
    logger.info(f"Generated link: {round_link}")
    return round_link


def get_content(link: str) -> tuple:
    try:
        response = requests.get(link)
        return response.status_code, response.content
    except Exception as e:
        logger.error(f"Error fetching content from {link}: {e}")
        return 500, None


def parse_content(content: str) -> list:
    soup = BeautifulSoup(content, "html.parser")
    round_number = soup.find("h6", class_="mb-0 mt-1").text
    cards = soup.find_all(
        "div",
        class_="card p-1 border-top-0 border-right border-left border-bottom rounded-0 bg-white text-center",
    )
    round_data = []
    for card in cards:
        # 1. Get all text-center text-uppercase divs inside card
        headings = card.find_all("div", class_="text-center text-uppercase")
        date = headings[0].get_text(strip=True) if len(headings) > 0 else None
        stadium = headings[1].get_text(strip=True) if len(headings) > 1 else None

        # 2. Extract match info
        row = card.find("div", class_="row small")
        if row:
            home_team = row.find_all("div", class_="p-0")[0].get_text(strip=True)
            score = row.find_all("div", class_="p-0")[1].get_text(strip=True)
            guest_team = row.find_all("div", class_="p-0")[2].get_text(strip=True)
        else:
            home_team = score = guest_team = None
        row_data = {
            "round": round_number,
            "match_date": date,
            "home_team": home_team,
            "score": score,
            "guest_team": guest_team,
            "stadium": stadium,
        }
        round_data.append(row_data)
    logger.info(f"Parsed {len(round_data)} matches for round {round_number}")
    return round_data


def get_data_for_round(
    base_link: str, year: int, round_number: int, db_path: str = "data/matches_db.json"
):
    """
    Fetches data for a specific year and round, parses it, and saves it to TinyDB.

    Args:
        base_link (str): The base URL for scraping.
        year (int): The year of the matches to scrape.
        round_number (int): The round number to scrape.
        db_path (str): Path to the TinyDB database (default: 'data/matches_db.json').
    """
    db, db_table = get_db(db_path)

    link = get_round_link(base_link, year, round_number)
    status_code, content = get_content(link)

    if status_code != 200:
        # print(f"Error fetching data: HTTP {status_code} for {link}")
        logger.error(f"Error fetching data: HTTP {status_code} for {link}")
        db.close()
        return

    round_matches = parse_content(content)
    if round_matches:
        for match in round_matches:
            insert_match(db_table, match)

        logger.info(f"Data for year {year}, round {round_number} saved successfully.")
    else:
        logger.warning(f"No matches found for year {year} round {round_number}")
    db.close()  # Always close the DB
    # print(f"Data for year {year}, round {round_number} saved successfully.")


if __name__ == "__main__":
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"

    get_data_for_round(base_link, year=2024, round_number=1)
