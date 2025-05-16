import requests
from bs4 import BeautifulSoup

from src.utils import read_config
from src.db import get_db, insert_match  # Import your TinyDB helpers


def get_round_link(link: str, year: int, round_number: int) -> str:
    round_link = f"{link}/{year}/rodada/{round_number}"
    print(round_link)
    return round_link


def get_content(link: str) -> tuple:
    response = requests.get(link)
    return response.status_code, response.content


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
    return round_data


def get_data(
    base_link,
    db_path="data/matches_db.json",
):
    db, db_table = get_db(db_path)
    config = read_config(path="config.yml")

    for year, rounds in config.items():
        for round_number in rounds:
            link = get_round_link(base_link, year, round_number)
            status_code, content = get_content(link)
            if status_code != 200:
                print(f"Error: {status_code}")
                continue

            round_matches = parse_content(content)
            for match in round_matches:
                insert_match(db_table, match)
    db.close()  # Close the TinyDB database


if __name__ == "__main__":
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"

    get_data(base_link)
