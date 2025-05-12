import json
import requests
from bs4 import BeautifulSoup

from src.utils import save_json, read_config


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
            home_team = score = guest = None
        row_data = {
            "round": round_number,
            "date": date,
            "home_team": home_team,
            "score": score,
            "guest_team": guest_team,
            "stadium": stadium,
        }
        round_data.append(row_data)
    return round_data


def get_data(
    base_link,
    save_path="data/raw_matches.json",
):
    data = {}
    config = read_config(path="config.yml")
    for year, rounds in config.items():
        round_data = {}
        for round_number in rounds:
            link = get_round_link(base_link, year, round_number)
            status_code, content = get_content(link)
            if status_code != 200:
                print(f"Error: {status_code}")
                continue
            round_data[f"round_{round_number}"] = parse_content(content)
        data[year] = round_data
    if save_path is not None:
        save_json(data, save_path)


if __name__ == "__main__":
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"

    get_data(base_link)
