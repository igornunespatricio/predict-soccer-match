import json
import requests
from bs4 import BeautifulSoup


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
            team1 = row.find_all("div", class_="p-0")[0].get_text(strip=True)
            score = row.find_all("div", class_="p-0")[1].get_text(strip=True)
            team2 = row.find_all("div", class_="p-0")[2].get_text(strip=True)
        else:
            team1 = score = team2 = None
        row_data = {
            "round": round_number,
            "date": date,
            "team1": team1,
            "score": score,
            "team2": team2,
            "stadium": stadium,
        }
        round_data.append(row_data)
    return round_data


def save_data(data: dict, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def main(
    base_link,
    years: list,
    rounds: list = range(1, 39),
    save_path="data/raw_matches.json",
):
    data = {}
    for year in years:
        round_data = {}
        for round_number in rounds:
            link = get_round_link(base_link, year, round_number)
            status_coode, content = get_content(link)
            if status_coode != 200:
                print(f"Error: {status_coode}")
                continue
            round_data[f"round_{round_number}"] = parse_content(content)
        data[year] = round_data
    if save_path is not None:
        save_data(data, save_path)


if __name__ == "__main__":
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"

    main(base_link, years=[2024, 2025])
