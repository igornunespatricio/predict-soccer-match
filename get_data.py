import requests
from bs4 import BeautifulSoup


def get_round_link(link: str, round_number: int) -> str:
    round_link = f"{link}{round_number}"
    print(round_link)
    return round_link


def get_content(link: str) -> tuple:
    response = requests.get(link)
    return response.status_code, response.content


def parse_content(content: str) -> list:
    soup = BeautifulSoup(content, "html.parser")
    round_number = soup.find("h6", class_="mb-0 mt-1").text
    print(round_number)
    games = soup.find_all(
        "div",
        class_="card p-1 border-top-0 border-right border-left border-bottom rounded-0 bg-white text-center",
    )
    l = []
    for game in games:

        list_of_info = game.text.split("\n")
        # remove empty strings
        cleaned_list = list(filter(None, list_of_info))
        print(cleaned_list)
        print("-" * 50)


def main(base_link, max_rounds=2):
    for round_number in range(1, max_rounds + 1):
        link = get_round_link(base_link, round_number)
        status_coode, content = get_content(link)
        if status_coode != 200:
            continue
        parse_content(content)


if __name__ == "__main__":
    base_link = (
        "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro/2025/rodada/"
    )

    main(base_link)
