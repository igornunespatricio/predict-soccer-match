import pandas as pd

from src import get_data, transform_data


def main():
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"
    get_data.get_data(base_link=base_link, db_path="data/matches_db.json")

    # transform_data.transform_data(path="data/matches.json")

    # df = pd.read_parquet("data/matches.parquet")
    # print(df.head())


if __name__ == "__main__":
    main()
