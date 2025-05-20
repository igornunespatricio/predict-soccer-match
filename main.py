from src import feature_engineering, get_data, transform


def main():
    base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"
    print("Scraping data")
    get_data.get_data(base_link=base_link, db_path="data/matches_db.json")

    print("Transforming data")
    _ = transform.transform_data(
        read_path="data/matches_db.json",
        table="matches",
        save_path="data/matches.parquet",
    )

    # print("Feature engineering")
    # _ = feature_engineering.feature_engineering(
    #     read_path="data/matches.parquet",
    #     save_path="data/matches_feature_engineered.parquet",
    # )

    # print("Done")


if __name__ == "__main__":
    main()


# TODO:
# 1. Feature engineering:
#   add for the last 5 matches:
#       - add goals scored
#       - add goals conceded
#       - add goals difference
#   add for the season:
#       - current position
#       - percentage wins
#       - percentage defeats
#       - percentage draws
#       - add goals scored
#       - add goals conceded
#       - add goals difference
