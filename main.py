from src import feature_engineering, get_data, transform
from src.logger import setup_logger

logger = setup_logger()


def main():
    # base_link = "https://www.api-futebol.com.br/campeonato/campeonato-brasileiro"
    # logger.info("Starting data scraping...")
    # rounds_to_extract = {
    #     2020: list(range(1, 39)),
    #     2021: list(range(1, 39)),
    #     2022: list(range(1, 39)),
    #     2023: list(range(1, 39)),
    #     2024: list(range(1, 39)),
    #     2025: list(range(1, 9)),
    # }

    # for year, rounds in rounds_to_extract.items():
    #     for round in rounds:
    #         get_data.get_data_for_round(
    #             base_link, year, round, db_path="data/matches_db.json"
    #         )
    # logger.info("Data scraping completed.")

    # logger.info("Transforming data")
    # _ = transform.transform_data(
    #     read_path="data/matches_db.json",
    #     table="matches",
    #     save_path="data/matches.parquet",
    # )
    # logger.info("Transform data step completed.")

    logger.info("Applying feature engineering")
    _ = feature_engineering.feature_engineering(
        read_path="data/matches.parquet",
        save_path="data/matches_feature_engineered.parquet",
    )
    logger.info("Feature engineering completed.")

    # logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
