from src.scraper import Scraper  # Import the new Scraper class
from src import feature_engineering, transform
from src.logger import setup_logger

logger = setup_logger()


def main():
    # Initialize the scraper
    scraper = Scraper()

    logger.info("Starting data scraping...")
    rounds_to_extract = {
        # 2020: list(range(1, 39)),
        # 2021: list(range(1, 39)),
        # 2022: list(range(1, 39)),
        # 2023: list(range(1, 39)),
        # 2024: list(range(1, 39)),
        # 2025: list(range(1, 13)),
        2023: [1, 2],
        2024: [1, 2],
    }

    for year, rounds in rounds_to_extract.items():
        for round_num in rounds:
            logger.info(f"Scraping year {year}, round {round_num}")
            success = scraper.scrape_round(year=year, round_number=round_num)

            if not success:
                logger.warning(f"Failed to scrape year {year}, round {round_num}")
                # Continue with next round instead of exiting
                continue

    logger.info("Data scraping completed.")

    # Rest of your existing pipeline remains unchanged
    logger.info("Transforming data")
    _ = transform.transform_data(
        read_path="data/matches_db.json",
        table="matches",
        save_path="data/matches.parquet",
    )
    logger.info("Transform data step completed.")

    # logger.info("Applying feature engineering")
    # _ = feature_engineering.feature_engineering(
    #     read_path="data/matches.parquet",
    #     save_path="data/matches_feature_engineered.parquet",
    # )
    # logger.info("Feature engineering completed.")

    # logger.info("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
