from src.scraper import Scraper
from src import feature_engineering
from src.transform import DataTransformer
from src.logger import setup_logger
from src.db import load_table_as_dataframe
import pandas as pd
import argparse

logger = setup_logger()


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Run the soccer match prediction pipeline"
    )
    parser.add_argument(
        "--skip-scrape", action="store_true", help="Skip the scraping phase"
    )
    parser.add_argument(
        "--skip-transform", action="store_true", help="Skip the transformation phase"
    )
    args = parser.parse_args()

    # Scraping phase
    if not args.skip_scrape:
        scraper = Scraper()
        logger.info("Starting data scraping...")

        rounds_to_extract = {
            2023: [1, 2],
            2024: [1, 2],
        }

        for year, rounds in rounds_to_extract.items():
            for round_num in rounds:
                logger.info(f"Scraping year {year}, round {round_num}")
                success = scraper.scrape_round(year=year, round_number=round_num)
                if not success:
                    logger.warning(f"Failed to scrape year {year}, round {round_num}")

        logger.info("Data scraping completed.")
    else:
        logger.info("Skipping scraping phase as requested")

    # Transformation phase
    if not args.skip_transform:
        logger.info("Starting data transformation...")
        transformer = DataTransformer()

        try:
            df = load_table_as_dataframe("data/matches_db.json", "matches")

            if df.empty:
                logger.warning("No data found in matches_db.json")
            else:
                logger.info(f"Loaded {len(df)} matches for transformation")
                transformed_df = transformer.transform(df)
                transformed_df.to_parquet("data/matches.parquet", index=False)
                logger.info("Data transformation completed successfully")

                # Optional: Show sample of transformed data
                logger.info("Sample of transformed data:")
                logger.info(transformed_df.head().to_string())

        except Exception as e:
            logger.error(f"Data transformation failed: {str(e)}")
            raise
    else:
        logger.info("Skipping transformation phase as requested")

    # Feature engineering (currently commented out)
    # logger.info("Applying feature engineering...")
    # feature_engineering.feature_engineering(...)

    logger.info("Pipeline completed")


if __name__ == "__main__":
    main()
