from src.scraper import Scraper
from src.feature_engineering import MatchFeatureEngineer  # Updated import
from src.transform import DataTransformer
from src.logger import setup_logger
from src.db import load_table_as_dataframe
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
    parser.add_argument(
        "--skip-features",
        action="store_true",
        help="Skip the feature engineering phase",
    )
    args = parser.parse_args()

    # Scraping phase
    if not args.skip_scrape:
        scraper = Scraper()
        logger.info("Starting data scraping...")

        rounds_to_extract = {
            2020: range(1, 39),
            2021: range(1, 39),
            2022: range(1, 39),
            2023: range(1, 39),
            2024: range(1, 39),
            2025: range(1, 14),
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

    # Feature engineering phase
    if not args.skip_features:
        logger.info("Starting feature engineering...")
        try:
            feature_engineer = MatchFeatureEngineer()
            engineered_df = feature_engineer.run_pipeline(
                read_path="data/matches.parquet",
                save_path="data/matches_feature_engineered.parquet",
            )

            logger.info("Feature engineering completed successfully")

            # Optional: Show sample of engineered features
            logger.info("Sample of engineered features:")
            logger.info(engineered_df.head().to_string())

        except Exception as e:
            logger.error(f"Feature engineering failed: {str(e)}")
            raise
    else:
        logger.info("Skipping feature engineering phase as requested")

    logger.info("Pipeline completed")


if __name__ == "__main__":
    main()
