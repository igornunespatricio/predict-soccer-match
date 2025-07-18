from datetime import datetime
from typing import Optional
import pandas as pd
import numpy as np
from dataclasses import dataclass
from pathlib import Path
import logging
from src.db import load_table_as_dataframe

# Setup logging
logger = logging.getLogger(__name__)


@dataclass
class MatchData:
    """Dataclass to represent match data structure"""

    round: str
    match_date: Optional[datetime]
    home_team: Optional[str]
    guest_team: Optional[str]
    stadium: Optional[str]
    score_home_team: Optional[int]
    score_guest_team: Optional[int]
    winning_team: Optional[str]
    date_added: Optional[datetime]


class DataTransformer:
    """Handles transformation of raw match data into cleaned, structured data"""

    def __init__(self):
        pass

    def split_score_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Splits score column into separate home and guest team scores"""
        try:
            score_split = df["score"].str.extract(r"(\d+)\s*x\s*(\d+)")
            df["score_home_team"] = score_split[0].astype("Int64")
            df["score_guest_team"] = score_split[1].astype("Int64")
            return df.drop(columns=["score"])
        except Exception as e:
            logger.error(f"Error splitting score column: {str(e)}")
            raise

    def determine_winner(self, df: pd.DataFrame) -> pd.DataFrame:
        """Determines match outcome (home win, guest win, or draw)"""
        try:
            # Initialize winning_team column with None (will be converted to pd.NA)
            df["winning_team"] = None

            # Create masks for different outcomes
            home_wins = df["score_home_team"] > df["score_guest_team"]
            guest_wins = df["score_home_team"] < df["score_guest_team"]
            draws = df["score_home_team"] == df["score_guest_team"]
            missing_scores = (
                df["score_home_team"].isna() | df["score_guest_team"].isna()
            )

            # Apply the conditions
            df.loc[home_wins, "winning_team"] = "home"
            df.loc[guest_wins, "winning_team"] = "guest"
            df.loc[draws, "winning_team"] = "draw"
            df.loc[missing_scores, "winning_team"] = pd.NA

            # Convert to pandas' nullable string type
            df["winning_team"] = df["winning_team"].astype("string")

            return df
        except Exception as e:
            logger.error(f"Error determining winner: {str(e)}")
            raise

    def parse_dates(
        self, df: pd.DataFrame, column: str, date_format: str
    ) -> pd.DataFrame:
        """
        Converts string dates to datetime objects

        Args:
            df: Input DataFrame
            column: Name of column to parse
            date_format: Format string for parsing (or "ISO8601" for automatic)

        Returns:
            DataFrame with parsed dates
        """
        try:
            df[column] = pd.to_datetime(df[column], format=date_format, errors="coerce")
            return df
        except Exception as e:
            logger.error(f"Error parsing dates in column {column}: {str(e)}")
            raise

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executes the full transformation pipeline"""
        logger.info("Starting data transformation")
        df = self.split_score_column(df)
        df = self.determine_winner(df)
        df = self.parse_dates(df, "match_date", "%d/%m/%Y %H:%M")
        df = self.parse_dates(df, "date_added", "ISO8601")
        logger.info("Data transformation completed")
        return df


def transform_data(
    read_path: str = "data/matches_db.json",
    table: str = "matches",
    save_path: str = "data/matches.parquet",
) -> pd.DataFrame:
    """
    Main function to load, transform and save match data
    Args:
        read_path: Path to source JSON file
        table: Name of table to load
        save_path: Path to save transformed data
    Returns:
        Transformed DataFrame
    """
    try:
        # Load data
        logger.info(f"Loading data from {read_path}")
        df = load_table_as_dataframe(read_path, table)

        # Transform data
        transformer = DataTransformer()
        transformed_df = transformer.transform(df)

        # Save results
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        transformed_df.to_parquet(save_path, index=False)
        logger.info(f"Data successfully saved to {save_path}")

        return transformed_df
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Example usage
    import logging

    logging.basicConfig(level=logging.INFO)

    try:
        df = transform_data()
        print("Transformation successful. Sample data:")
        print(df.head())
    except Exception as e:
        print(f"Error during transformation: {str(e)}")
