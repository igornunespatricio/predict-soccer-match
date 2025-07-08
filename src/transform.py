import pandas as pd
import numpy as np

import src.db as db


def split_score_column(df):
    """Splits the 'score' column into 'score_team1' and 'score_team2' as integers."""
    score_split = df["score"].str.extract(r"(\d+)\s*x\s*(\d+)")
    df["score_home_team"] = score_split[0].astype("Int64")
    df["score_guest_team"] = score_split[1].astype("Int64")
    return df.drop(columns=["score"])


def define_winning_team(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'winning_team' column indicating 'home', 'guest', 'draw', or NaN if scores are missing.

    Args:
        df (pd.DataFrame): DataFrame with 'score_home_team' and 'score_guest_team'.

    Returns:
        pd.DataFrame: Updated DataFrame with 'winning_team'.
    """

    def get_winner(row):
        home = row["score_home_team"]
        guest = row["score_guest_team"]

        if pd.isna(home) or pd.isna(guest):
            return np.nan  # Or "unknown" if you prefer a string label

        if home > guest:
            return "home"
        elif home < guest:
            return "guest"
        else:
            return "draw"

    df["winning_team"] = df.apply(get_winner, axis=1)

    return df


def parse_date_column(df, date_column="match_date", format="%d/%m/%Y %H:%M"):
    df[date_column] = pd.to_datetime(df[date_column], format=format, errors="coerce")
    return df


def transform_data(
    read_path: str = "data/matches_db.json",
    table: str = "matches",
    save_path: str = "data/matches.parquet",
):
    df = db.load_table_as_dataframe(path=read_path, table_name=table)
    df = split_score_column(df)
    df = define_winning_team(df)
    df = parse_date_column(df, date_column="match_date", format="%d/%m/%Y %H:%M")
    df = parse_date_column(df, date_column="date_added", format="%Y-%m-%dT%H:%M:%S.%f")
    df.to_parquet(save_path, index=False)
    return df


if __name__ == "__main__":

    print(
        transform_data(
            read_path="data/matches_db.json",
            table="matches",
            save_path="data/matches.parquet",
        ).head()
    )
