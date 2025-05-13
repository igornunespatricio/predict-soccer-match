import pandas as pd
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# 1. BASIC MATCH FEATURES
# ─────────────────────────────────────────────────────────────


def add_is_weekend(df, date_col="date"):
    df["is_weekend"] = df[date_col].dt.dayofweek >= 5
    return df


def add_match_hour(df, date_col="date"):
    df["hour"] = df[date_col].dt.hour
    return df


def add_day_of_week(df, date_col="date"):
    df["day_of_week"] = df[date_col].dt.day_name()
    return df


def feature_engineering(path: str):
    df = pd.read_parquet(path)
    df = add_is_weekend(df)
    df = add_match_hour(df)
    df = add_day_of_week(df)
    return df


if __name__ == "__main__":
    df = feature_engineering(path="data/matches.parquet")
    print(df.head())
