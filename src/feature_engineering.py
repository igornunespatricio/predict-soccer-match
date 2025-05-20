import pandas as pd


# ─────────────────────────────────────────────────────────────
# 1. BASIC MATCH FEATURES
# ─────────────────────────────────────────────────────────────


def add_is_weekend(df, date_col="date"):
    df["is_weekend"] = df[date_col].dt.dayofweek >= 5
    return df


def add_match_period(df: pd.DataFrame, date_col="date"):
    hour_column = df[date_col].dt.hour

    df["match_period"] = hour_column.apply(
        lambda x: "morning" if 0 <= x < 12 else "afternoon" if 12 <= x < 18 else "night"
    )
    return df


def add_day_of_week(df, date_col="date"):
    df["day_of_week"] = df[date_col].dt.day_name()
    return df


# ─────────────────────────────────────────────────────────────
# 1. HISTORY FEATURES
# ─────────────────────────────────────────────────────────────


def add_history_last_five_matches_each_team() -> pd.DataFrame:
    df = pd.read_parquet("data/matches.parquet")

    print(df.head())


def feature_engineering(
    read_path: str = "data/matches.parquet",
    save_path: str = "data/matches_feature_engineered.parquet",
):
    df = pd.read_parquet(read_path)
    df = add_is_weekend(df, date_col="match_date")
    df = add_match_period(df, date_col="match_date")
    df = add_day_of_week(df, date_col="match_date")
    df.to_parquet(save_path, index=False)
    return df


if __name__ == "__main__":
    # df = feature_engineering(read_path="data/matches.parquet")
    # print(df.head())
    add_history_last_five_matches_each_team()
