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


def add_history_last_five_matches_each_team(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("match_date").reset_index(drop=True)

    # Initialize new columns
    for team_type in ["home_team", "guest_team"]:
        for result in ["wins", "draws", "loses"]:
            df[f"{team_type}_{result}_last_5"] = 0

    for idx, row in df.iterrows():
        match_date = row["match_date"]

        for team_col, team_side in [("home_team", "home"), ("guest_team", "guest")]:
            team_name = row[team_col]

            past_matches = (
                df[
                    (
                        (
                            (df["home_team"] == team_name)
                            | (df["guest_team"] == team_name)
                        )
                        & (df["match_date"] < match_date)
                    )
                ]
                .sort_values("match_date", ascending=False)
                .head(5)
            )

            wins = draws = loses = 0

            for _, match in past_matches.iterrows():
                if match["winning_team"] == "draw":
                    draws += 1
                elif (
                    match["winning_team"] == "home" and match["home_team"] == team_name
                ) or (
                    match["winning_team"] == "guest"
                    and match["guest_team"] == team_name
                ):
                    wins += 1
                else:
                    loses += 1

            df.at[idx, f"{team_col}_wins_last_5"] = wins
            df.at[idx, f"{team_col}_draws_last_5"] = draws
            df.at[idx, f"{team_col}_loses_last_5"] = loses

    df.to_excel("experiment/matches_with_history.xlsx", index=False)
    return df


def feature_engineering(
    read_path: str = "data/matches.parquet",
    save_path: str = "data/matches_feature_engineered.parquet",
):
    df = pd.read_parquet(read_path)
    df = add_is_weekend(df, date_col="match_date")
    df = add_match_period(df, date_col="match_date")
    df = add_day_of_week(df, date_col="match_date")
    df = add_history_last_five_matches_each_team(df=df)
    df.to_parquet(save_path, index=False)
    return df


if __name__ == "__main__":
    df = feature_engineering(read_path="data/matches.parquet")
