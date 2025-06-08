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

        df[f"{team_type}_goals_scored_last_5"] = 0
        df[f"{team_type}_goals_conceded_last_5"] = 0
        df[f"{team_type}_goal_difference_last_5"] = 0

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
            goals_scored = 0
            goals_conceded = 0

            for _, match in past_matches.iterrows():
                # Count goals scored based on whether the team was home or guest
                if match["home_team"] == team_name:
                    goals_scored += match["score_home_team"]
                    goals_conceded += match["score_guest_team"]
                elif match["guest_team"] == team_name:
                    goals_scored += match["score_guest_team"]
                    goals_conceded += match["score_home_team"]

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

            goal_difference = goals_scored - goals_conceded

            df.at[idx, f"{team_col}_wins_last_5"] = wins
            df.at[idx, f"{team_col}_draws_last_5"] = draws
            df.at[idx, f"{team_col}_loses_last_5"] = loses
            df.at[idx, f"{team_col}_goals_scored_last_5"] = goals_scored
            df.at[idx, f"{team_col}_goals_conceded_last_5"] = goals_conceded
            df.at[idx, f"{team_col}_goal_difference_last_5"] = goal_difference

    # df.to_excel("experiment/matches_with_history.xlsx", index=False)
    return df


def add_current_position_in_season_optimized(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    # Convert types
    data["match_date"] = pd.to_datetime(data["match_date"], unit="ms")
    data["score_home_team"] = pd.to_numeric(data["score_home_team"], errors="coerce")
    data["score_guest_team"] = pd.to_numeric(data["score_guest_team"], errors="coerce")
    data["year"] = data["match_date"].dt.year

    data = data.sort_values("match_date").reset_index(drop=True)

    # Initialize columns
    data["home_team_current_position"] = None
    data["guest_team_current_position"] = None

    data["home_team_goals_scored"] = 0
    data["home_team_goals_conceded"] = 0
    data["home_team_goal_difference"] = 0

    data["guest_team_goals_scored"] = 0
    data["guest_team_goals_conceded"] = 0
    data["guest_team_goal_difference"] = 0

    for year in data["year"].unique():
        year_data = data[data["year"] == year].copy()
        year_data = year_data.sort_values("match_date")

        # Initialize standings dict
        standings = {}

        for idx, row in year_data.iterrows():
            home = row["home_team"]
            guest = row["guest_team"]

            # Ensure teams are in standings
            for team in [home, guest]:
                if team not in standings:
                    standings[team] = {
                        "points": 0,
                        "goals_scored": 0,
                        "goals_conceded": 0,
                    }

            # Calculate current standings before this match
            table = pd.DataFrame.from_dict(standings, orient="index").assign(
                team=lambda x: x.index
            )
            table["goal_difference"] = table["goals_scored"] - table["goals_conceded"]
            table = table.sort_values(
                by=["points", "goal_difference", "goals_scored"],
                ascending=[False, False, False],
            ).reset_index(drop=True)
            table["position"] = table.index + 1

            # Assign current positions
            home_pos = table.loc[table["team"] == home, "position"]
            guest_pos = table.loc[table["team"] == guest, "position"]

            data.loc[idx, "home_team_current_position"] = (
                int(home_pos.values[0]) if not home_pos.empty else None
            )
            data.loc[idx, "guest_team_current_position"] = (
                int(guest_pos.values[0]) if not guest_pos.empty else None
            )

            # Assign current cumulative goals stats before the match
            data.loc[idx, "home_team_goals_scored"] = standings[home]["goals_scored"]
            data.loc[idx, "home_team_goals_conceded"] = standings[home][
                "goals_conceded"
            ]
            data.loc[idx, "home_team_goal_difference"] = (
                standings[home]["goals_scored"] - standings[home]["goals_conceded"]
            )

            data.loc[idx, "guest_team_goals_scored"] = standings[guest]["goals_scored"]
            data.loc[idx, "guest_team_goals_conceded"] = standings[guest][
                "goals_conceded"
            ]
            data.loc[idx, "guest_team_goal_difference"] = (
                standings[guest]["goals_scored"] - standings[guest]["goals_conceded"]
            )

            # Only update standings if the match has valid scores
            if pd.notna(row["score_home_team"]) and pd.notna(row["score_guest_team"]):
                home_goals = row["score_home_team"]
                guest_goals = row["score_guest_team"]

                # Goals
                standings[home]["goals_scored"] += home_goals
                standings[home]["goals_conceded"] += guest_goals

                standings[guest]["goals_scored"] += guest_goals
                standings[guest]["goals_conceded"] += home_goals

                # Points
                if row["winning_team"] == "home":
                    standings[home]["points"] += 3
                elif row["winning_team"] == "guest":
                    standings[guest]["points"] += 3
                elif row["winning_team"] == "draw":
                    standings[home]["points"] += 1
                    standings[guest]["points"] += 1

    return data


def add_wins_draws_losses_in_season(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()

    # Preprocessing
    data["match_date"] = pd.to_datetime(data["match_date"], unit="ms")
    data["score_home_team"] = pd.to_numeric(data["score_home_team"], errors="coerce")
    data["score_guest_team"] = pd.to_numeric(data["score_guest_team"], errors="coerce")
    data["year"] = data["match_date"].dt.year

    data = data.sort_values("match_date").reset_index(drop=True)

    # Initialize result columns
    result_types = ["wins", "draws", "losses"]
    for prefix in ["home", "guest"]:
        for r in result_types:
            data[f"{prefix}_team_{r}_so_far"] = 0

    # Process year by year
    for year in data["year"].unique():
        year_data = data[data["year"] == year]
        year_data = year_data.sort_values("match_date")

        # Track record per team
        team_stats = {}

        for idx in year_data.index:
            row = data.loc[idx]
            home = row["home_team"]
            guest = row["guest_team"]
            result = row["winning_team"]

            # Initialize team record if needed
            for team in [home, guest]:
                if team not in team_stats:
                    team_stats[team] = {"wins": 0, "draws": 0, "losses": 0}

            # Assign current stats before the match
            data.at[idx, "home_team_wins_so_far"] = team_stats[home]["wins"]
            data.at[idx, "home_team_draws_so_far"] = team_stats[home]["draws"]
            data.at[idx, "home_team_losses_so_far"] = team_stats[home]["losses"]

            data.at[idx, "guest_team_wins_so_far"] = team_stats[guest]["wins"]
            data.at[idx, "guest_team_draws_so_far"] = team_stats[guest]["draws"]
            data.at[idx, "guest_team_losses_so_far"] = team_stats[guest]["losses"]

            # Update stats **after** assigning
            if pd.notna(result):
                if result == "home":
                    team_stats[home]["wins"] += 1
                    team_stats[guest]["losses"] += 1
                elif result == "guest":
                    team_stats[guest]["wins"] += 1
                    team_stats[home]["losses"] += 1
                elif result == "draw":
                    team_stats[home]["draws"] += 1
                    team_stats[guest]["draws"] += 1

    # Compute percentages after all stats are collected
    for prefix in ["home", "guest"]:
        wins = data[f"{prefix}_team_wins_so_far"]
        draws = data[f"{prefix}_team_draws_so_far"]
        losses = data[f"{prefix}_team_losses_so_far"]
        total = wins + draws + losses

        # Avoid NaNs by replacing 0 total matches with 0.0
        data[f"{prefix}_team_wins_pct_so_far"] = (wins / total).fillna(0.0)
        data[f"{prefix}_team_draws_pct_so_far"] = (draws / total).fillna(0.0)
        data[f"{prefix}_team_losses_pct_so_far"] = (losses / total).fillna(0.0)

    data.to_excel("experiment/matches_with_history.xlsx", index=False)
    return data


def feature_engineering(
    read_path: str = "data/matches.parquet",
    save_path: str = "data/matches_feature_engineered.parquet",
):
    df = pd.read_parquet(read_path)
    df = add_is_weekend(df, date_col="match_date")
    df = add_match_period(df, date_col="match_date")
    df = add_day_of_week(df, date_col="match_date")
    df = add_history_last_five_matches_each_team(df=df)
    df = add_current_position_in_season_optimized(df=df)
    df = add_wins_draws_losses_in_season(df=df)
    df.to_parquet(save_path, index=False)
    return df


if __name__ == "__main__":
    df = feature_engineering(read_path="data/matches.parquet")
