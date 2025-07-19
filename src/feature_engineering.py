import pandas as pd
from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class FeatureConfig:
    date_col: str = "match_date"
    home_team_col: str = "home_team"
    guest_team_col: str = "guest_team"
    score_home_col: str = "score_home_team"
    score_guest_col: str = "score_guest_team"
    winning_team_col: str = "winning_team"
    output_path: str = "data/matches_feature_engineered.parquet"


class MatchFeatureEngineer:
    def __init__(self, config: FeatureConfig = FeatureConfig()):
        self.config = config

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """Validate required columns exist in dataframe."""
        required_cols = [
            self.config.date_col,
            self.config.home_team_col,
            self.config.guest_team_col,
            self.config.score_home_col,
            self.config.score_guest_col,
            self.config.winning_team_col,
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")

    def _calculate_team_stats_last_n(
        self,
        df: pd.DataFrame,
        team_name: str,
        match_date: pd.Timestamp,
        n_matches: int = 5,
    ) -> Dict[str, float]:
        """Calculate team statistics from last n matches before given date."""
        past_matches = (
            df[
                (
                    (df[self.config.home_team_col] == team_name)
                    | (df[self.config.guest_team_col] == team_name)
                )
                & (df[self.config.date_col] < match_date)
            ]
            .sort_values(self.config.date_col, ascending=False)
            .head(n_matches)
        )

        stats = {
            "wins": 0,
            "draws": 0,
            "loses": 0,
            "goals_scored": 0,
            "goals_conceded": 0,
        }

        for _, match in past_matches.iterrows():
            is_home = match[self.config.home_team_col] == team_name

            # Goals calculation
            if is_home:
                stats["goals_scored"] += match[self.config.score_home_col]
                stats["goals_conceded"] += match[self.config.score_guest_col]
            else:
                stats["goals_scored"] += match[self.config.score_guest_col]
                stats["goals_conceded"] += match[self.config.score_home_col]

            # Match result calculation
            if match[self.config.winning_team_col] == "draw":
                stats["draws"] += 1
            elif (match[self.config.winning_team_col] == "home" and is_home) or (
                match[self.config.winning_team_col] == "guest" and not is_home
            ):
                stats["wins"] += 1
            else:
                stats["loses"] += 1

        stats["goal_difference"] = stats["goals_scored"] - stats["goals_conceded"]
        return stats

    def add_recent_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add features about team performance in last 5 matches."""
        df = df.copy().sort_values(self.config.date_col).reset_index(drop=True)

        # Initialize columns
        for team_type in ["home_team", "guest_team"]:
            for result in ["wins", "draws", "loses"]:
                df[f"{team_type}_{result}_last_5"] = 0
            df[f"{team_type}_goals_scored_last_5"] = 0
            df[f"{team_type}_goals_conceded_last_5"] = 0
            df[f"{team_type}_goal_difference_last_5"] = 0

        # Calculate stats for each match
        for idx, row in df.iterrows():
            for team_col, team_name in [
                (self.config.home_team_col, row[self.config.home_team_col]),
                (self.config.guest_team_col, row[self.config.guest_team_col]),
            ]:
                stats = self._calculate_team_stats_last_n(
                    df, team_name, row[self.config.date_col]
                )

                prefix = (
                    "home_team"
                    if team_col == self.config.home_team_col
                    else "guest_team"
                )

                df.at[idx, f"{prefix}_wins_last_5"] = stats["wins"]
                df.at[idx, f"{prefix}_draws_last_5"] = stats["draws"]
                df.at[idx, f"{prefix}_loses_last_5"] = stats["loses"]
                df.at[idx, f"{prefix}_goals_scored_last_5"] = stats["goals_scored"]
                df.at[idx, f"{prefix}_goals_conceded_last_5"] = stats["goals_conceded"]
                df.at[idx, f"{prefix}_goal_difference_last_5"] = stats[
                    "goal_difference"
                ]

        return df

    def _update_standings(
        self,
        standings: Dict[str, Dict[str, int]],
        home_team: str,
        guest_team: str,
        home_goals: int,
        guest_goals: int,
        result: str,
    ) -> None:
        """Update league standings based on match result."""
        for team, goals_for, goals_against in [
            (home_team, home_goals, guest_goals),
            (guest_team, guest_goals, home_goals),
        ]:
            if team not in standings:
                standings[team] = {
                    "points": 0,
                    "goals_scored": 0,
                    "goals_conceded": 0,
                }

            standings[team]["goals_scored"] += goals_for
            standings[team]["goals_conceded"] += goals_against

            if result == "draw":
                standings[team]["points"] += 1
            elif (result == "home" and team == home_team) or (
                result == "guest" and team == guest_team
            ):
                standings[team]["points"] += 3

    def add_season_position_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add features about current position in the season."""
        df = df.copy()
        df["year"] = pd.to_datetime(df[self.config.date_col]).dt.year
        df = df.sort_values(self.config.date_col).reset_index(drop=True)

        # Initialize columns
        for prefix in ["home_team", "guest_team"]:
            df[f"{prefix}_current_position"] = None
            df[f"{prefix}_goals_scored"] = 0
            df[f"{prefix}_goals_conceded"] = 0
            df[f"{prefix}_goal_difference"] = 0

        standings = {}
        current_year = None

        for idx, row in df.iterrows():
            year = row["year"]

            # Reset standings for new year
            if year != current_year:
                standings = {}
                current_year = year

            home = row[self.config.home_team_col]
            guest = row[self.config.guest_team_col]

            # Calculate current standings before this match
            table = pd.DataFrame.from_dict(standings, orient="index")
            if not table.empty:
                table = table.reset_index()
                table.columns = ["team", "points", "goals_scored", "goals_conceded"]
                table["goal_difference"] = (
                    table["goals_scored"] - table["goals_conceded"]
                )

                table = table.sort_values(
                    by=["points", "goal_difference", "goals_scored"],
                    ascending=[False, False, False],
                ).reset_index(drop=True)
                table["position"] = table.index + 1

                # Assign current positions
                for team, prefix in [(home, "home_team"), (guest, "guest_team")]:
                    if team in table["team"].values:
                        team_stats = table[table["team"] == team].iloc[0]
                        df.at[idx, f"{prefix}_current_position"] = team_stats[
                            "position"
                        ]
                        df.at[idx, f"{prefix}_goals_scored"] = team_stats[
                            "goals_scored"
                        ]
                        df.at[idx, f"{prefix}_goals_conceded"] = team_stats[
                            "goals_conceded"
                        ]
                        df.at[idx, f"{prefix}_goal_difference"] = team_stats[
                            "goal_difference"
                        ]

            # Update standings after match (if scores are valid)
            if pd.notna(row[self.config.score_home_col]) and pd.notna(
                row[self.config.score_guest_col]
            ):
                self._update_standings(
                    standings,
                    home,
                    guest,
                    row[self.config.score_home_col],
                    row[self.config.score_guest_col],
                    row[self.config.winning_team_col],
                )

        # Calculate position and goal differences
        df["home_team_position_difference"] = (
            df["home_team_current_position"] - df["guest_team_current_position"]
        )
        df["home_team_goal_scored_difference"] = (
            df["home_team_goals_scored"] - df["guest_team_goals_scored"]
        )
        df["home_team_goal_conceded_difference"] = (
            df["home_team_goals_conceded"] - df["guest_team_goals_conceded"]
        )

        return df

    def add_season_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add features about team performance in current season."""
        df = df.copy()
        df["year"] = pd.to_datetime(df[self.config.date_col]).dt.year
        df = df.sort_values(self.config.date_col).reset_index(drop=True)

        # Initialize columns
        for prefix in ["home_team", "guest_team"]:
            for result in ["wins", "draws", "losses"]:
                df[f"{prefix}_{result}_so_far"] = 0

        # Track season stats per team
        team_stats = {}
        current_year = None

        for idx, row in df.iterrows():
            year = row["year"]

            # Reset stats for new year
            if year != current_year:
                team_stats = {}
                current_year = year

            home = row[self.config.home_team_col]
            guest = row[self.config.guest_team_col]
            result = row[self.config.winning_team_col]

            # Initialize team records if needed
            for team in [home, guest]:
                if team not in team_stats:
                    team_stats[team] = {"wins": 0, "draws": 0, "losses": 0}

            # Assign current stats before the match
            for team, prefix in [(home, "home_team"), (guest, "guest_team")]:
                df.at[idx, f"{prefix}_wins_so_far"] = team_stats[team]["wins"]
                df.at[idx, f"{prefix}_draws_so_far"] = team_stats[team]["draws"]
                df.at[idx, f"{prefix}_losses_so_far"] = team_stats[team]["losses"]

            # Update stats after the match (if result is valid)
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

        # Calculate percentages
        for prefix in ["home_team", "guest_team"]:
            wins = df[f"{prefix}_wins_so_far"]
            draws = df[f"{prefix}_draws_so_far"]
            losses = df[f"{prefix}_losses_so_far"]
            total = wins + draws + losses

            # Avoid division by zero
            mask = total > 0
            df.loc[mask, f"{prefix}_wins_pct_so_far"] = wins[mask] / total[mask]
            df.loc[mask, f"{prefix}_draws_pct_so_far"] = draws[mask] / total[mask]
            df.loc[mask, f"{prefix}_losses_pct_so_far"] = losses[mask] / total[mask]

            # Fill NaN values (where total=0)
            df[f"{prefix}_wins_pct_so_far"] = df[f"{prefix}_wins_pct_so_far"].fillna(
                0.0
            )
            df[f"{prefix}_draws_pct_so_far"] = df[f"{prefix}_draws_pct_so_far"].fillna(
                0.0
            )
            df[f"{prefix}_losses_pct_so_far"] = df[
                f"{prefix}_losses_pct_so_far"
            ].fillna(0.0)

        # Calculate differences
        df["wins_difference"] = (
            df["home_team_wins_so_far"] - df["guest_team_wins_so_far"]
        )
        df["draws_difference"] = (
            df["home_team_draws_so_far"] - df["guest_team_draws_so_far"]
        )
        df["losses_difference"] = (
            df["home_team_losses_so_far"] - df["guest_team_losses_so_far"]
        )

        return df

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the complete feature engineering pipeline."""
        self._validate_dataframe(df)

        df = self.add_recent_performance_features(df)
        df = self.add_season_position_features(df)
        df = self.add_season_performance_features(df)

        return df

    def run_pipeline(
        self, read_path: str = "data/matches.parquet", save_path: Optional[str] = None
    ) -> pd.DataFrame:
        """Run complete pipeline from reading data to saving results."""
        df = pd.read_parquet(read_path)
        df = self.process(df)

        if save_path is None:
            save_path = self.config.output_path

        df.to_parquet(save_path, index=False)
        return df


if __name__ == "__main__":
    feature_engineer = MatchFeatureEngineer()
    df = feature_engineer.run_pipeline(read_path="data/matches.parquet")
