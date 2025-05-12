import pandas as pd

import src.utils as utils


def flatten_matches(nested_json):
    """Flattens the nested dictionary structure (year → rounds → matches) into a list of records."""
    records = []
    for year, rounds in nested_json.items():
        for round_key, matches in rounds.items():
            for match in matches:
                records.append(
                    {
                        "year": int(year),
                        "round": match.get("round"),
                        "date": match.get("date"),
                        "home_team": match.get("home_team"),
                        "score": match.get("score"),
                        "guest_team": match.get("guest_team"),
                        "stadium": match.get("stadium"),
                    }
                )
    return pd.DataFrame(records)


def split_score_column(df):
    """Splits the 'score' column into 'score_team1' and 'score_team2' as integers."""
    score_split = df["score"].str.extract(r"(\d+)\s*x\s*(\d+)")
    df["score_home_team"] = score_split[0].astype("Int64")
    df["score_guest_team"] = score_split[1].astype("Int64")
    return df.drop(columns=["score"])


def parse_dates(df):
    """Parses the 'date' column into datetime objects."""
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y %H:%M", errors="coerce")
    return df


def transform_data(path):
    """Main function to transform nested match data into a clean tabular DataFrame."""
    nested_json = utils.read_json(path)
    df = flatten_matches(nested_json)
    df = split_score_column(df)
    df = parse_dates(df)
    utils.save_parquet(df=df, output_path="data/matches.parquet")


if __name__ == "__main__":

    transform_data(path="data/raw_matches.json")
