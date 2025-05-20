import pandas as pd

from src.db import load_table_as_dataframe


def test_load_table_as_dataframe():
    df = load_table_as_dataframe("data/matches_db.json")
    assert isinstance(df, pd.DataFrame)


def test_parquet_has_value():
    df = pd.read_parquet("data/matches.parquet")
    assert not df.empty
    print(df.head())


def feature_engineered_table():
    df = pd.read_parquet("data/matches_feature_engineered.parquet")
    print(df.head())
    assert not df.empty


if __name__ == "__main__":
    feature_engineered_table()
