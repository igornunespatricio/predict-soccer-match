import json
import yaml
import pandas as pd


def read_config(path: str = "config.yml") -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    config = convert_config(config)
    return config


def convert_config(config: dict) -> dict:
    for key, value in config.items():
        if value == "all":
            config[key] = list(range(1, 39))
        else:
            config[key] = [int(x) for x in value.split(",")]
    return config


# def save_json(data: dict, filename: str):
#     with open(filename, "w", encoding="utf-8") as f:
#         json.dump(data, f, ensure_ascii=False, indent=4)


# def read_json(path: str) -> dict:
#     with open(path, "r", encoding="utf-8") as f:
#         data = json.load(f)
#     return data


# def save_parquet(df: pd.DataFrame, output_path: str):
#     df.to_parquet(output_path, index=False)


# def read_parquet(path: str) -> pd.DataFrame:
#     df = pd.read_parquet(path)
#     return df


# if __name__ == "__main__":
#     df = read_parquet("data/matches.parquet")
#     print(df.head())
