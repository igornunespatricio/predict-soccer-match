from datetime import datetime
from tinydb import TinyDB
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware
from pathlib import Path
import json


class UTF8JSONStorage(JSONStorage):
    def __init__(self, path):
        self._handle = path

    def read(self):
        try:
            with open(self._handle, "r", encoding="utf-8") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def write(self, data):
        with open(self._handle, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def close(self):
        pass


def get_db(path: str = "data/matches_db.json"):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # db = TinyDB(path, storage=CachingMiddleware(UTF8JSONStorage))  # Use caching middleware
    db = TinyDB(path, storage=UTF8JSONStorage)  # Don't use caching middleware
    return db, db.table("matches")


def insert_match(db_table, match: dict):
    match_with_timestamp = {
        **match,
        "date_added": datetime.now().isoformat(),  # Local time in ISO 8601 format
    }
    db_table.insert(match_with_timestamp)


# Example usage
if __name__ == "__main__":
    db, matches_table = get_db()

    insert_match(
        matches_table,
        {
            "round": "Rodada 1",
            "date": "13/04/2024 18:30",
            "home_team": "Criciúma",
            "score": "1 x 1",
            "guest_team": "Juventude",
            "stadium": "Heriberto Hülse",
        },
    )

    db.close()
