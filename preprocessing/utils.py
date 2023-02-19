from datetime import datetime

import pandas as pd


def timezone_map(x):
    try:
        x = " ".join(x.split(" ")[:4])  # remove timestamp, keep only date
        date_format = "%a, %d %b %Y"
        return datetime.strptime(x, date_format)
    except Exception:
        return None


def get_episodes(x: pd.Series):
    """helper function for getting episodes from data. Mainly used as a lambda function

    Args:
        x (pd.Series): row of dataframe

    Returns:
        _type_: _description_
    """
    id = x["databaseId"]
    df = pd.DataFrame(x["scraped"]["episodes"])
    df["podcastId"] = id
    return df
