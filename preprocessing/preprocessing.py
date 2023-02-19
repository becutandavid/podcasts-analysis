import pandas as pd

from . import utils


def split_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """split data into podcasts and episodes

    Args:
        df (pd.DataFrame): loaded df chunk

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: podcasts, episodes
    """
    podcasts = df.apply(lambda x: pd.Series(x["scraped"]["meta"]), axis=1)
    podcasts["id"] = df["databaseId"]

    episodes = df.apply(utils.get_episodes, axis=1)
    episodes = pd.concat(episodes.to_list(), ignore_index=True)

    return podcasts, episodes


def filter_podcasts(podcasts: pd.DataFrame) -> pd.DataFrame:
    """filter podcasts and drop columns

    Args:
        podcasts (pd.DataFrame): podcasts dataframe

    Returns:
        pd.DataFrame: filtered podcasts dataframe
    """
    value_counts = (podcasts.isna().sum() / podcasts.shape[0]).sort_values()
    podcasts.drop(columns=value_counts[value_counts > 0.9].index, inplace=True)
    podcasts.drop(columns=["type", "funding"], inplace=True, errors="ignore")
    podcasts["explicit"] = podcasts["explicit"].astype(bool)


def filter_episodes(episodes: pd.DataFrame) -> pd.DataFrame:
    """filter episodes and drop columns

    Args:
        episodes (pd.DataFrame): episodes dataframe

    Returns:
        pd.DataFrame: filtered episodes dataframe
    """
    value_counts = (episodes.isna().sum() / episodes.shape[0]).sort_values()
    episodes.drop(columns=value_counts[value_counts > 0.9].index, inplace=True)
    episodes.drop(
        columns=["funding", "transcript", "soundbite"],
        inplace=True,
        errors="ignore",
    )
    episodes["explicit"] = episodes["explicit"].astype(bool)

    episodes["pubDate"] = episodes.pubDate.map(utils.timezone_map)
