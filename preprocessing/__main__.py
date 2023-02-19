import glob
import os
from pathlib import Path

import pandas as pd
import typer
from tqdm import tqdm

from . import preprocessing

app = typer.Typer()


@app.command(help="Preprocess data")
def preprocess_data(
    data_path: Path = typer.Option(
        ..., help="path to folder with pickle chunks"
    ),
    output_path: Path = typer.Option(
        ..., help="path to folder to save processed data"
    ),
):
    os.makedirs(output_path, exist_ok=True)

    for data in tqdm(glob.glob(os.path.join(data_path, "*"))):
        # if (
        #     len(
        #         glob.glob(
        #             os.path.join(output_path, f"{os.path.basename(data)}_*")
        #         )
        #     )
        #     > 0
        # ):
        #     continue
        if not os.path.exists(
            os.path.join(output_path, f"{os.path.basename(data)}_podcasts.pkl")
        ) and not os.path.exists(
            os.path.join(output_path, f"{os.path.basename(data)}_episodes.pkl")
        ):

            df = pd.read_json(data, lines=True)
            podcasts, episodes = preprocessing.split_data(df)
            del df
            preprocessing.filter_podcasts(podcasts)
            podcasts.to_pickle(
                os.path.join(
                    output_path, f"{os.path.basename(data)}_podcasts.pkl"
                )
            )

            preprocessing.filter_episodes(episodes)
            episodes.to_pickle(
                os.path.join(
                    output_path, f"{os.path.basename(data)}_episodes.pkl"
                )
            )

            del podcasts, episodes


def main():
    try:
        app(prog_name="preprocess_data")
    except KeyboardInterrupt:
        print("command canceled by user")


if __name__ == "__main__":
    main()
