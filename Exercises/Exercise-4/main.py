from pathlib import Path
import json
import pandas as pd
from tqdm import tqdm


def flatten_json_files(json_files) -> pd.DataFrame:

    df = pd.DataFrame()
    for file in tqdm(json_files):
        with open(file, 'r+') as f:
            data = json.load(f)
            flatten_json = pd.json_normalize(data)

            # Add the 'file' column for backtracking
            flatten_json['file'] = f'{file.stem}.json'

            df = pd.concat([df, flatten_json], ignore_index=True)

    return df


def main():

    destination_dir = './data'
    output_file = 'result.csv'
    # Get all json files
    json_files = Path('./').rglob('*.json')

    df = flatten_json_files(json_files=json_files)

    # Save to ./data
    df.to_csv(f'{destination_dir}/{output_file}', index=False)

    pass


if __name__ == "__main__":
    main()
