import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import os
import glob
from pathlib import Path
import re


def get_files_to_download(url, timestamp) -> list[str]:

    # Get files that is last modified at <timestamp>
    result = []

    # Get the HTML content of the url
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    # Access the last modified time of each file
    rows = soup.find_all('tr')
    for row in tqdm(rows):
        cols = row.find_all('td')

        # If the number of 'td's is not 4 (standard), move to the next row
        if len(cols) != 4:
            print(len(cols))
            continue

        last_modified = cols[1].text.strip()
        if last_modified == timestamp:
            file_name = cols[0].text.strip()
            result.append(file_name)

    return result


def make_uris_from_file_names(url, file_names):
    for idx, file_name in enumerate(file_names):
        file_names[idx] = url + file_name
    
    return file_names


def create_new_directory(path) -> bool:

    # Create a new directory if it doesn't exist
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'\'{path}\' created successfully.')
        return True
    
    print(f'\'{path}\' already existed.')
    return False


def download_from_uri(uri, save_dir) -> bool:

    file_name = uri.split('/')[-1].split('.')[0]
    save_path = f'./{save_dir}/{file_name}.csv'

    # Check if the file existed or not
    if os.path.isfile(save_path):
        print('File already exist.')
        return False
    
    print('\nFetching the file...')
    response = requests.get(uri)

    # Check if the uri is valid
    if response.status_code != 200:
        print('The URI is not valid.')
        return False

    with open(save_path, 'wb') as f:
        f.write(response.content)
    print('File successfully downloaded.')
    return True


def download_from_uris(uris):
    save_dir = 'downloads'
    for uri in tqdm(uris):
        download_from_uri(uri=uri, save_dir=save_dir)


def write_concatenated_file_from_source(file_name, source, destination) -> Path:
    dfs = []
    csv_files = Path('./').glob(f'{source}/*.csv')

    for file in tqdm(csv_files):
        data = pd.read_csv(file)
        # .stem is method for pathlib objects to get the filename w/o the extension
        data['file'] = file.stem
        dfs.append(data)

    df = pd.concat(dfs, ignore_index=True)
    df.to_csv(f'{destination}/{file_name}', index=False)
    return Path(f'{destination}/{file_name}')


def get_record_with_max_required_col(in_file, required_col, destination, out_file):

    df = pd.read_csv(in_file)
    numeric_required_data = pd.to_numeric(df[required_col], errors='coerce')
    record_with_max_required_data = df.iloc[numeric_required_data.idxmax()]
    record_with_max_required_data.to_csv(f'{destination}/{out_file}', index=True)


def main():
    
    timestamp = '2024-01-19 10:45'
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    required_col = 'HourlyDryBulbTemperature'
    concatenated_file_name = 'concatenated.csv'
    source_dir = './downloads'
    destination_dir = './data'
    result_file_name = 'result.csv'

    
    files_to_download = get_files_to_download(url=url, timestamp=timestamp)
    create_new_directory('./downloads')

    uris = make_uris_from_file_names(url=url, file_names=files_to_download)
    download_from_uris(uris=uris)

    create_new_directory('./data')
    file_path = write_concatenated_file_from_source(
        file_name=concatenated_file_name, 
        source=source_dir, 
        destination=destination_dir
    )

    get_record_with_max_required_col(
        in_file=file_path,
        required_col=required_col, 
        destination=destination_dir, 
        out_file=result_file_name
    )
    
    pass


if __name__ == "__main__":
    main()
