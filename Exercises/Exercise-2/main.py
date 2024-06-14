import requests
import pandas
from bs4 import BeautifulSoup
from tqdm import tqdm
import os


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


def main():
    timestamp = '2024-01-19 10:45'
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    
    files_to_download = get_files_to_download(url=url, timestamp=timestamp)
    create_new_directory('./downloads')

    uris = make_uris_from_file_names(url=url, file_names=files_to_download)
    print(uris)
    download_from_uris(uris=uris)


    pass


if __name__ == "__main__":
    main()
