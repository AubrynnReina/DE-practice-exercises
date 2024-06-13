import requests
import os
from tqdm import tqdm
import glob
import zipfile
import shutil


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


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
    save_path = f'./{save_dir}/{file_name}.zip'

    # Check if the file existed or not
    if os.path.isfile(save_path):
        print('File already exist.')
        return False
    
    print('Fetching the file...')
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


def extract_from_zip(zip_file, save_dir) -> bool:

    print(f'\nExtracting {zip_file}...')
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(save_dir)
    return True


def extract_from_zips(zip_files):

    save_dir = './data/'
    for zip_file in tqdm(zip_files):
        extract_from_zip(zip_file=zip_file, save_dir=save_dir)

    shutil.rmtree(f'{save_dir}__MACOSX/')
        

def main():

    # Create './downloads' directory if it doesn't exist
    create_new_directory('./downloads')

    # Create './data' directory if it doesn't exist
    create_new_directory('./data')

    # Download zip files from the uris
    download_from_uris(download_uris)

    all_zip_files = glob.glob('./downloads/*')
    extract_from_zips(all_zip_files)
    
    
    pass


if __name__ == "__main__":
    main()
