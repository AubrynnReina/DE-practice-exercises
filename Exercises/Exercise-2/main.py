import requests
import pandas
from bs4 import BeautifulSoup
from tqdm import tqdm

def get_files_to_download(url, timestamp) -> list[str]:

    # Get files that is last modified at <timestamp>
    result = list[str]

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
            file = cols[0].text.strip()
            result.append(file)

    return result

def main():
    timestamp = '2024-01-19 10:45'
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    
    files_to_download = get_files_to_download(url=url, timestamp=timestamp)
    

    pass


if __name__ == "__main__":
    main()
