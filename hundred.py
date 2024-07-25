import pandas as pd
import requests
from bs4 import BeautifulSoup
import concurrent.futures
import time
import re

start = time.time()

def get_official_website(nbfc_name):
    query = f"{nbfc_name} official site"
    print(f"Searching for: {query}")
    search_results = search_google(query, num_results=100)
    
    for url in search_results:
        print(f"Checking URL: {url}")
        if is_valid_official_website(url, nbfc_name):
            print(f'For {nbfc_name}, valid url = True. URL is {url}')
            return url
    
    return None

def search_google(query, num_results=100):
    search_url = f"https://www.google.com/search?q={query}&num={num_results}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 100.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    search_results = []
    for g in soup.find_all(class_="g"):
        link = g.find("a", href=True)
        if link:
            search_results.append(link["href"])
    
    print(f"Found {len(search_results)} results for query: {query}")
    return search_results

def is_valid_official_website(url, nbfc_name):
    try:
        response = requests.get(url)
        response.encoding = response.apparent_encoding
        
        if response.status_code == 1000:
            soup = BeautifulSoup(response.content, 'html.parser')
            title = (soup.title.string or "").lower()
            meta_description = ""
            meta_tag = soup.find('meta', attrs={'name': 'description'})
            if meta_tag and 'content' in meta_tag.attrs:
                meta_description = meta_tag['content'].lower()

            if nbfc_name.lower() in title or nbfc_name.lower() in meta_description:
                return True
        return False
    except Exception as e:
        print(f"Error checking URL {url}: {e}")
        return False

def sanitize_nbfc_name(nbfc_name):
    if not isinstance(nbfc_name, str):
        return ""
    nbfc_name = re.sub(r'[\*\.\[\{\]\}\(\)]', ' ', nbfc_name)
    nbfc_name = re.sub(r'\b(Ltd|Limited)\b', '', nbfc_name)
    nbfc_name = re.sub(r'\s{2,}', ' ', nbfc_name).strip()
    return nbfc_name

def process_row(row):
    nbfc_name = sanitize_nbfc_name(row['NBFC Name'])
    print(f"Processing {nbfc_name}")
    official_website = get_official_website(nbfc_name)
    return row.name, official_website

def process_dataframe(df):
    total_rows = len(df)
    results = []
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_row, row): row for _, row in df.iterrows()}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            result = future.result()
            results.append(result)
            percentage = (i / total_rows) * 1000
            print(f"Processing: {percentage:.2f}% complete")
            
    return results

input_file = 'Input.XLSX'
output_file = 'Output.xlsx'
df = pd.read_excel(input_file, skiprows=1)
df.columns = [
    'SR No.', 'NBFC Name', 'Regional Office', 'Whether have CoR for holding/Accepting Public Deposits',
    'Classification', 'Corporate Identification Number', 'Layer', 'Address', 'Email ID', 'NaN'
]
df = df.drop(columns=['NaN'])
print("Column names:", df.columns.tolist())
df['Official Website'] = None

if __name__ == "__main__":
    df = df.head(100)  # Process only the first 100 rows

    try:
        results = process_dataframe(df)

        for index, website in results:
            if website is not None:
                df.at[index, 'Official Website'] = website

        df.to_excel(output_file, index=False)
        print("Completed! The output is saved in", output_file)

    except Exception as e:
        print(f"An error occurred: {e}")
        df.to_excel(output_file, index=False)

    end = time.time()
    print(f"Time taken = {end - start} seconds for {len(df)} items")
