from concurrent.futures import ThreadPoolExecutor, as_completed
from db_connection import get_next_pending_url, remove_pending_url, add_pending_url, save_page_data
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import hashlib
import time

# Calculate the hash of the page content
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# Download and process the web page
def fetch_url(url):
    try:
        response = requests.get(url, timeout=10, headers={'User-Agent': 'AstianBot'})
        if response.status_code == 200:
            return response.text
        else:
            print(f"Error {response.status_code} accessing {url}")
    except requests.RequestException as e:
        print(f"Error accessing {url}: {e}")
    return None

# Processing and saving a web page
def process_page(url):
    print(f"Processing {url}...")
    html_content = fetch_url(url)
    if not html_content:
        return []

    # Analyze page content
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.string if soup.title else "Sin título"
    content_hash = calculate_hash(html_content)

    # Determine whether it is internal or external
    parsed_url = urlparse(url)
    base_domain = parsed_url.netloc

    # Save the page in the database
    save_page_data(url, title, content_hash, is_external=False)

    # Extract links
    links = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])
        parsed_link = urlparse(full_url)
        if parsed_link.scheme in ['http', 'https']:
            is_external = parsed_link.netloc != base_domain
            links.add((full_url, is_external))
    return links

# Processing a URL
def process_pending_url(url_id, url):
    links = process_page(url)
    remove_pending_url(url_id)
    for link, is_external in links:
        add_pending_url(link)
        save_page_data(link, None, None, is_external)

# Function to handle concurrency
def run_concurrent_crawling(max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            futures = []
            for _ in range(max_workers):
                pending_url = get_next_pending_url()
                if not pending_url:
                    break
                url_id, url = pending_url
                futures.append(executor.submit(process_pending_url, url_id, url))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing a URL: {e}")

            if not futures:
                print("There are no more URLs pending. Ending...")
                break