from concurrent.futures import ThreadPoolExecutor, as_completed
from db_connection import get_next_pending_url, remove_pending_url, add_pending_url, save_page_data, normalize_url
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
import hashlib

# Calculate the hash of the page content
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# Download and process the web page
def fetch_url(url):
    try:
        print(f"[INFO] Downloading: {url}")  # Log: URL being downloaded
        response = requests.get(url, timeout=10, headers={'User-Agent': 'AstianBot'})
        if response.status_code == 200:
            return response.text
        else:
            print(f"[ERROR] Error {response.status_code} accessing {url}")
    except requests.RequestException as e:
        print(f"[ERROR] Error accessing {url}: {e}")
    return None

# Process a web page
def process_page(url):
    print(f"\n[INFO] Processing page: {url}")  # Log principal
    html_content = fetch_url(url)
    if not html_content:
        print(f"[WARNING] Unable to download: {url}")
        return []

    # Analyze page content
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.string if soup.title else "Sin título"
    content_hash = calculate_hash(html_content)

    # Saving the page data in the database
    save_page_data(url, title, content_hash, is_external=False)
    print(f"[SUCCESS] Saved page: {url} (Title: {title})")  # Confirmation of saving

    # Extract and normalize links
    links = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])  # Solve relative links
        normalized_url = normalize_url(full_url)
        links.add(normalized_url)

    print(f"[INFO] Enlaces encontrados en {url}: {len(links)}")  # Link quantity log
    return links

# Process a pending URL
def process_pending_url(url_id, url):
    try:
        links = process_page(url)  # Process the page and obtain links
    finally:
        remove_pending_url(url_id)  # Remove the URL from the queue, even if errors occurred

    for link in links:
        add_pending_url(link)  # Only add unique URLs
        save_page_data(link, None, None, is_external=urlparse(link).netloc != urlparse(url).netloc)

# Running the concurrent crawler
def run_concurrent_crawling(max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            futures = []
            for _ in range(max_workers):
                # Get the following pending URL
                pending_url = get_next_pending_url()
                if not pending_url:
                    break  # No more pending URLs, exit loop
                url_id, url = pending_url
                # Send the task to the thread pool
                print(f"[INFO] Adding to the pool: {url}")  # Log of URLs added to the pool
                futures.append(executor.submit(process_pending_url, url_id, url))

            # Wait for all tasks to be completed
            for future in as_completed(futures):
                try:
                    future.result()  # Lifts exceptions if any
                except Exception as e:
                    print(f"[ERROR] Error processing a URL: {e}")

            # If no more tasks have been sent, finish
            if not futures:
                print("\n[INFO] There are no more URLs pending. Ending...")
                break