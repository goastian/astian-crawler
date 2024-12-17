import time
import hashlib
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from db_connection import init_db, add_pending_url, get_next_pending_url, remove_pending_url, save_page_data

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
    html_content = fetch_url(url)
    if not html_content:
        return None

    # Analyze page content
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.string if soup.title else "Sin título"
    content_hash = calculate_hash(html_content)

    # Determine whether it is internal or external
    parsed_url = urlparse(url)
    base_domain = parsed_url.netloc

    # Save the page in the database
    save_page_data(url, title, content_hash, is_external=False)

    # Extract links (internal and external)
    links = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])
        parsed_link = urlparse(full_url)

        # Filter http(s) links only
        if parsed_link.scheme in ['http', 'https']:
            is_external = parsed_link.netloc != base_domain
            links.add((full_url, is_external))

    return links


# Function to process a URL from the queue
def process_pending_url(url_id, url):
    link = process_page(url)
    remove_pending_url(url_id) #Mark the URL as processed

    # Add new links to the queue
    for link, is_external in link:
        add_pending_url(link)
        save_page_data(link, None, None, is_external)

# Main crawler
def crawl_web():
    while True:
        # Get the following pending URL
        pending_url = get_next_pending_url()
        if not pending_url:
            print("No more URLs pending. Ending...")
            break

        url_id, url = pending_url
        print(f"Processing {url}...")

        # Process the page
        links = process_page(url)

        # Mark the URL as processed
        remove_pending_url(url_id)

        # Add new links found to the queue
        if links:
            for link, is_external in links:
                add_pending_url(link)
                save_page_data(link, None, None, is_external)

        time.sleep(2)  # Delay entre requests

# Main
if __name__ == "__main__":
    init_db()
    
    # Add initial URL
    start_url = "https://astian.org"
    add_pending_url(start_url)

    # Running the crawler
    crawl_web()