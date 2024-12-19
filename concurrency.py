import asyncio
import aiohttp
from aiohttp import ClientSession, ClientError
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import hashlib
from db_connection import save_page_data, add_pending_url, remove_pending_url, normalize_url, get_next_pending_url, get_db_connection

# Calculate the hash of the page content
def calculate_hash(content):
    return hashlib.sha256(content.encode('utf-8')).hexdigest()

# Downloading the content of a URL using aiohttp
async def fetch_url(url: str, retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=10, allow_redirects=True) as response:
                    if response.status == 200:
                        print(f"[INFO] Downloading: {url}")
                        html = await response.text(encoding=response.charset or "utf-8", errors="replace")
                        return html
                    else:
                        print(f"[ERROR] Error {response.status} accessing {url}")
        except (ClientError, asyncio.TimeoutError) as e:
            print(f"[WARNING] Error accessing {url} (Attempt {attempt+1}/{retries}): {e}")
            await asyncio.sleep(2 ** attempt)  # Exponential Retry
    return None


async def process_page(url: str):
    print(f"[INFO] Processing page: {url}")
    html_content = await fetch_url(url)

    if not html_content:
        print(f"[WARNING] Could not download: {url}")
        return []

    content_hash = calculate_hash(html_content)
    print(f"[INFO] Hash calculated for {url}: {content_hash}")

    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.string.strip() if soup.title and soup.title.string else "Untitled"

    print(f"[INFO] Title extracted for {url}: {title}")
    save_page_data(url, title, content_hash, is_external=False)

    links = set()
    base_domain = urlparse(url).netloc
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])
        normalized_url = normalize_url(full_url)

        if normalized_url and urlparse(normalized_url).scheme in ['http', 'https']:
            is_external = urlparse(normalized_url).netloc != base_domain
            links.add((normalized_url, is_external))

    print(f"[INFO] Links found in {url}: {len(links)}")
    return links


# Process a pending URL
async def process_pending_url(url_id: int, url: str):
    links = await process_page(url)
    remove_pending_url(url_id)

    for link, is_external in links:
        if not url_exists(link):
            add_pending_url(link)
            save_page_data(link, None, None, is_external)


# Check if a URL already exists in websites or pending_urls
def url_exists(url):
    conn = get_db_connection()
    if conn is None:
        return True
    cursor = conn.cursor()
    url = normalize_url(url)
    cursor.execute("""
        SELECT 1 FROM websites WHERE url = %s
        UNION
        SELECT 1 FROM pending_urls WHERE url = %s;
    """, (url, url))
    result = cursor.fetchone()
    conn.close()
    return result is not None


# Running the asynchronous crawler
async def run_crawler(max_concurrent_tasks=10):
    print("[INFO] Starting the asynchronous crawler...")

    while True:
        tasks = []
        for _ in range(max_concurrent_tasks):
            pending_url = get_next_pending_url()
            if not pending_url:
                break
            url_id, url = pending_url
            print(f"[INFO] Adding to processing: {url}")
            tasks.append(process_pending_url(url_id, url))

        if not tasks:
            print("[INFO] There are no more URLs pending. Ending...")
            break

        await asyncio.gather(*tasks)