import asyncio
from db_connection import init_db, add_pending_url
from concurrency import run_crawler

if __name__ == "__main__":
    init_db()
    
    # Initial URLs
    start_urls = [
        "https://astian.org",
        "https://reddit.com",
        "https://amazon.com",
        "https://www.python.org",
        "https://www.wikipedia.org",
        "https://www.github.com",
        "https://www.ebay.com",
        "https://www.gitlab.com",
        "https://www.laravel.com",
        "https://www.microsoft.com",
        "https://www.facebook.com",
        "https://www.x.com",
        "https://www.tiktok.com",
        "https://www.instagram.com"
        "https://www.mozilla.com",
        "https://www.gitee.com"
    ]

    # Add initial URLs to the queue
    for url in start_urls:
        add_pending_url(url)

    # Running the asynchronous crawler
    asyncio.run(run_crawler(max_concurrent_tasks=10))