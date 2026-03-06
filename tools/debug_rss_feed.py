#!/usr/bin/env python3
"""Debug tagesschau RSS discovery"""

import feedparser
import requests

feed_url = "https://www.tagesschau.de/wirtschaft/index~rss2.xml"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

response = requests.get(feed_url, headers=headers, timeout=10)
feed = feedparser.parse(response.content)

print(f"Feed entries: {len(feed.entries)}")
print(f"Feed bozo: {feed.bozo}\n")

articles = []

for i, entry in enumerate(feed.entries[:20], 1):
    # Extract article URL
    article_url = entry.get("link")
    if not article_url:
        print(f"{i}. ❌ NO LINK")
        continue
    
    # Extract title
    title = entry.get("title", "").strip()
    if not title:
        print(f"{i}. ❌ NO TITLE - URL: {article_url[:50]}")
        continue
    
    if len(title) < 8:
        print(f"{i}. ❌ TITLE SHORT ({len(title)} chars): {title}")
        continue
    
    print(f"{i}. ✅ PASS ({len(title)} chars): {title[:60]}")
    articles.append((article_url, title, "date"))

print(f"\n\nResult: {len(articles)} articles would be added")
