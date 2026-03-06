#!/usr/bin/env python3
"""Check RSS feed structure in detail"""

import feedparser
import requests
import json

feed_url = "https://www.tagesschau.de/wirtschaft/index~rss2.xml"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

response = requests.get(feed_url, headers=headers, timeout=10)
feed = feedparser.parse(response.content)

print(f"Feed keys: {feed.keys()}\n")
print("First entry attributes:")
if feed.entries:
    entry = feed.entries[0]
    print(f"  Keys: {entry.keys()}\n")
    
    for key in ["link", "links", "id", "title", "summary"]:
        if key in entry:
            val = entry[key]
            if isinstance(val, list):
                print(f"  {key}: (list with {len(val)} items)")
                if val and hasattr(val[0], 'href'):
                    print(f"           First item href: {val[0].href}")
            else:
                print(f"  {key}: {str(val)[:100]}")

print("\n\nProcessing logic simulation (first 5 entries):")
for i, entry in enumerate(feed.entries[:5], 1):
    # Exact same logic as in discovery.py
    article_url = entry.get("link")
    if not article_url:
        article_url = None
        for link in entry.get("links", []):
            if link.get("rel") == "alternate":
                article_url = link.get("href")
                break
    
    title = entry.get("title", "").strip()
    
    print(f"\n{i}. Title: {title[:50]}")
    print(f"   URL from .get('link'): {article_url is not None}")
    if article_url:
        print(f"   URL: {article_url[:70]}")
