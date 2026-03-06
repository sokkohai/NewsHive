#!/usr/bin/env python3
"""Check if feedparser detects errors (bozo)"""

import feedparser
import requests

feed_url = "https://www.tagesschau.de/wirtschaft/index~rss2.xml"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

response = requests.get(feed_url, headers=headers, timeout=10)
feed = feedparser.parse(response.content)

print(f"Feed bozo: {feed.bozo}")
if feed.bozo:
    print(f"Bozo exception: {feed.bozo_exception}")

print(f"Feed entries: {len(feed.entries)}")

if feed.bozo:
    print("\nFeed has bozo flag - this would cause code to skip it!")
    print("The code checks: if not feed.entries or feed.bozo: continue")
else:
    print("\nFeed is clean (no bozo flag)")
