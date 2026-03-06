#!/usr/bin/env python3
"""Quick script to check tagesschau RSS feed URLs"""

import feedparser
import requests
from collections import defaultdict
from urllib.parse import urlparse

feed_url = "https://www.tagesschau.de/wirtschaft/index~rss2.xml"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

print(f"Fetching RSS feed: {feed_url}\n")

response = requests.get(feed_url, headers=headers, timeout=10)
feed = feedparser.parse(response.content)

print(f"Found {len(feed.entries)} entries\n")

# Group URLs by their path patterns
path_patterns = defaultdict(list)

for i, entry in enumerate(feed.entries[:20], 1):
    url = entry.get("link", "")
    title = entry.get("title", "")[:60]
    
    if url:
        parsed = urlparse(url)
        path = parsed.path
        # Extract main path segment
        parts = path.split('/')
        main_segment = parts[2] if len(parts) > 2 else "root"
        
        path_patterns[main_segment].append(url)
        
        print(f"{i}. {title}")
        print(f"   URL: {url}\n")

print("\n" + "="*80)
print("Path segments found:")
for segment in sorted(path_patterns.keys()):
    print(f"  /{segment}/  ({len(path_patterns[segment])} URLs)")
