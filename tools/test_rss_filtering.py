#!/usr/bin/env python3
"""Check how tagesschau RSS URLs are filtered"""

import feedparser
import requests

feed_url = "https://www.tagesschau.de/wirtschaft/index~rss2.xml"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

response = requests.get(feed_url, headers=headers, timeout=10)
feed = feedparser.parse(response.content)

# Current config from sources.yaml
include_patterns = ["/wirtschaft/", "/inland/", "/investigativ/"]
exclude_patterns = ["/multimedia/", "/sendung/", "/wetter/", "/sport/"]

def matches_pattern(url, pattern):
    """Check if pattern is in URL (simple substring match)"""
    return pattern in url

print("Filtering 69 RSS entries with current config:")
print(f"Include: {include_patterns}")
print(f"Exclude: {exclude_patterns}\n")

passed = []
failed_by_include = []
failed_by_exclude = []

for entry in feed.entries:
    url = entry.get("link", "")
    title = entry.get("title", "")[:50]
    
    if not url:
        continue
    
    # Check include
    include_match = any(matches_pattern(url, pattern) for pattern in include_patterns)
    
    if not include_match:
        failed_by_include.append((url, title))
        continue
    
    # Check exclude
    exclude_match = any(matches_pattern(url, pattern) for pattern in exclude_patterns)
    
    if exclude_match:
        failed_by_exclude.append((url, title))
        continue
    
    passed.append((url, title))

print(f"✅ PASSED: {len(passed)} URLs")
for url, title in passed[:10]:
    print(f"   {title[:40]:40s} {url.split('/')[-1][:40]}")
if len(passed) > 10:
    print(f"   ... and {len(passed)-10} more")

print(f"\n❌ FAILED (include): {len(failed_by_include)} URLs")
for url, title in failed_by_include[:5]:
    print(f"   {title[:40]:40s}")

print(f"\n❌ FAILED (exclude): {len(failed_by_exclude)} URLs")
for url, title in failed_by_exclude[:5]:
    print(f"   {title[:40]:40s}")
