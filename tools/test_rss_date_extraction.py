#!/usr/bin/env python3
"""Analyse RSS feeds from config.json and determine best date extraction strategy."""

import sys
import re
import logging
from datetime import datetime, timezone

sys.path.insert(0, str(__file__).rsplit("tools", 1)[0])

import feedparser
import requests
import random

logging.basicConfig(level=logging.WARNING)

from src.discovery import _normalize_discovered_date, _extract_date_from_url

FEEDS = [
    {"name": "LTO",                  "url": "https://www.lto.de/rss/feed.xml"},
    {"name": "Handelsblatt",         "url": "https://www.handelsblatt.com/contentexport/feed/schlagzeilen"},
    {"name": "Tagesschau",           "url": "https://www.tagesschau.de/wirtschaft/index~rss2.xml"},
    {"name": "Manager Magazin",      "url": "https://www.manager-magazin.de/news/index.rss"},
    {"name": "IDW",                  "url": "https://www.idw.de/idw/idw-aktuell/index.xml"},
    {"name": "Beck Aktuell",         "url": "https://rsw.beck.de/feeds/beck-aktuell-nachrichten"},
    {"name": "BAFA Bundesamt",       "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_bundesamt.xml?nn=1468854"},
    {"name": "BAFA Außenwirtschaft", "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_aussenwirtschaft.xml?nn=1468854"},
    {"name": "BAFA Energie",         "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_energie.xml?nn=1468854"},
    {"name": "BAFA Wirtschaft",      "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_wirtschaft.xml?nn=1468854"},
    {"name": "BAFA Lieferketten",    "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_lieferketten.xml?nn=1468854"},
    {"name": "BVerfG",               "url": "https://www.bundesverfassungsgericht.de/SiteGlobals/Functions/RSSFeed/DE/RSSNewsticker/RSSNewsticker_Presse.xml"},
    {"name": "BGH",                  "url": "https://www.bundesgerichtshof.de/DE/Service/RSSFeed/Function/RSS_PM.xml"},
    {"name": "Buzer",                "url": "https://www.buzer.de/gesetze_feed.xml"},
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
}

def check_feed(name: str, url: str) -> dict:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return {"name": name, "error": f"HTTP {resp.status_code}"}
        feed = feedparser.parse(resp.content)
        if not feed.entries:
            return {"name": name, "error": "No entries"}

        sample = feed.entries[:5]
        feed_field_hits = 0
        url_pattern_hits = 0
        no_date = 0
        example_url = ""
        example_feed_fields = {}
        
        for entry in sample:
            article_url = entry.get("link", "")
            if not example_url:
                example_url = article_url

            # Check feed fields
            date_from_feed = "unknown"
            for field in ["published_parsed", "updated_parsed", "created_parsed"]:
                if hasattr(entry, field) and getattr(entry, field):
                    try:
                        t = getattr(entry, field)
                        dt = datetime(*t[:6], tzinfo=timezone.utc)
                        date_from_feed = dt.isoformat()
                        break
                    except Exception:
                        pass
            if date_from_feed == "unknown":
                for field in ["published", "updated", "created"]:
                    val = entry.get(field, "")
                    if val:
                        normalized = _normalize_discovered_date(val)
                        if normalized != "unknown":
                            date_from_feed = normalized
                            break

            # Collect which feed fields exist for inspection
            for field in ["published", "updated", "created", "published_parsed"]:
                val = entry.get(field) or getattr(entry, field, None)
                if val and field not in example_feed_fields:
                    example_feed_fields[field] = str(val)[:60]

            # Check URL pattern
            date_from_url = _extract_date_from_url(article_url)

            if date_from_feed != "unknown":
                feed_field_hits += 1
            elif date_from_url != "unknown":
                url_pattern_hits += 1
            else:
                no_date += 1

        n = len(sample)
        return {
            "name": name,
            "entries": len(feed.entries),
            "feed_fields": f"{feed_field_hits}/{n}",
            "url_pattern": f"{url_pattern_hits}/{n}",
            "no_date": f"{no_date}/{n}",
            "recommended": (
                "feed_fields" if feed_field_hits == n
                else "url_pattern" if feed_field_hits == 0 and url_pattern_hits > 0
                else "both"
            ),
            "example_url": example_url,
            "feed_field_examples": example_feed_fields,
        }
    except Exception as e:
        return {"name": name, "error": str(e)}


def main():
    print(f"\n{'='*100}")
    print(f"{'Feed':<22} {'Entries':>7}  {'feed_fields':>11}  {'url_pattern':>11}  {'no_date':>7}  {'Recommended':<14}  Beispiel-URL")
    print(f"{'='*100}")

    recommendations = {}
    for feed in FEEDS:
        r = check_feed(feed["name"], feed["url"])
        if "error" in r:
            print(f"{r['name']:<22}  ERROR: {r['error']}")
            continue
        line = (
            f"{r['name']:<22} {r['entries']:>7}  "
            f"{r['feed_fields']:>11}  {r['url_pattern']:>11}  {r['no_date']:>7}  "
            f"{r['recommended']:<14}  {r['example_url'][:60]}"
        )
        print(line)
        if r.get("feed_field_examples"):
            for k, v in r["feed_field_examples"].items():
                print(f"  {'':22}  field '{k}': {v}")
        recommendations[r["name"]] = r["recommended"]

    print(f"\n{'='*100}")
    print("EMPFOHLENE CONFIG-EINSTELLUNGEN:")
    for name, rec in recommendations.items():
        if rec != "both":
            print(f"  {name:<22}: rss_date_extraction = \"{rec}\"")
        else:
            print(f"  {name:<22}: (default 'both' - ok)")


if __name__ == "__main__":
    sys.exit(main())


import sys
import logging
from datetime import datetime, timezone

# Add src directory to path
sys.path.insert(0, str(__file__).replace("tools\\test_rss_date_extraction.py", ""))

import feedparser
import requests
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from src.discovery import _normalize_discovered_date, _extract_date_from_url

# Test RSS feeds from config
TEST_FEEDS = [
    {
        "name": "Buzer.de",
        "url": "https://www.buzer.de/gesetze_feed.xml",
        "rss_date_extraction": "feed_fields",
    },
    {
        "name": "BAFA - Allgemeine Nachrichten",
        "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_bundesamt.xml?nn=1468854",
        "rss_date_extraction": "url_pattern",
    },
    {
        "name": "BAFA - Außenwirtschaft",
        "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_aussenwirtschaft.xml?nn=1468854",
        "rss_date_extraction": "url_pattern",
    },
    {
        "name": "BAFA - Energie",
        "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_energie.xml?nn=1468854",
        "rss_date_extraction": "url_pattern",
    },
    {
        "name": "BAFA - Wirtschaft",
        "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_wirtschaft.xml?nn=1468854",
        "rss_date_extraction": "url_pattern",
    },
    {
        "name": "BAFA - Lieferketten",
        "url": "https://www.bafa.de/DE/Service/RSSNewsfeed/_functions/rssnewsfeed_lieferketten.xml?nn=1468854",
        "rss_date_extraction": "url_pattern",
    },
]


def test_rss_feed(feed_name: str, feed_url: str, rss_date_extraction: str = "both") -> bool:
    """Test if RSS feed can be parsed and dates extracted."""
    logger.info(f"\n{'='*60}")
    logger.info(f"Testing: {feed_name}")
    logger.info(f"URL: {feed_url}")
    logger.info(f"Date Extraction Strategy: {rss_date_extraction}")
    logger.info(f"{'='*60}")
    
    try:
        # Fetch the feed
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        ]
        headers = {
            "User-Agent": random.choice(user_agents),
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml",
        }
        
        logger.info("Fetching feed...")
        response = requests.get(feed_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch feed: HTTP {response.status_code}")
            return False
        
        logger.info(f"✅ Feed fetched successfully ({len(response.content)} bytes)")
        
        # Parse the feed
        logger.info("Parsing feed...")
        feed = feedparser.parse(response.content)
        
        if feed.bozo:
            logger.warning(f"⚠️  Feed parsing issues detected: {feed.bozo_exception}")
        
        if not feed.entries:
            logger.error("❌ Feed has no entries")
            return False
        
        logger.info(f"✅ Found {len(feed.entries)} entries")
        
        # Test date extraction from first 5 entries
        sample_size = min(5, len(feed.entries))
        date_success_count = 0
        
        logger.info(f"\nTesting date extraction from first {sample_size} entries:")
        logger.info("-" * 60)
        
        for i, entry in enumerate(feed.entries[:sample_size], 1):
            logger.info(f"\nEntry {i}:")
            
            # Extract URL and title
            url = entry.get("link", "N/A")
            title = entry.get("title", "N/A")[:50] + ("..." if len(entry.get("title", "")) > 50 else "")
            
            logger.info(f"  Title: {title}")
            logger.info(f"  URL: {url}")
            
            # Try to extract date
            published_date = "unknown"
            date_source = None
            
            # Try various date fields (ordered by priority)
            for date_field in ["published_parsed", "updated_parsed", "created_parsed"]:
                if hasattr(entry, date_field):
                    time_tuple = getattr(entry, date_field)
                    if time_tuple:
                        try:
                            dt = datetime(*time_tuple[:6], tzinfo=timezone.utc)
                            published_date = dt.isoformat().replace("+00:00", "Z")
                            date_source = f"{date_field} (parsed)"
                            break
                        except Exception as e:
                            logger.debug(f"  Failed to parse {date_field}: {e}")
            
            # Try string date fields if parsed dates failed
            if published_date == "unknown":
                for date_field in ["published", "updated", "created"]:
                    if hasattr(entry, date_field):
                        date_str = getattr(entry, date_field)
                        if date_str:
                            published_date = _normalize_discovered_date(date_str)
                            if published_date != "unknown":
                                date_source = f"{date_field} (string, normalized)"
                                break
            
            # Try to extract date from URL as fallback
            if published_date == "unknown" and url:
                published_date = _extract_date_from_url(url)
                if published_date != "unknown":
                    date_source = "URL pattern (YYYYMMDD_)"
            
            if published_date != "unknown":
                logger.info(f"  ✅ Date: {published_date} (from {date_source})")
                date_success_count += 1
            else:
                logger.warning(f"  ⚠️  No date found")
                # Log available date fields for debugging
                for field in ["published", "updated", "created", "published_parsed", "updated_parsed", "created_parsed"]:
                    if hasattr(entry, field):
                        val = getattr(entry, field)
                        if val:
                            logger.debug(f"    Available {field}: {val}")
        
        logger.info("-" * 60)
        logger.info(f"\nDate extraction success rate: {date_success_count}/{sample_size} ({date_success_count*100//sample_size}%)")
        
        if date_success_count > 0:
            logger.info("✅ Feed OK - Dates extracted successfully")
            return True
        else:
            logger.warning("⚠️  Feed parsed but dates could not be extracted")
            return False
            
    except requests.exceptions.Timeout:
        logger.error("❌ Request timeout")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"❌ Connection error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    logger.info("\n" + "="*60)
    logger.info("RSS Feed Date Extraction Test")
    logger.info("="*60)
    
    results = {}
    
    for feed_config in TEST_FEEDS:
        try:
            success = test_rss_feed(
                feed_config["name"],
                feed_config["url"],
                rss_date_extraction=feed_config.get("rss_date_extraction", "both")
            )
            results[feed_config["name"]] = success
        except KeyboardInterrupt:
            logger.info("\n⏸️  Test interrupted by user")
            break
        except Exception as e:
            logger.error(f"Unexpected error testing {feed_config['name']}: {e}")
            results[feed_config["name"]] = False
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)
    
    for feed_name, success in results.items():
        status = "✅ OK" if success else "❌ FAILED"
        logger.info(f"{status}: {feed_name}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    logger.info(f"\nTotal: {passed}/{total} feeds passed")
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
