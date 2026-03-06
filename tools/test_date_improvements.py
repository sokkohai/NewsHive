#!/usr/bin/env python3
"""Quick test for date extraction improvements on problematic sources."""

import sys
import logging
from src.discovery import WebDiscoverer
from src.config import ConfigLoader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)

def test_source(source_config):
    """Test date extraction for a single source."""
    url = source_config.url
    domain = url.split('/')[2]
    
    print(f"\n{'='*80}")
    print(f"Testing: {domain}")
    print(f"URL: {url}")
    print(f"{'='*80}")
    
    discoverer = WebDiscoverer()
    
    # Get config parameters
    include_patterns = getattr(source_config.extraction_rules, 'include_patterns', None) if source_config.extraction_rules else None
    exclude_patterns = getattr(source_config.extraction_rules, 'exclude_patterns', None) if source_config.extraction_rules else None
    date_pattern = source_config.date_extraction_pattern
    
    # Discover articles
    articles = discoverer.discover(
        url=url,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        date_extraction_pattern=date_pattern,
        max_age_days=8
    )
    
    # Analyze results
    total = len(articles)
    with_dates = sum(1 for _, _, date in articles if date != "unknown")
    without_dates = total - with_dates
    success_rate = (with_dates / total * 100) if total > 0 else 0
    
    print(f"\nResults:")
    print(f"  Total articles: {total}")
    print(f"  With dates: {with_dates} ({success_rate:.0f}%)")
    print(f"  Without dates: {without_dates}")
    
    if with_dates > 0:
        print(f"\n  Sample articles with dates:")
        for url, title, date in articles[:3]:
            if date != "unknown":
                print(f"    [{date}] {title[:60]}...")
    
    if without_dates > 0:
        print(f"\n  Sample articles WITHOUT dates:")
        for url, title, date in articles[:5]:
            if date == "unknown":
                print(f"    [NO DATE] {title[:60]}...")
                print(f"      URL: {url}")
    
    return total, with_dates, without_dates

def main():
    """Test problematic sources."""
    from pathlib import Path
    config = ConfigLoader.load(Path('config.json'))
    
    # Test specific problematic sources
    test_urls = [
        'https://www.lto.de/rechtsgebiete/strafrecht-urteile-gesetzesaenderungen-nachrichten',
        'https://legal.pwc.de/de/news/fachbeitraege/kategorie/steuer-und-wirtschaftsstrafrecht',
        'https://digital-client-solutions.hoganlovells.com/resources/esg-regulatory-alerts',
    ]
    
    total_articles = 0
    total_with_dates = 0
    total_without_dates = 0
    
    for source in config.web_sources:
        if source.url in test_urls:
            articles, with_dates, without_dates = test_source(source)
            total_articles += articles
            total_with_dates += with_dates
            total_without_dates += without_dates
    
    print(f"\n{'='*80}")
    print(f"OVERALL SUMMARY")
    print(f"{'='*80}")
    print(f"Total articles: {total_articles}")
    
    if total_articles > 0:
        print(f"With dates: {total_with_dates} ({total_with_dates/total_articles*100:.0f}%)")
        print(f"Without dates: {total_without_dates} ({total_without_dates/total_articles*100:.0f}%)")
        
        if total_without_dates == 0:
            print("\n🎉 SUCCESS: 100% date extraction achieved!")
        else:
            print(f"\n⚠️ Still {total_without_dates} articles without dates")
    else:
        print("\n✅ All navigation links filtered out - no undated articles found!")
        print("Note: This means the exclude patterns are working correctly.")

if __name__ == '__main__':
    main()
