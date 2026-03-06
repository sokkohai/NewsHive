import pytest
from unittest.mock import Mock, patch

from src.discovery import WebDiscoverer


@pytest.mark.parametrize("paginated_url", [
    "https://www.schulte-lawyers.com/schulteblog?offset=1736870518439",
    "https://www.schulte-lawyers.com/schulteblog/?offset=42",
    "https://www.schulte-lawyers.com/schulteblog/page/2/",
    "https://www.schulte-lawyers.com/schulteblog?start=20",
])
def test_local_discovery_skips_paginated_links(paginated_url):
    web_discoverer = WebDiscoverer()
    base_url = "https://www.schulte-lawyers.com/schulteblog/"

    html = f"""
    <html>
      <body>
        <a href=\"{base_url}new-article\">Fresh Article</a>
        <a href=\"{paginated_url}\">Older Posts</a>
      </body>
    </html>
    """

    mock_response = Mock(status_code=200, text=html)

    with patch("requests.get", return_value=mock_response):
        articles, stopped = web_discoverer._discover_with_local(base_url)

    assert articles is not None
    assert stopped is False
    urls = [url for url, _, _ in articles]
    assert base_url + "new-article" in urls
    assert paginated_url not in urls
    assert len(urls) == 1
