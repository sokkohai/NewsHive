import json
from types import SimpleNamespace
from unittest.mock import patch

from src.extraction import WebExtractor
from src.pattern_extractors import get_listing_extractor_for_url


SAMPLE_HTML = """
<html>
  <body>
    <h1>Entscheidungen vom 15.05.2026</h1>
    <ul>
      <li>
        <a href="https://nrwe.justiz.nrw.de/fgs/muenster/j2026/5_K_2060_24_U_Urteil_20260225.html">
          5 K 2060/24 U (https://nrwe.justiz.nrw.de/fgs/muenster/j2026/5_K_2060_24_U_Urteil_20260225.html)
        </a>
        <div class="teaser">Umsatzsteuer - Nachweis einer innergemeinschaftlichen Lieferung.</div>
      </li>
      <li>
        <a href="https://nrwe.justiz.nrw.de/fgs/muenster/j2026/8_K_820_24_G_F_Urteil_20260416.html">
          8 K 820/24 G, F
        </a>
        <div class="teaser">Einkommensteuer - Zulassigkeit von Ruecklagen.</div>
      </li>
    </ul>
  </body>
</html>
"""


def test_fg_muenster_resolver_and_extraction():
    url = "https://www.fg-muenster.nrw.de/behoerde/presse/entscheidungen/15_05_2026/index.php"
    extractor = get_listing_extractor_for_url(url)

    assert extractor is not None

    articles = extractor.extract_articles(SAMPLE_HTML, url)
    assert len(articles) == 2
    assert articles[0]["title"] == "5 K 2060/24 U"
    assert articles[0]["date"].startswith("2026-05-15")


def test_web_extractor_expands_fg_muenster_listing_payload():
    extractor = WebExtractor(config=None)
    url = "https://www.fg-muenster.nrw.de/behoerde/presse/entscheidungen/15_05_2026/index.php"

    with patch("src.extraction.requests.get") as mock_get:
        mock_get.return_value = SimpleNamespace(status_code=200, text=SAMPLE_HTML)
        result = extractor.extract(url, fetch_method="static")

    assert result is not None
    payload, method = result
    assert method == "listings"

    data = json.loads(payload)
    assert data["count"] == 2
    assert data["articles"][0]["url"].startswith("https://nrwe.justiz.nrw.de/fgs/muenster/")
