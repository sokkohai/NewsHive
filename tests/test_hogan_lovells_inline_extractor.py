from src.pattern_extractors import get_inline_listing_extractor_for_url


SAMPLE_HTML = """
<html>
  <body>
    <div class="article-panel">
      <h3 class="title">EU Sanctions Update</h3>
      <time datetime="2026-06-01T00:00:00Z"></time>
      <button onclick="showNTArticlePopup(12345)">Read</button>
    </div>
    <div class="article-panel">
      <h3 class="title">Investigations Briefing</h3>
      <button onclick="showNTArticlePopup(67890)">Read</button>
    </div>
  </body>
</html>
"""


def test_hogan_lovells_inline_extractor_resolves_and_extracts_candidates():
    url = "https://digital-client-solutions.hoganlovells.com/resources/compliance"
    extractor = get_inline_listing_extractor_for_url(url)

    assert extractor is not None

    candidates = extractor.extract_inline_candidates(SAMPLE_HTML, url)

    assert len(candidates) == 2
    assert candidates[0]["url"].endswith("#12345")
    assert candidates[0]["title"] == "EU Sanctions Update"
    assert candidates[0]["date"] == "2026-06-01T00:00:00Z"
