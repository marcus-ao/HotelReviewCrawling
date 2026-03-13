"""Pagination URL helpers for hotel list crawling."""

from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit


def update_url_param(url: str, key: str, value: int) -> str:
    """Update or add a query param in URL."""
    if not url:
        return url

    parts = urlsplit(url)
    query = parse_qs(parts.query, keep_blank_values=True)
    query[key] = [str(value)]
    new_query = urlencode(query, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
