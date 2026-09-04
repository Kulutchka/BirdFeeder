#!/usr/bin/env python3
"""
RSS Feed Discovery Agent

Reads domains from company-domains.txt, discovers RSS/Atom feeds on each domain
via the local SearXNG instance and common-path guessing, validates that each
feed is live and well-formed, generates a name/description/category with the
DeepSeek API, and writes the results to rss-feeds.csv.

Usage:
    pip install -r requirements.txt
    DEEPSEEK_API_KEY=... python3 rss_agent.py [DOMAINS_FILE] [--output CSV]
        [--unsuccessful TXT] [--limit N] [--retry-unsuccessful] [--verbose]

Notable changes from the previous version:
  - Fixed a bug where two different domains whose feeds happened to be
    byte-identical (e.g. empty/boilerplate "no posts yet" templates) had
    the second one silently dropped. It's now recorded with a warning
    instead of lost.
  - Feed links found in a page's HTML are now resolved against the final,
    post-redirect URL rather than the URL that was originally requested.
  - HTML parsing for <link>/<a> tags now uses html.parser.HTMLParser
    instead of hand-rolled regexes, which chokes far less on real-world
    markup (script blocks, odd quoting, self-closing tags, etc).
  - Domains already recorded as "no feeds found" are skipped on repeat
    runs by default; pass --retry-unsuccessful to re-probe them (entries
    that succeed on retry are pruned from the unsuccessful file).
  - GET requests get a couple of automatic retries with backoff on
    transient errors (connection resets, 429/5xx) via a urllib3 Retry
    adapter, and responses are always closed via a context manager.
  - Module-level file paths are no longer mutated via `global`; they're
    threaded through function arguments, which makes the pipeline easier
    to test and reason about.
  - Added --limit (cap domains processed, handy for testing) and
    --verbose (print per-request probe errors) flags.
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import feedparser
except ImportError:
    sys.exit(
        "Missing dependency 'feedparser'. Install requirements with:\n"
        "    pip install -r requirements.txt"
    )

# --- Configuration -----------------------------------------------------------

DOMAINS_FILE = "company-domains.txt"
OUTPUT_FILE = "rss-feeds.csv"
UNSUCCESSFUL_FILE = "unsuccessful.txt"

SEARXNG_URL = "http://karagoz:8080/search"
SEARXNG_QUERIES = [
    "site:{domain} rss",
    "site:{domain} feed",
    "site:{domain} atom",
    "site:{domain} blog rss",
]

# Common feed paths tried against the bare domain and www subdomain.
COMMON_PATHS = [
    "/feed",
    "/feed/",
    "/rss",
    "/rss/",
    "/atom.xml",
    "/rss.xml",
    "/rss2.xml",
    "/feed.xml",
    "/index.xml",
    "/index.rss",
    "/atom",
    "/blog/feed",
    "/blog/feed/",
    "/blog.rss",
    "/blog/atom.xml",
    "/blog/feed.xml",
    "/blog/rss",
    "/blog/rss.xml",
    "/english/rss",
    "/news/",
    "/news/rss.xml",
    "/news/feed",
    "/news/rss",
    "/newsroom/",
    "/newsroom/rss-feed.rss",
    "/newsroom/rssfeed.json",
    "/oem/rss",
    "/posts/default",
    "/feeds/posts/default",
    "/press-releases/rss",
    "/press/",
    "/psirtrss20/",
    "/rss/news.rss",
    "/rss/rss.xml",
    "/us/rc1004-rss",
]

# Subdomains probed for every base domain (in addition to the bare domain).
SUBDOMAINS = [
    "about", "azure", "blog", "blogs", "cloudblog", "community",
    "deepmind", "developer", "huggingface", "investors", "ir",
    "machinelearning", "news", "newsroom", "nvidianews", "openai",
    "pr", "research", "rocm", "sec", "security", "www",
]

# Concurrency and limits (env-tunable via MAX_WORKERS / FEEDS_PER_DOMAIN).
MAX_WORKERS = 12          # parallel HTTP probes
FEEDS_PER_DOMAIN = 3      # distinct feeds to capture before moving on
SNIFF_BYTES = 2048        # bytes read to decide "is this a feed?"
MAX_FEED_BYTES = 5_000_000  # cap on total feed bytes we download
MAX_HTML_BYTES = 65_536   # cap on HTML bytes we download to find <link> tags

DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_MODEL = "deepseek-chat"

# Network timeouts (seconds). These are defaults; they can be overridden via
# the .env file (CONNECT_TIMEOUT, REQUEST_TIMEOUT, DEEPSEEK_TIMEOUT,
# POLITE_DELAY, SEARXNG_URL).
CONNECT_TIMEOUT = 5   # time to establish a connection
REQUEST_TIMEOUT = 10  # time to wait for a response after connecting
DEEPSEEK_TIMEOUT = 60  # time to wait for a DeepSeek API response
POLITE_DELAY = 0.75  # seconds between network calls

# Automatic retry behavior for transient GET failures.
RETRY_TOTAL = 2
RETRY_BACKOFF = 0.3
RETRY_STATUS_FORCELIST = (429, 500, 502, 503, 504)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    )
}

CSV_FIELDS = [
    "domain",
    "feed_url",
    "title",
    "name",
    "description",
    "category",
    "is_up",
    "is_rss",
    "checked_at",
    "content_hash",
]

FEED_CONTENT_TYPES = (
    "application/rss+xml",
    "application/atom+xml",
    "text/xml",
    "application/xml",
)

SYSTEM_PROMPT = (
    "You generate concise metadata for RSS feeds. Respond with JSON only and "
    'nothing else, using exactly this shape: {"name": ..., "description": ..., '
    '"category": ...}. Keep each field short (name < 8 words, description 1-2 '
    "sentences, category one or two words)."
)


# --- Small utilities ---------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def load_dotenv(path: str = ".env") -> None:
    """Load KEY=VALUE pairs from a .env file into os.environ (if not set)."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = value


def _env_number(name: str, default: float) -> float:
    """Read a numeric value from the environment, falling back to default."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def apply_env_overrides() -> None:
    """Apply .env values to the tunable module-level settings."""
    global CONNECT_TIMEOUT, REQUEST_TIMEOUT, DEEPSEEK_TIMEOUT, POLITE_DELAY
    global MAX_WORKERS, FEEDS_PER_DOMAIN, SEARXNG_URL
    CONNECT_TIMEOUT = _env_number("CONNECT_TIMEOUT", CONNECT_TIMEOUT)
    REQUEST_TIMEOUT = _env_number("REQUEST_TIMEOUT", REQUEST_TIMEOUT)
    DEEPSEEK_TIMEOUT = _env_number("DEEPSEEK_TIMEOUT", DEEPSEEK_TIMEOUT)
    POLITE_DELAY = _env_number("POLITE_DELAY", POLITE_DELAY)
    MAX_WORKERS = int(_env_number("MAX_WORKERS", MAX_WORKERS))
    FEEDS_PER_DOMAIN = int(_env_number("FEEDS_PER_DOMAIN", FEEDS_PER_DOMAIN))
    SEARXNG_URL = os.environ.get("SEARXNG_URL", SEARXNG_URL)


def host_matches_domain(url: str, domain: str) -> bool:
    """True if the URL's host equals domain or is a subdomain of it."""
    host = urlparse(url).hostname
    if not host:
        return False
    host = host.lower()
    domain = domain.lower()
    return host == domain or host.endswith("." + domain)


def looks_like_feed(url: str) -> bool:
    """Heuristic: URL path/query hints at a feed."""
    path = urlparse(url).path.lower()
    return any(token in path for token in ("rss", "feed", "atom", ".xml"))


class _FeedLinkParser(HTMLParser):
    """Collects <link rel="alternate" type="...rss/atom/xml"> hrefs and any
    <a href> that looks feed-like. Built on the stdlib parser rather than
    regex so script/style bodies and odd quoting don't produce garbage."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.feed_hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_d = {k.lower(): (v or "") for k, v in attrs}
        href = attrs_d.get("href")
        if not href:
            return
        if tag == "link":
            rel = attrs_d.get("rel", "").lower()
            typ = attrs_d.get("type", "").lower()
            if "alternate" in rel and ("rss" in typ or "atom" in typ or "xml" in typ):
                self.feed_hrefs.append(href)
        elif tag == "a" and looks_like_feed(href):
            self.feed_hrefs.append(href)


def extract_feed_links(html: str, base_url: str) -> list[str]:
    """Find feed URLs declared in HTML (link rel=alternate and feed <a> links),
    resolved against base_url (pass the final, post-redirect URL here)."""
    parser = _FeedLinkParser()
    try:
        parser.feed(html)
    except Exception:
        return []
    return [urljoin(base_url, href) for href in parser.feed_hrefs]


def build_session() -> requests.Session:
    """Create a connection-pooled requests.Session shared across threads, with
    a couple of automatic retries on transient connection/HTTP errors."""
    s = requests.Session()
    retry_kwargs = dict(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=RETRY_STATUS_FORCELIST,
        raise_on_status=False,
    )
    try:
        retry = Retry(allowed_methods=frozenset({"GET"}), **retry_kwargs)
    except TypeError:
        # Older urllib3 (<1.26) used method_whitelist instead.
        retry = Retry(method_whitelist=frozenset({"GET"}), **retry_kwargs)
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS, max_retries=retry
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


def parse_feed(body: bytes) -> tuple[bool, str, str]:
    """Return (is_feed, content_hash, title) from raw feed bytes."""
    content_hash = hashlib.md5(body).hexdigest()
    parsed = feedparser.parse(body)
    version = getattr(parsed, "version", None)
    bozo = bool(getattr(parsed, "bozo", False))
    title = getattr(parsed.get("feed", {}), "title", "") or ""

    if version and not bozo:
        return (True, content_hash, title)
    # feedparser choked but the body looks like a feed; accept it.
    sniff = body[:512].lower()
    if b"<rss" in sniff or b"<feed" in sniff:
        return (True, content_hash, title)
    return (False, content_hash, title)


def _read_capped(first: bytes, iterator, cap: int) -> bytes:
    """Read the rest of a streamed body, stopping once `cap` bytes are hit."""
    chunks = [first]
    received = len(first)
    for chunk in iterator:
        chunks.append(chunk)
        received += len(chunk)
        if received >= cap:
            break
    return b"".join(chunks)


def probe_url(session: requests.Session, url: str, verbose: bool = False) -> dict:
    """Sniff a URL with minimal download, then finish reading only if it is a
    feed or an HTML page. Returns a dict with status, is_feed, hash, etc."""
    out = {
        "url": url,
        "final_url": url,
        "status": 0,
        "content_type": "",
        "is_feed": False,
        "content_hash": "",
        "title": "",
        "html": None,
    }
    try:
        with session.get(
            url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT), stream=True
        ) as resp:
            out["status"] = resp.status_code
            out["final_url"] = resp.url
            out["content_type"] = (resp.headers.get("Content-Type", "") or "").lower()

            iterator = resp.iter_content(chunk_size=SNIFF_BYTES)
            first = next(iterator, b"")
            sniff = first[:512].lower()

            feed_like = (
                any(ct in out["content_type"] for ct in FEED_CONTENT_TYPES)
                or b"<rss" in sniff
                or b"<feed" in sniff
            )
            html_like = (
                "html" in out["content_type"]
                or b"<html" in sniff
                or b"<!doctype html" in sniff
            )

            if resp.status_code == 200 and feed_like:
                body = _read_capped(first, iterator, MAX_FEED_BYTES)
                out["is_feed"], out["content_hash"], out["title"] = parse_feed(body)
            elif resp.status_code == 200 and html_like:
                body = _read_capped(first, iterator, MAX_HTML_BYTES)
                out["html"] = body.decode("utf-8", "ignore")
    except requests.RequestException as exc:
        if verbose:
            log(f"    [probe] {url}: {exc.__class__.__name__}: {exc}")
    except Exception as exc:
        if verbose:
            log(f"    [probe] {url}: unexpected {exc.__class__.__name__}: {exc}")
    return out


def _dedupe(urls: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def _parallel_probe(
    session: requests.Session, urls: Iterable[str], verbose: bool = False
) -> list[dict]:
    """Probe many URLs concurrently and return their result dicts."""
    urls = _dedupe(urls)
    results: list[dict] = []
    if not urls:
        return results
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(probe_url, session, u, verbose): u for u in urls}
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception:
                pass
    return results


def extract_json(text: str):
    """Best-effort extraction of a JSON object from an LLM reply."""
    if not text:
        raise ValueError("empty response")
    text = text.strip()
    # Strip markdown code fences if present.
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE)
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("no JSON object found")
    return json.loads(text[start : end + 1])


# --- Domain discovery --------------------------------------------------------

def read_domains(domains_file: str) -> list[str]:
    with open(domains_file, "r", encoding="utf-8") as f:
        domains = []
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            domains.append(line)
    return domains


def read_urls(urls_file: str) -> list[str]:
    """Read one URL per line from a raw RSS URL file (skip blanks/comments)."""
    with open(urls_file, "r", encoding="utf-8") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]


def normalize_url(url: str) -> str:
    """Canonical key for URL dedupe: lowercased scheme+netloc, no trailing slash."""
    p = urlparse(url.strip())
    scheme = p.scheme.lower() or "https"
    netloc = p.netloc.lower()
    path = p.path.rstrip("/")
    query = ("?" + p.query) if p.query else ""
    return f"{scheme}://{netloc}{path}{query}"


def existing_feed_urls(output_file: str) -> set[str]:
    """Return normalized feed_url values already recorded in the CSV."""
    if not os.path.exists(output_file):
        return set()
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        return {
            normalize_url(row["feed_url"])
            for row in csv.DictReader(f)
            if row.get("feed_url")
        }


def derive_domain(url: str) -> str:
    """Derive a domain label from a URL's hostname (www. stripped)."""
    host = (urlparse(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def already_processed_domains(output_file: str) -> set[str]:
    if not os.path.exists(output_file):
        return set()
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return {row["domain"] for row in reader if row.get("domain")}


def _existing_lines(path: str) -> set[str]:
    """Read non-empty, non-comment lines from a plain text file into a set."""
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return {
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        }


def record_unsuccessful(domain: str, unsuccessful_file: str, known: set[str]) -> None:
    """Append a domain to the unsuccessful file unless already present.
    `known` is mutated in place so callers avoid re-reading the file."""
    if domain in known:
        return
    known.add(domain)
    with open(unsuccessful_file, "a", encoding="utf-8") as f:
        f.write(domain + "\n")


def prune_unsuccessful(unsuccessful_file: str, output_file: str) -> None:
    """Remove domains from the unsuccessful file that now have recorded
    feeds (relevant after a --retry-unsuccessful run)."""
    if not os.path.exists(unsuccessful_file):
        return
    succeeded = already_processed_domains(output_file)
    remaining = sorted(d for d in _existing_lines(unsuccessful_file) if d not in succeeded)
    with open(unsuccessful_file, "w", encoding="utf-8") as f:
        for d in remaining:
            f.write(d + "\n")


def existing_content_hashes(output_file: str) -> dict[str, str]:
    """Map content_hash -> domain, built from prior CSV rows. Used only to
    give a helpful warning on cross-domain content collisions; it never
    causes a discovered feed to be dropped."""
    if not os.path.exists(output_file):
        return {}
    with open(output_file, "r", encoding="utf-8", newline="") as f:
        return {
            row["content_hash"]: row["domain"]
            for row in csv.DictReader(f)
            if row.get("content_hash")
        }


def searxng_urls(domain: str) -> list[str]:
    """Query SearXNG (last resort) and return on-domain result URLs."""
    urls: list[str] = []
    for query_tpl in SEARXNG_QUERIES:
        query = query_tpl.format(domain=domain)
        params = {"q": query, "format": "json"}
        try:
            resp = requests.get(
                SEARXNG_URL,
                params=params,
                timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT),
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log(f"    [searxng] error for '{query}': {exc}")
            continue
        for result in data.get("results", []):
            url = result.get("url", "")
            if url and host_matches_domain(url, domain):
                urls.append(url)
        time.sleep(POLITE_DELAY)
    return urls


def hostnames(domain: str) -> list[str]:
    """Return hostnames to probe: bare domain plus configured subdomains."""
    return _dedupe([domain] + [f"{sub}.{domain}" for sub in SUBDOMAINS])


def _feed_record(domain: str, r: dict) -> dict:
    return {
        "domain": domain,
        "feed_url": r.get("final_url") or r["url"],
        "title": r["title"],
        "name": "",
        "description": "",
        "category": "",
        "is_up": "true",
        "is_rss": "true",
        "content_hash": r["content_hash"],
    }


def _normalize_title(title: str) -> str:
    """Lowercase and strip punctuation/whitespace for semantic comparison."""
    t = re.sub(r"[^a-z0-9]+", " ", title.lower())
    return re.sub(r"\s+", " ", t).strip()


def _is_junk_feed(url: str, title: str) -> bool:
    """True for feeds we should never record (comments feeds, placeholders)."""
    u = url.lower()
    t = title.lower()
    if "/comments" in u or "/comment-" in u:
        return True
    if "comments on" in t or "comments for" in t or "trackback" in t:
        return True
    if "do not use" in t or "not in use" in t:
        return True
    return False


def _subdomain_label(host: str, domain: str) -> str:
    """Return the leading label of a host relative to the base domain."""
    h = host.lower()
    d = domain.lower()
    if h == d:
        return ""
    if h.endswith("." + d):
        return h[: -len(d) - 1]
    return h


def _feed_quality_score(url: str, domain: str) -> float:
    """Rank a feed so the best three distinct feeds win the quota slots."""
    host = urlparse(url).hostname or ""
    label = _subdomain_label(host, domain)
    path = urlparse(url).path.lower()
    u = url.lower()

    GOOD_LABELS = (
        "newsroom", "news", "press", "blog", "blogs", "investors",
        "ir", "research", "security", "developer", "deepmind",
    )
    BAD_LABELS = ("community", "forum", "forums")

    score = 0.0
    for g in GOOD_LABELS:
        if g in label:
            score += 2.0
    for b in BAD_LABELS:
        if b in label or b in host:
            score -= 4.0

    # path hints
    if any(k in path for k in ("newsroom", "press", "releases", "news")):
        score += 1.0
    if any(k in u for k in ("/community", "/forum")):
        score -= 3.0

    # shallow, root-level feed URLs are slightly preferred
    if path.rstrip("/").count("/") <= 1:
        score += 1.0
    return score


def _select_best_feeds(domain: str, results: list[dict]) -> list[dict]:
    """Filter junk, dedupe by hash + title *within this domain's candidates*,
    rank, and return the top N records."""
    seen_hashes: set[str] = set()
    seen_titles: set[str] = set()
    selected: list[dict] = []

    for r in results:
        if not r["is_feed"] or not r["content_hash"]:
            continue
        feed_url = r.get("final_url") or r["url"]
        if _is_junk_feed(feed_url, r["title"]):
            continue
        if r["content_hash"] in seen_hashes:
            continue
        norm_title = _normalize_title(r["title"])
        if norm_title and norm_title in seen_titles:
            continue
        seen_hashes.add(r["content_hash"])
        if norm_title:
            seen_titles.add(norm_title)
        selected.append(r)

    selected.sort(
        key=lambda r: _feed_quality_score(r.get("final_url") or r["url"], domain),
        reverse=True,
    )
    return [_feed_record(domain, r) for r in selected[:FEEDS_PER_DOMAIN]]


def discover_feeds(
    domain: str, session: requests.Session | None = None, verbose: bool = False
) -> list[dict]:
    """Discover validated feed records for a domain.

    Phase A: concurrently root-probe the bare domain + subdomains to find which
             hostnames are live and extract any feed links from their HTML.
    Phase B: concurrently probe COMMON_PATHS on live hostnames (sniff-first).
    Selection: filter junk, dedupe, rank, keep the best N.
    Fallback: only when nothing was found, query SearXNG.
    """
    if session is None:
        session = build_session()

    hosts = hostnames(domain)
    root_urls = [f"https://{h}" for h in hosts]

    # Phase A: find live hostnames + feed links declared in their HTML.
    # Hostnames are taken from the *final*, post-redirect URL so a root
    # domain that redirects to e.g. www. gets probed at the right place.
    live_hosts: list[str] = []
    discovered_links: list[str] = []
    for r in _parallel_probe(session, root_urls, verbose):
        if r["status"] == 200:
            final_host = urlparse(r["final_url"]).hostname or urlparse(r["url"]).hostname
            if final_host:
                live_hosts.append(final_host)
            if r["html"]:
                for link in extract_feed_links(r["html"], r["final_url"]):
                    if host_matches_domain(link, domain):
                        discovered_links.append(link)

    # Phase B: common paths on live hostnames + declared links.
    candidates = _dedupe(discovered_links)
    for host in _dedupe(live_hosts):
        for path in COMMON_PATHS:
            candidates.append(f"https://{host}{path}")

    feeds = _select_best_feeds(domain, _parallel_probe(session, candidates, verbose))

    # Fallback: SearXNG, only when direct probing found nothing.
    if not feeds:
        fallback = [u for u in _dedupe(searxng_urls(domain)) if looks_like_feed(u)]
        feeds = _select_best_feeds(domain, _parallel_probe(session, fallback, verbose))

    return feeds


# --- DeepSeek definition generation ------------------------------------------

def generate_definition(domain: str, url: str, title: str, attempts: int = 2) -> dict:
    """Ask DeepSeek for name/description/category. Retries once on a
    transient (429/5xx) failure before letting the caller fall back."""
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError(
            "DEEPSEEK_API_KEY environment variable is not set; "
            "cannot generate definitions."
        )

    model = os.environ.get("DEEPSEEK_MODEL", DEEPSEEK_MODEL)
    user_content = (
        f"Domain: {domain}\nFeed URL: {url}\nFeed title: {title or '(unknown)'}"
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.2,
    }

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                DEEPSEEK_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=DEEPSEEK_TIMEOUT,
            )
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} from DeepSeek", response=resp)
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            return extract_json(content)
        except Exception as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(1.5 * attempt)
    assert last_exc is not None
    raise last_exc


# --- CSV writing -------------------------------------------------------------

def init_csv(output_file: str) -> bool:
    """Create the CSV with headers if it does not exist. Returns True if new."""
    if os.path.exists(output_file):
        return False
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
    return True


def append_row(output_file: str, row: dict) -> None:
    with open(output_file, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)


# --- Main --------------------------------------------------------------------

def process_domain(
    domain: str,
    seen_hashes: dict[str, str],
    session: requests.Session,
    output_file: str,
    unsuccessful_file: str,
    unsuccessful_known: set[str],
    verbose: bool = False,
) -> int:
    """Discover and record feeds for one domain. Returns the number recorded."""
    records = discover_feeds(domain, session, verbose)
    log(f"[{domain}] {len(records)} feed(s) discovered")

    found = 0
    for rec in records:
        prior_domain = seen_hashes.get(rec["content_hash"])
        if prior_domain and prior_domain != domain:
            log(
                f"[{domain}] note: byte-identical to a feed already recorded "
                f"for '{prior_domain}' ({rec['feed_url']}) — recording anyway"
            )
        seen_hashes[rec["content_hash"]] = domain

        rec["checked_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        log(f"[{domain}] valid feed: {rec['feed_url']}")

        try:
            meta = generate_definition(domain, rec["feed_url"], rec["title"])
            rec["name"] = meta.get("name", "")
            rec["description"] = meta.get("description", "")
            rec["category"] = meta.get("category", "")
        except Exception as exc:
            log(f"[{domain}] deepseek fallback ({exc}); using feed title")
            rec["name"] = rec["title"] or ""
            rec["description"] = ""
            rec["category"] = ""

        append_row(output_file, rec)
        found += 1
        time.sleep(POLITE_DELAY)

    if found == 0:
        record_unsuccessful(domain, unsuccessful_file, unsuccessful_known)
        log(f"[{domain}] no RSS feeds found")
    else:
        log(f"[{domain}] recorded {found} feed(s)")
    return found


def process_urls(
    urls_file: str,
    output_file: str,
    session: requests.Session,
    seen_urls: set[str],
    seen_hashes: set[str],
    verbose: bool = False,
) -> int:
    """Validate a raw list of feed URLs and append the new ones to the CSV.

    Dedupes by normalized URL and by byte-identical content (content_hash),
    against both the existing CSV and within the file itself. Returns the
    number of new feeds appended.
    """
    urls = read_urls(urls_file)
    log(f"Loaded {len(urls)} raw URL(s) from {urls_file}")

    added = 0
    for r in _parallel_probe(session, urls, verbose):
        if not r["is_feed"] or not r["content_hash"]:
            continue
        final_url = r.get("final_url") or r["url"]
        if (
            normalize_url(final_url) in seen_urls
            or normalize_url(r["url"]) in seen_urls
        ):
            log(f"duplicate URL, skipping: {final_url}")
            continue
        if r["content_hash"] in seen_hashes:
            log(f"duplicate content, skipping: {final_url}")
            continue
        seen_urls.add(normalize_url(final_url))
        seen_urls.add(normalize_url(r["url"]))
        seen_hashes.add(r["content_hash"])

        domain = derive_domain(final_url)
        rec = _feed_record(domain, r)
        rec["checked_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        log(f"valid feed: {final_url}")

        try:
            meta = generate_definition(domain, rec["feed_url"], rec["title"])
            rec["name"] = meta.get("name", "")
            rec["description"] = meta.get("description", "")
            rec["category"] = meta.get("category", "")
        except Exception as exc:
            log(f"deepseek fallback ({exc}); using feed title")
            rec["name"] = rec["title"] or ""
            rec["description"] = ""
            rec["category"] = ""

        append_row(output_file, rec)
        added += 1
        time.sleep(POLITE_DELAY)

    log(f"Added {added} new feed(s) from {len(urls)} URL(s).")
    return added


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Discover RSS/Atom feeds for a list of domains and write results "
            "to a CSV, recording domains with no feeds to an unsuccessful file."
        )
    )
    parser.add_argument(
        "domains",
        nargs="?",
        default=DOMAINS_FILE,
        help=f"Path to the domains text file (default: {DOMAINS_FILE})",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help=f"Path to the output CSV file (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--unsuccessful",
        default=UNSUCCESSFUL_FILE,
        help=f"Path to the unsuccessful-domains file (default: {UNSUCCESSFUL_FILE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N domains this run (useful for testing).",
    )
    parser.add_argument(
        "--retry-unsuccessful",
        action="store_true",
        help=(
            "Also re-probe domains previously recorded with zero feeds found. "
            "By default those are skipped on later runs. Domains that succeed "
            "on retry are pruned from the unsuccessful file."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-request errors during probing (noisy; useful for debugging).",
    )
    parser.add_argument(
        "--urls",
        default=None,
        help=(
            "Path to a raw RSS feed URL text file (one URL per line). Validates "
            "and appends them without duplicates instead of running domain discovery."
        ),
    )
    return parser.parse_args()


def main() -> int:
    load_dotenv()
    apply_env_overrides()

    args = parse_args()

    if args.urls:
        init_csv(args.output)
        seen_urls = existing_feed_urls(args.output)
        seen_hashes = set(existing_content_hashes(args.output).keys())
        session = build_session()
        process_urls(
            args.urls, args.output, session, seen_urls, seen_hashes, args.verbose
        )
        return 0

    domains = read_domains(args.domains)
    succeeded = already_processed_domains(args.output)
    unsuccessful_known = _existing_lines(args.unsuccessful)

    processed = set(succeeded)
    if not args.retry_unsuccessful:
        processed |= unsuccessful_known

    todo = [d for d in domains if d not in processed]
    if args.limit is not None:
        todo = todo[: args.limit]

    log(
        f"Loaded {len(domains)} domain(s); "
        f"{len(processed)} already processed; {len(todo)} to process."
    )

    if not todo:
        log("Nothing to do.")
        return 0

    init_csv(args.output)

    seen_hashes = existing_content_hashes(args.output)
    session = build_session()

    total_feeds = 0
    domains_with_feeds = 0
    try:
        for idx, domain in enumerate(todo, 1):
            log(f"[{idx}/{len(todo)}] {domain}")
            try:
                found = process_domain(
                    domain,
                    seen_hashes,
                    session,
                    args.output,
                    args.unsuccessful,
                    unsuccessful_known,
                    args.verbose,
                )
                total_feeds += found
                if found:
                    domains_with_feeds += 1
            except Exception as exc:
                log(f"[{domain}] failed: {exc}")
    except KeyboardInterrupt:
        log("\nInterrupted by user; progress made so far has already been saved.")
        return 130

    if args.retry_unsuccessful:
        prune_unsuccessful(args.unsuccessful, args.output)

    log(
        f"Done. {total_feeds} feed(s) recorded across "
        f"{domains_with_feeds}/{len(todo)} domain(s) in {args.output}."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
