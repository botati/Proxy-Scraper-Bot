"""Scrapes free proxy lists from multiple public sources, concurrently.

Made by @AntonysrmNafi
"""
import asyncio
import logging
import re
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from config import SCRAPE_TIMEOUT

logger = logging.getLogger(__name__)

PROXY_PATTERN = re.compile(r"\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?")
RETRY_ATTEMPTS = 2
RETRY_DELAY = 0.6  # seconds, between retries on the same source


class Scraper:
    def __init__(self, method, url_template):
        self.method = method
        self._url = url_template

    def get_url(self, **kwargs):
        return self._url.format(**kwargs, method=self.method)

    async def fetch(self, client):
        return await client.get(self.get_url(), timeout=SCRAPE_TIMEOUT)

    async def parse(self, response):
        return response.text

    async def scrape(self, client):
        """Returns (proxies, ok). A dead/slow source never raises, it just reports ok=False."""
        last_error = None
        for attempt in range(RETRY_ATTEMPTS):
            try:
                response = await self.fetch(client)
                text = await self.parse(response)
                return PROXY_PATTERN.findall(text), True
            except Exception as e:
                last_error = e
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(RETRY_DELAY)
        logger.debug("Source failed after %d attempts: %s (%s)", RETRY_ATTEMPTS, self.get_url(), last_error)
        return [], False


class SpysMeScraper(Scraper):
    def __init__(self, method):
        super().__init__(method, "https://spys.me/{mode}.txt")

    def get_url(self, **kwargs):
        mode = "proxy" if self.method == "http" else "socks"
        return super().get_url(mode=mode, **kwargs)


class ProxyScrapeScraper(Scraper):
    def __init__(self, method, timeout=1000, country="All"):
        self.timeout_param = timeout
        self.country = country
        super().__init__(
            method,
            "https://api.proxyscrape.com/?request=getproxies"
            "&proxytype={method}&timeout={timeout_param}&country={country}",
        )

    def get_url(self, **kwargs):
        return super().get_url(timeout_param=self.timeout_param, country=self.country, **kwargs)


class GeoNodeScraper(Scraper):
    def __init__(self, method, limit="500", page="1", sort_by="lastChecked", sort_type="desc"):
        self.limit, self.page = limit, page
        self.sort_by, self.sort_type = sort_by, sort_type
        super().__init__(
            method,
            "https://proxylist.geonode.com/api/proxy-list?"
            "&limit={limit}&page={page}&sort_by={sort_by}&sort_type={sort_type}",
        )

    def get_url(self, **kwargs):
        return super().get_url(
            limit=self.limit, page=self.page, sort_by=self.sort_by, sort_type=self.sort_type, **kwargs
        )


class ProxyListDownloadScraper(Scraper):
    def __init__(self, method, anon):
        self.anon = anon
        super().__init__(method, "https://www.proxy-list.download/api/v1/get?type={method}&anon={anon}")

    def get_url(self, **kwargs):
        return super().get_url(anon=self.anon, **kwargs)


class GeneralTableScraper(Scraper):
    async def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        proxies = set()
        table = soup.find("table", attrs={"class": "table table-striped table-bordered"})
        if table:
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    proxies.add(f"{cells[0].text.strip()}:{cells[1].text.strip()}")
        return "\n".join(proxies)


class GeneralDivScraper(Scraper):
    async def parse(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        proxies = set()
        container = soup.find("div", attrs={"class": "list"})
        if container:
            for row in container.find_all("div"):
                cells = row.find_all("div", attrs={"class": "td"})[:2]
                if len(cells) == 2:
                    proxies.add(f"{cells[0].text.strip()}:{cells[1].text.strip()}")
        return "\n".join(proxies)


class GitHubScraper(Scraper):
    async def parse(self, response):
        proxies = set()
        for line in response.text.splitlines():
            if self.method in line:
                proxies.add(line.split("//")[-1].strip())
        return "\n".join(proxies)


class MonosansGeoScraper(Scraper):
    """monosans/proxy-list's proxies.json - smaller than most sources, but each entry is
    pre-verified with real MaxMind geolocation, which tends to give good country coverage."""

    def __init__(self, method):
        super().__init__(method, "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json")

    async def parse(self, response):
        try:
            entries = response.json()
        except Exception:
            return ""
        wanted = {"socks4", "socks5"} if self.method == "socks" else {self.method}
        return "\n".join(
            f"{e['host']}:{e['port']}"
            for e in entries
            if e.get("protocol") in wanted and e.get("host") and e.get("port")
        )


def _build_scrapers():
    return [
        SpysMeScraper("http"),
        SpysMeScraper("socks"),
        ProxyScrapeScraper("http"),
        ProxyScrapeScraper("socks4"),
        ProxyScrapeScraper("socks5"),
        GeoNodeScraper("socks"),
        ProxyListDownloadScraper("https", "elite"),
        ProxyListDownloadScraper("http", "elite"),
        ProxyListDownloadScraper("http", "transparent"),
        ProxyListDownloadScraper("http", "anonymous"),
        GeneralTableScraper("https", "http://sslproxies.org"),
        GeneralTableScraper("http", "http://free-proxy-list.net"),
        GeneralTableScraper("http", "http://us-proxy.org"),
        GeneralTableScraper("socks", "http://socks-proxy.net"),
        GeneralDivScraper("http", "https://freeproxy.lunaproxy.com/"),
        # Combined-method files (each line prefixed "http://", "socks5://" etc.) - need GitHubScraper's filter.
        GitHubScraper("http", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt"),
        GitHubScraper("socks4", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt"),
        GitHubScraper("socks5", "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt"),
        GitHubScraper("http", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt"),
        GitHubScraper("socks", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/all.txt"),

        # Pre-split single-method files, no scheme prefix - plain Scraper (no filtering needed/possible).
        # NOTE: zloi-user was previously wired through GitHubScraper, whose "method in line" filter silently
        # dropped every line because these lines have no scheme text in them - fixed to yield proxies again.
        Scraper("https", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/https.txt"),
        Scraper("http", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt"),
        Scraper("http", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt"),
        Scraper("http", "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt"),
        Scraper("http", "https://raw.githubusercontent.com/sunny9577/proxy-scraper/master/proxies.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),

        # Huge yield (100k+ lines each) - verified alive.
        Scraper("http", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt"),

        Scraper("http", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/im-razvan/proxy_list/main/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Https.txt"),
        Scraper("http", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt"),
        Scraper("socks4", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt"),
        Scraper("socks5", "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"),
        Scraper("http", "https://raw.githubusercontent.com/almroot/proxylist/master/list.txt"),
        Scraper("http", "https://raw.githubusercontent.com/opsxcq/proxy-list/master/list.txt"),

        # Geo-tagged (real MaxMind country data at the source, good for country-targeted requests).
        MonosansGeoScraper("http"),
        MonosansGeoScraper("socks4"),
        MonosansGeoScraper("socks5"),
    ]


def _source_id(scraper: Scraper) -> str:
    """A stable, readable id for a scraper, used to track per-source quality. Plain domain for
    most sources; for raw.githubusercontent.com (30+ of our sources), the owner/repo is
    included too, otherwise every GitHub-hosted list would collapse into one indistinguishable
    "source" and defeat the whole point of ranking sources against each other."""
    parsed = urlparse(scraper.get_url())
    netloc = parsed.netloc or scraper.__class__.__name__
    if netloc == "raw.githubusercontent.com":
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) >= 2:
            return f"{netloc}/{parts[0]}/{parts[1]}"
    return netloc


async def scrape_all(method: str):
    """
    Scrape every source matching `method` (http / socks4 / socks5 / socks).
    Returns (proxies, proxy_sources, sources_ok, sources_total):
      - proxies: sorted, deduped list of proxy strings
      - proxy_sources: {proxy: source_id} - which source it came from (first source that
        yielded it "owns" it if more than one source has the same proxy), used to check
        proxies from historically-better sources first (see storage.get_source_scores)
    """
    methods = [method] + (["socks4", "socks5"] if method == "socks" else [])
    scrapers = [s for s in _build_scrapers() if s.method in methods]
    if not scrapers:
        raise ValueError(f"Unsupported method: {method}")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        results = await asyncio.gather(*(s.scrape(client) for s in scrapers))

    proxy_sources: dict[str, str] = {}
    for scraper, (group, ok) in zip(scrapers, results):
        if not ok:
            continue
        src = _source_id(scraper)
        for p in group:
            if p and p not in proxy_sources:
                proxy_sources[p] = src

    proxies = sorted(proxy_sources.keys())
    sources_ok = sum(1 for _, ok in results if ok)
    return proxies, proxy_sources, sources_ok, len(scrapers)


async def scrape_country_boost(method: str, iso_code: str) -> list:
    """
    Best-effort EXTRA proxies for one specific country (ISO 3166-1 alpha-2, e.g. "US").
    The general pool from scrape_all() is unfiltered and now huge (100k+), so a country
    filter applied only after checking a random sample can easily come back empty even
    for common countries - this fetches candidates that are already known/likely to be
    in that country, so they actually get checked. Failures here are non-fatal; callers
    still have the general pool as a fallback.
    """
    methods = [method] + (["socks4", "socks5"] if method == "socks" else [])
    results = set()

    async with httpx.AsyncClient(follow_redirects=True, timeout=SCRAPE_TIMEOUT) as client:
        # monosans' geo-tagged JSON, filtered client-side by real geolocation data
        try:
            resp = await client.get("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json")
            for e in resp.json():
                country = (e.get("geolocation") or {}).get("country") or {}
                if e.get("protocol") in methods and country.get("iso_code") == iso_code and e.get("host"):
                    results.add(f"{e['host']}:{e['port']}")
        except Exception:
            logger.debug("Country-boost monosans fetch failed for %s", iso_code)

        # ProxyScrape's own per-country filter, one call per protocol variant needed
        for m in methods:
            try:
                resp = await client.get(
                    "https://api.proxyscrape.com/?request=getproxies"
                    f"&proxytype={m}&timeout=2000&country={iso_code}"
                )
                results.update(PROXY_PATTERN.findall(resp.text))
            except Exception:
                logger.debug("Country-boost proxyscrape fetch failed for %s/%s", m, iso_code)

    return list(results)
