"""Proxy checker: single-URL speed test via `requests`, run across a thread pool so many
proxies are checked concurrently (requests is blocking/sync, so real OS threads are what
give us concurrency here, unlike the rest of the bot which uses asyncio).

A proxy is hit once with GET TEST_URL (config.py). If it responds within CHECK_TIMEOUT
seconds, ping_ms is how long that took and the proxy is a candidate for Live. To make sure
it's actually routing traffic through itself (and not silently leaking your own connection
through instead, which would make any ping/country reading meaningless), we make one more
request THROUGH the proxy to ip-api.com's self-report endpoint and require the IP it says
it saw to equal the proxy's own IP - only then is it counted Live, and only then is its
country trusted.

Proxies are checked in fixed-size batches (CHECK_BATCH_SIZE) so a graceful /stop only has to
wait for the current batch, never the full list.

Made by @AntonysrmNafi
"""
import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter

from config import TEST_URL, IP_API_URL, CHECK_TIMEOUT, CHECK_CONNECT_TIMEOUT, CHECK_BATCH_SIZE, CHECK_THREADS

SCHEME_MAP = {"http": "http", "https": "https", "socks4": "socks4", "socks5": "socks5", "socks": "socks5"}
SELF_LOOKUP_URL = IP_API_URL.format(ip="")  # no IP -> ip-api.com reports whoever is asking
REQUEST_TIMEOUT = (CHECK_CONNECT_TIMEOUT, CHECK_TIMEOUT)  # (connect, read) - dead proxies fail fast, alive ones get the full budget

# Shared pool: all proxy checks (bulk jobs + /check) run their blocking `requests` calls here.
_executor = ThreadPoolExecutor(max_workers=CHECK_THREADS)

# One requests.Session per worker thread, reused across every proxy that thread checks - avoids
# rebuilding SSL contexts/connection-pool objects on every single check (each proxy is still a
# fresh connection since the target IP differs, but the Session's setup itself is reused).
_thread_local = threading.local()


def _session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session


@dataclass
class CheckResult:
    proxy: str
    is_live: bool
    ping_ms: int = 0
    country: str = "Unknown"

    @property
    def status(self) -> str:
        return "Live" if self.is_live else "Dead"

    def format_line(self) -> str:
        """The bot's save/display format: 'IP:PORT | 180ms | Live/Dead | Country'."""
        if self.is_live:
            return f"{self.proxy} | {self.ping_ms}ms | Live | {self.country}"
        return f"{self.proxy} | Dead"


def _proxy_dict(proxy: str, method: str) -> dict:
    scheme = SCHEME_MAP.get(method, "http")
    proxy_url = f"{scheme}://{proxy}"
    return {"http": proxy_url, "https": proxy_url}


def _check_one_sync(proxy: str, method: str) -> CheckResult:
    """Blocking; must only run inside a worker thread (via _executor), never on the event loop."""
    if method == "socks":
        res = _check_one_sync(proxy, "socks5")
        if res.is_live:
            return res
        return _check_one_sync(proxy, "socks4")

    proxies = _proxy_dict(proxy, method)
    session = _session()

    start = time.monotonic()
    try:
        with session.get(TEST_URL, proxies=proxies, timeout=REQUEST_TIMEOUT, stream=True) as resp:
            ping_ms = int((time.monotonic() - start) * 1000)
            resp.close()  # drop the connection without reading the body - we only wanted the timing
    except requests.RequestException:
        return CheckResult(proxy=proxy, is_live=False)

    if ping_ms > CHECK_TIMEOUT * 1000:
        return CheckResult(proxy=proxy, is_live=False)

    try:
        resp = session.get(
            f"{SELF_LOOKUP_URL}?fields=status,country", proxies=proxies, timeout=REQUEST_TIMEOUT
        )
        data = resp.json()
    except (requests.RequestException, ValueError):
        return CheckResult(proxy=proxy, is_live=False)

    if data.get("status") != "success":
        return CheckResult(proxy=proxy, is_live=False)

    country = data.get("country") or "Unknown"
    return CheckResult(proxy=proxy, is_live=True, ping_ms=ping_ms, country=country)


def _lookup_details_sync(proxy: str, method: str) -> dict:
    """Blocking; richer on-demand result for the /check command (region/city/isp/org/asn)."""
    if method == "socks":
        res = _lookup_details_sync(proxy, "socks5")
        if res.get("is_live"):
            return res
        return _lookup_details_sync(proxy, "socks4")

    result = {"proxy": proxy, "method": method, "is_live": False, "ping_ms": None}
    proxies = _proxy_dict(proxy, method)
    session = _session()

    start = time.monotonic()
    try:
        with session.get(TEST_URL, proxies=proxies, timeout=REQUEST_TIMEOUT, stream=True) as resp:
            ping_ms = int((time.monotonic() - start) * 1000)
            resp.close()
    except requests.RequestException:
        result["error"] = "unreachable"
        return result

    if ping_ms > CHECK_TIMEOUT * 1000:
        result["error"] = "unreachable"
        return result

    fields = "status,country,regionName,city,isp,org,as"
    try:
        resp = session.get(f"{SELF_LOOKUP_URL}?fields={fields}", proxies=proxies, timeout=REQUEST_TIMEOUT)
        data = resp.json()
    except (requests.RequestException, ValueError):
        result["error"] = "responded, but the follow-up lookup failed"
        return result

    if data.get("status") != "success":
        result["error"] = "responded, but the follow-up lookup failed"
        return result

    result["is_live"] = True
    result["ping_ms"] = ping_ms
    result.update(
        country=data.get("country") or "Unknown",
        region=data.get("regionName") or "",
        city=data.get("city") or "",
        isp=data.get("isp") or "Unknown",
        org=data.get("org") or "",
        asn=data.get("as") or "",
    )
    return result


async def lookup_proxy_details(proxy: str, method: str) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, _lookup_details_sync, proxy, method)


async def check_proxies(proxies, method, stop_event, live_limit, on_progress, proxy_sources=None):
    """
    Check `proxies` in batches of CHECK_BATCH_SIZE, each proxy's blocking `requests` call
    running in its own thread (see _executor) so a batch checks concurrently, not one-by-one.
    """
    proxy_sources = proxy_sources or {}
    live_results: list[CheckResult] = []
    checked = 0
    loop = asyncio.get_event_loop()

    for i in range(0, len(proxies), CHECK_BATCH_SIZE):
        if stop_event.is_set():
            break

        batch = proxies[i:i + CHECK_BATCH_SIZE]
        results = await asyncio.gather(
            *(loop.run_in_executor(_executor, _check_one_sync, p, method) for p in batch)
        )

        checked += len(batch)
        dead_batch = [r.proxy for r in results if not r.is_live]
        live_batch = [(r.proxy, r.country, r.ping_ms) for r in results if r.is_live]
        live_results.extend(r for r in results if r.is_live)

        source_batch: dict[str, list] = {}
        for r in results:
            src = proxy_sources.get(r.proxy, "unknown")
            entry = source_batch.setdefault(src, [0, 0])
            entry[0] += 1
            if r.is_live:
                entry[1] += 1
        source_batch = {k: tuple(v) for k, v in source_batch.items()}

        await on_progress(checked, len(live_results), len(proxies), dead_batch, live_batch, source_batch)

        if live_limit and len(live_results) >= live_limit:
            break

    live_results.sort(key=lambda r: r.ping_ms)  # fastest (lowest ms) first
    return live_results
