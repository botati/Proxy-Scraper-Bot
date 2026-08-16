"""Shared configuration and constants.

Made by @AntonysrmNafi
"""
import os

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is not set")

# A proxy is checked by timing a single GET to this URL through it. Live iff it responds
# within CHECK_TIMEOUT; ping_ms is how long that took.
TEST_URL = "https://1.1.1.1"
IP_API_URL = "http://ip-api.com/json/{ip}"
SCRAPE_TIMEOUT = 10                     # seconds, per-source scrape request
CHECK_TIMEOUT = 5                       # seconds, total per-proxy check budget; over this -> Dead
CHECK_CONNECT_TIMEOUT = 2               # seconds to establish the connection; dead proxies fail this fast
CHECK_BATCH_SIZE = 25                   # proxies checked per batch; also the /stop granularity
CHECK_THREADS = 100                     # worker threads used to check proxies concurrently (I/O-bound, so this can safely exceed CPU core count)
MAX_CHECK_PER_JOB = 6000                # hard cap on proxies checked per job (sources can yield 100k+ raw)
MAX_SCRAPE_ROUNDS = 5                    # safety cap: stop re-scraping for more if live_limit still isn't met
PROGRESS_EDIT_MIN_INTERVAL = 1.2        # seconds between Telegram message edits, avoids flood-control errors
OUTPUT_DIR = "output"                   # where result files are written before upload
DB_PATH = os.environ.get("DB_PATH", "data/proxybot.db")  # dead/active proxy lists (need a persistent volume)
