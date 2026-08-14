"""Telegram bot: queue-based proxy scrape + check jobs, with a button-first UI.

Jobs run one at a time (a single background worker consumes the queue), so the
bot and the host machine stay under a predictable load even when many chats
request jobs at once. The Stop button (or /stop) asks the currently running
job to stop after its current check batch finishes.

Every command also has an equivalent button, reachable from /start's main menu
(see MENU_TEXT / _main_menu_keyboard) - there are no /scrape or /check commands,
everything is button/inline-driven except typed free text where a number or a
proxy is genuinely needed (custom limits, a proxy for the Check flow).

Made by @AntonysrmNafi
"""
import asyncio
import html
import logging
import os
import re
import time
import uuid
from collections import Counter, OrderedDict
from dataclasses import dataclass, field

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from checker import check_proxies, lookup_proxy_details
from config import BOT_TOKEN, MAX_CHECK_PER_JOB, MAX_SCRAPE_ROUNDS, OUTPUT_DIR, PROGRESS_EDIT_MIN_INTERVAL
from scraper import scrape_all, scrape_country_boost
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

VALID_METHODS = {"http", "socks4", "socks5", "socks"}
LIVE_LIMIT_CHOICES = [10, 25, 50, 100]
PROGRESS_BAR_WIDTH = 14
TOP_COUNTRY_BUTTONS = 6
COMPLETED_CACHE_SIZE = 30  # how many finished jobs' results we keep around for the country-filter buttons
PROXY_INPUT_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}:\d{1,5}$")
DIVIDER = "┄" * 22

TOP_COUNTRIES = [
    "United States", "United Kingdom", "Germany", "Canada", "Australia",
    "France", "Japan", "Netherlands", "Switzerland", "India",
    "Mexico", "Indonesia", "Brazil", "Singapore", "Russia",
]

# Name (as shown on buttons, or typed via CLI) -> ISO 3166-1 alpha-2, for the country-boost fetch.
# Lowercase keys; common aliases included since CLI usage isn't limited to the button labels.
COUNTRY_ISO = {
    "united states": "US", "usa": "US", "us": "US", "america": "US",
    "united kingdom": "GB", "uk": "GB", "britain": "GB", "england": "GB",
    "germany": "DE", "canada": "CA", "australia": "AU", "france": "FR", "japan": "JP",
    "netherlands": "NL", "holland": "NL",
    "switzerland": "CH", "india": "IN", "mexico": "MX", "indonesia": "ID",
    "brazil": "BR", "singapore": "SG", "russia": "RU",
    "south korea": "KR", "korea": "KR", "china": "CN",
}


@dataclass
class Job:
    chat_id: int
    method: str
    live_limit: int | None
    country_filter: str | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    status_message_id: int | None = None
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    last_edit_at: float = 0.0  # monotonic time of the last progress edit, throttles Telegram flood control


job_queue: "asyncio.Queue[Job]" = asyncio.Queue()
active_job: Job | None = None        # job currently being processed
pending_custom_limit: dict[int, tuple] = {}  # chat_id -> (method, country_filter), waiting on a typed custom number
pending_check: set[int] = set()      # chat_ids waiting on a typed "ip:port [method]" for /check
pending_restore: set[int] = set()    # admin chat_ids waiting on a backup file to restore
chats_with_job: set[int] = set()     # chat_ids that already have a job queued or active - blocks duplicate submissions

# job_id -> (all live CheckResults, ordered list of distinct countries found), for the country-filter buttons.
# Bounded so it can never grow unbounded across a long-running bot process.
completed_results: "OrderedDict[str, tuple[list, list[str]]]" = OrderedDict()


def _remember_results(job_id: str, live_results, countries: list[str]):
    completed_results[job_id] = (live_results, countries)
    if len(completed_results) > COMPLETED_CACHE_SIZE:
        completed_results.popitem(last=False)


# ============================================================================
# UI text - kept together so the bot's voice/format stays consistent everywhere
# ============================================================================

def _card(rows: list[tuple[str, str]]) -> str:
    """A neat aligned key/value block, rendered monospace so columns actually line up."""
    width = max(len(label) for label, _ in rows)
    body = "\n".join(f"{label.ljust(width)}  {value}" for label, value in rows)
    return f"<pre>{html.escape(body)}</pre>"


async def _main_menu_text() -> str:
    stats = await storage.get_stats()
    total_active = sum(m["count"] for m in stats["active"].values())
    total_dead = sum(stats["dead"].values())
    return (
        "🛰 <b>Proxy Scraper Bot</b>\n"
        "Fresh, verified proxies, on demand.\n\n"
        f"📊 In database: <b>{total_active:,}</b> active  •  <b>{total_dead:,}</b> known-dead\n\n"
        "Pick an option below to get started."
    )


HELP_TEXT = (
    "❓ <b>How This Bot Works</b>\n"
    f"{DIVIDER}\n\n"
    "<b>1. Start a scrape</b>\n"
    "Choose a proxy type → a country (or Random) → how many live proxies you want.\n\n"
    "<b>2. Checking</b>\n"
    "Every proxy is tested with one real request; ping is how long that took. "
    "Proxies that were live before are re-checked first, before anything new is "
    "scraped. Dead ones are remembered and skipped on future runs.\n\n"
    "<b>3. Filter results</b>\n"
    "When a job finishes, tap a country button under the summary to get just "
    "that country's list.\n\n"
    "<b>Getting around</b>\n"
    "Everything is button-based. Tap 🚀 Start Scrape or 🔎 Check a Proxy from the menu.\n"
    "<code>/start</code> - open the main menu\n"
    "<code>/stop</code> - stop the running job after its current batch\n\n"
    "Jobs run one at a time. If the bot's busy, yours is queued automatically.\n\n"
    f"{DIVIDER}\n"
)

CHECK_PROMPT_TEXT = (
    "🔎 <b>Check a Proxy</b>\n\n"
    "Send it as <code>ip:port</code>, optionally followed by the method:\n"
    "<code>1.2.3.4:8080</code>\n"
    "<code>1.2.3.4:8080 socks5</code>"
)

RESTORE_PROMPT_TEXT = (
    "♻️ <b>Restore Backup</b>\n\n"
    "Send the backup file (<code>.jsonl</code>) to merge it in.\n"
    "Existing entries are never overwritten, only genuinely new proxies get added."
)

SETTINGS_TEXT = "⚙️ <b>Settings</b>\nManage the proxy database."


def _stats_text(stats: dict) -> str:
    rows = []
    methods = sorted(set(stats["active"]) | set(stats["dead"]))
    if not methods:
        return "📊 <b>Database Stats</b>\n\nNothing checked yet. Run a scrape first."
    for m in methods:
        active = stats["active"].get(m, {"count": 0, "avg_ping": 0})
        dead = stats["dead"].get(m, 0)
        rows.append((m.upper(), f"{active['count']:,} active ({active['avg_ping']}ms avg)  •  {dead:,} dead"))
    card = _card(rows)
    top = ", ".join(f"{c} ({n})" for c, n in stats["top_countries"]) or "Unknown"
    return f"📊 <b>Database Stats</b>\n{card}\n\n🌍 <b>Top countries</b>\n{html.escape(top)}"


def _format_since(first_seen: float | None) -> str | None:
    if not first_seen:
        return None
    days = max(0, int((time.time() - first_seen) / 86400))
    if days == 0:
        return "today"
    if days == 1:
        return "1 day ago"
    return f"{days} days ago"


def _profile_text(user, db_stats: dict, user_stats: dict) -> str:
    full_name = " ".join(filter(None, [user.first_name, user.last_name])) or "Unknown"
    identity = _card([
        ("Name", full_name),
        ("Username", f"@{user.username}" if user.username else "(not set)"),
        ("User ID", str(user.id)),
    ])

    total_active = sum(m["count"] for m in db_stats["active"].values())
    total_dead = sum(db_stats["dead"].values())
    overview = _card([
        ("Total tracked", f"{total_active + total_dead:,}"),
        ("Active", f"{total_active:,}"),
        ("Dead", f"{total_dead:,}"),
    ])

    methods = sorted(set(db_stats["active"]) | set(db_stats["dead"]))
    if methods:
        by_type = _card([
            (m.upper(), f"{db_stats['active'].get(m, {'count': 0})['count']:,} active  •  {db_stats['dead'].get(m, 0):,} dead")
            for m in methods
        ])
    else:
        by_type = "<i>Nothing checked yet.</i>"

    activity_rows = [
        ("Scrapes run", f"{user_stats.get('jobs_run', 0):,}"),
        ("Proxies received", f"{user_stats.get('proxies_delivered', 0):,}"),
    ]
    since = _format_since(user_stats.get("first_seen"))
    if since:
        activity_rows.append(("Member since", since))
    activity = _card(activity_rows)

    return (
        f"👤 <b>Your Profile</b>\n{identity}\n\n"
        f"📊 <b>Database Overview</b>\n{overview}\n\n"
        f"🗂 <b>By Type</b>\n{by_type}\n\n"
        f"⚡ <b>Your Activity</b>\n{activity}"
    )


# ---------- keyboards ----------

def _main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀  Start Scrape", callback_data="menu:scrape")],
        [
            InlineKeyboardButton("🔎  Check a Proxy", callback_data="menu:check"),
            InlineKeyboardButton("⏹  Stop Job", callback_data="menu:stop"),
        ],
        [
            InlineKeyboardButton("👤  Profile", callback_data="menu:profile"),
            InlineKeyboardButton("⚙️  Settings", callback_data="menu:settings"),
        ],
        [InlineKeyboardButton("❓  Help", callback_data="menu:help")],
    ])


def _settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💾  Backup", callback_data="menu:backup"),
            InlineKeyboardButton("♻️  Restore", callback_data="menu:restore"),
        ],
        [
            InlineKeyboardButton("📊  Stats", callback_data="menu:stats"),
            InlineKeyboardButton("🧹  Clean Dead List", callback_data="menu:clean_dead"),
        ],
        [InlineKeyboardButton("‹ Back", callback_data="menu:main")],
    ])


def _confirm_keyboard(confirm_data: str, cancel_data: str = "menu:settings") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Confirm", callback_data=confirm_data),
        InlineKeyboardButton("✖️ Cancel", callback_data=cancel_data),
    ]])


def _back_keyboard(target: str, label: str = "‹ Back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=target)]])


def _method_keyboard() -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(m.upper(), callback_data=f"method:{m}") for m in ("http", "socks4", "socks5")]
    return InlineKeyboardMarkup([
        row,
        [InlineKeyboardButton("SOCKS (4 + 5)", callback_data="method:socks")],
        [InlineKeyboardButton("‹ Back", callback_data="menu:main")],
    ])


def _country_select_keyboard(method: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("🌍 Random (all countries)", callback_data=f"csel:{method}:ALL")]]
    buttons = [InlineKeyboardButton(c, callback_data=f"csel:{method}:{i}") for i, c in enumerate(TOP_COUNTRIES)]
    rows += [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([InlineKeyboardButton("‹ Back", callback_data="menu:method")])
    return InlineKeyboardMarkup(rows)


def _resolve_country_token(token: str) -> str | None:
    if token == "ALL":
        return None
    return TOP_COUNTRIES[int(token)]


def _live_limit_keyboard(method: str, country_token: str) -> InlineKeyboardMarkup:
    row = [InlineKeyboardButton(str(n), callback_data=f"limit:{method}:{country_token}:{n}") for n in LIVE_LIMIT_CHOICES]
    return InlineKeyboardMarkup([
        row,
        [InlineKeyboardButton("♾ No limit (MAX)", callback_data=f"limit:{method}:{country_token}:0")],
        [InlineKeyboardButton("✏️ Custom number...", callback_data=f"limit:{method}:{country_token}:custom")],
        [InlineKeyboardButton("‹ Back", callback_data=f"menu:country:{method}")],
    ])


def _stop_keyboard(job_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏹ Stop", callback_data=f"stop:{job_id}")]])


def _finish_keyboard(job_id: str | None, countries: list[str]) -> InlineKeyboardMarkup:
    rows = []
    if job_id and countries:
        buttons = [InlineKeyboardButton(c, callback_data=f"cf:{job_id}:{i}") for i, c in enumerate(countries)]
        rows += [buttons[i:i + 3] for i in range(0, len(buttons), 3)]
    rows.append([
        InlineKeyboardButton("🚀 New Scrape", callback_data="menu:scrape"),
        InlineKeyboardButton("🏠 Menu", callback_data="menu:main"),
    ])
    return InlineKeyboardMarkup(rows)


async def _safe_edit(bot, chat_id, message_id, text, reply_markup=None):
    """Edit a status message, tolerating the routine failure modes: no-op edits, flood control, blocked bot."""
    try:
        await bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text,
            parse_mode=ParseMode.HTML, reply_markup=reply_markup,
        )
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            logger.warning("Failed to edit status message in chat %s: %s", chat_id, e)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 0.5)
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text,
                parse_mode=ParseMode.HTML, reply_markup=reply_markup,
            )
        except Exception:
            logger.warning("Failed to edit status message in chat %s after flood-control wait", chat_id)
    except Forbidden:
        pass  # user blocked the bot or left the chat, nothing more we can do


# ---------- job submission (shared by command args and buttons) ----------

async def _submit_job(bot, chat_id: int, method: str, live_limit, country_filter: str | None = None):
    if chat_id in chats_with_job:
        markup = _stop_keyboard("current") if active_job else None
        await bot.send_message(
            chat_id,
            "⚠️ <b>Already Running</b>\nYou already have a job queued or in progress. "
            "Stop it, or wait for it to finish.",
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
        return

    chats_with_job.add(chat_id)
    queue_position = job_queue.qsize() + (1 if active_job else 0)
    await job_queue.put(Job(chat_id=chat_id, method=method, live_limit=live_limit, country_filter=country_filter))

    details = _card([
        ("Type", method.upper()),
        ("Country", country_filter or "Random (all)"),
        ("Target", f"{live_limit} live" if live_limit else "No limit (MAX)"),
    ])

    if queue_position == 0:
        header = "✅ <b>Job Started</b>"
    else:
        header = f"⏳ <b>Job Queued</b>, position {queue_position + 1}, starts automatically."
    await bot.send_message(chat_id, f"{header}\n{details}", parse_mode=ParseMode.HTML)


# ---------- commands ----------

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await storage.touch_user(update.effective_chat.id)
    await update.message.reply_text(
        await _main_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_main_menu_keyboard()
    )


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if active_job is None:
        await update.message.reply_text("ℹ️ No job is currently running.")
        return
    active_job.stop_event.set()
    await update.message.reply_text("⏹ <b>Stop Requested</b>\nThe current batch will finish, then the job will stop.", parse_mode=ParseMode.HTML)


def _format_proxy_details(r: dict) -> str:
    proxy = html.escape(r["proxy"])
    if not r["is_live"]:
        error = r.get("error", "not responding")
        return (
            f"❌ <b>{proxy}</b>\n"
            f"{_card([('Status', 'Dead'), ('Method', r['method'].upper()), ('Reason', error)])}"
        )

    location = ", ".join(filter(None, [r.get("city"), r.get("region"), r.get("country")])) or "Unknown"
    rows = [
        ("Status", "Live ✅"),
        ("Method", r["method"].upper()),
        ("Ping", f"{r['ping_ms']}ms"),
        ("Location", location),
    ]
    if r.get("isp"):
        rows.append(("ISP", r["isp"]))
    if r.get("org") and r["org"] != r.get("isp"):
        rows.append(("Org", r["org"]))
    if r.get("asn"):
        rows.append(("ASN", r["asn"]))
    return f"✅ <b>{proxy}</b>\n{_card(rows)}"


def _parse_check_input(text: str):
    parts = text.strip().split()
    if not parts or not PROXY_INPUT_RE.match(parts[0]):
        return None
    proxy = parts[0]
    method = parts[1].lower() if len(parts) > 1 else "http"
    if method not in VALID_METHODS:
        return None
    return proxy, method


async def _run_check(bot, chat_id: int, proxy: str, method: str):
    msg = await bot.send_message(chat_id, f"🔎 Checking <code>{html.escape(proxy)}</code>…", parse_mode=ParseMode.HTML)
    result = await lookup_proxy_details(proxy, method)
    await _safe_edit(bot, msg.chat_id, msg.message_id, _format_proxy_details(result))


async def button_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if update.effective_chat.type != "private":
        await query.answer()
        return  # this bot only operates in DMs - every button we send is already private-only,
        # this just makes sure a callback can never do anything if the bot ends up in a group

    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("menu:"):
        action = data.split(":", 1)[1]

        if action == "main":
            await query.edit_message_text(await _main_menu_text(), parse_mode=ParseMode.HTML, reply_markup=_main_menu_keyboard())

        elif action == "help":
            await query.edit_message_text(HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=_back_keyboard("menu:main"))

        elif action == "settings":
            await query.edit_message_text(SETTINGS_TEXT, parse_mode=ParseMode.HTML, reply_markup=_settings_keyboard())

        elif action == "profile":
            db_stats = await storage.get_stats()
            user_stats = await storage.get_user_stats(chat_id)
            await query.edit_message_text(
                _profile_text(query.from_user, db_stats, user_stats),
                parse_mode=ParseMode.HTML,
                reply_markup=_back_keyboard("menu:main"),
            )

        elif action == "backup":
            backup = await storage.export_backup()
            total = sum(len(rows) for rows in backup.values())
            if total == 0:
                await context.bot.send_message(chat_id, "ℹ️ Nothing to back up yet. Both lists are empty.")
                return

            backup_text = await storage.export_backup_text()
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            backup_path = os.path.join(OUTPUT_DIR, f"proxybot_backup_{int(time.time())}.jsonl")
            with open(backup_path, "w") as f:
                f.write(backup_text)
            with open(backup_path, "rb") as f:
                await context.bot.send_document(
                    chat_id, document=f, filename=os.path.basename(backup_path),
                    caption=(
                        f"💾 Backup ready, {len(backup['dead_proxies'])} dead, "
                        f"{len(backup['active_proxies'])} active proxies."
                    ),
                )
            os.remove(backup_path)

        elif action == "restore":
            pending_restore.add(chat_id)
            await query.edit_message_text(RESTORE_PROMPT_TEXT, parse_mode=ParseMode.HTML, reply_markup=_back_keyboard("menu:settings"))

        elif action == "stats":
            stats = await storage.get_stats()
            await query.edit_message_text(_stats_text(stats), parse_mode=ParseMode.HTML, reply_markup=_back_keyboard("menu:settings"))

        elif action == "clean_dead":
            stats = await storage.get_stats()
            total_dead = sum(stats["dead"].values())
            if total_dead == 0:
                await query.answer("The dead list is already empty.", show_alert=True)
                return
            await query.edit_message_text(
                f"🧹 <b>Clean Dead List</b>\n\n"
                f"This removes all <b>{total_dead:,}</b> known-dead proxies from memory. "
                f"They'll get a fresh chance on the next scrape instead of being skipped. Continue?",
                parse_mode=ParseMode.HTML,
                reply_markup=_confirm_keyboard("menu:clean_dead_confirm"),
            )

        elif action == "clean_dead_confirm":
            removed = await storage.clear_dead_list()
            await query.edit_message_text(
                f"✅ <b>Dead List Cleared</b>\n{removed:,} entries removed.",
                parse_mode=ParseMode.HTML,
                reply_markup=_back_keyboard("menu:settings"),
            )

        elif action in ("scrape", "method"):
            await query.edit_message_text(
                "🚀 <b>Start a Scrape</b>\nStep 1 of 3: choose a proxy type",
                parse_mode=ParseMode.HTML,
                reply_markup=_method_keyboard(),
            )

        elif action == "check":
            pending_check.add(chat_id)
            await query.edit_message_text(CHECK_PROMPT_TEXT, parse_mode=ParseMode.HTML, reply_markup=_back_keyboard("menu:main"))

        elif action == "stop":
            if active_job is None:
                await query.answer("No job is currently running.", show_alert=True)
            else:
                active_job.stop_event.set()
                await query.answer("Stop requested.")
                await query.edit_message_text(
                    "⏹ <b>Stop Requested</b>\nThe current batch will finish, then the job will stop.",
                    parse_mode=ParseMode.HTML,
                )

        elif action.startswith("country:"):
            method = action.split(":", 1)[1]
            await query.edit_message_text(
                f"🚀 <b>Start a Scrape</b>\nMethod: <b>{method.upper()}</b>\nStep 2 of 3: choose a country",
                parse_mode=ParseMode.HTML,
                reply_markup=_country_select_keyboard(method),
            )

    elif data.startswith("method:"):
        method = data.split(":", 1)[1]
        await query.edit_message_text(
            f"🚀 <b>Start a Scrape</b>\nMethod: <b>{method.upper()}</b>\nStep 2 of 3: choose a country",
            parse_mode=ParseMode.HTML,
            reply_markup=_country_select_keyboard(method),
        )

    elif data.startswith("csel:"):
        _, method, token = data.split(":", 2)
        country = _resolve_country_token(token)
        label = country or "Random (all)"
        await query.edit_message_text(
            f"🚀 <b>Start a Scrape</b>\nMethod: <b>{method.upper()}</b> | Country: <b>{html.escape(label)}</b>\n"
            f"Step 3 of 3: how many live proxies?",
            parse_mode=ParseMode.HTML,
            reply_markup=_live_limit_keyboard(method, token),
        )

    elif data.startswith("limit:"):
        _, method, country_token, limit_str = data.split(":", 3)
        country = _resolve_country_token(country_token)

        if limit_str == "custom":
            pending_custom_limit[chat_id] = (method, country)
            await query.edit_message_text(
                f"✏️ <b>Custom Limit</b>\nMethod: <b>{method.upper()}</b>\n"
                f"Type how many live proxies you want (e.g. <code>29</code>):",
                parse_mode=ParseMode.HTML,
            )
            return

        live_limit = int(limit_str) or None
        await query.edit_message_text(
            f"✅ Method: <b>{method.upper()}</b> | Live limit: <b>{live_limit or 'MAX'}</b>", parse_mode=ParseMode.HTML
        )
        await _submit_job(context.bot, chat_id, method, live_limit, country)

    elif data.startswith("stop:"):
        job_id = data.split(":", 1)[1]
        if active_job and (job_id == "current" or active_job.id == job_id):
            active_job.stop_event.set()
            await query.answer("Stop requested", show_alert=False)
        else:
            await query.answer("That job already finished.", show_alert=False)

    elif data.startswith("cf:"):
        _, job_id, idx_str = data.split(":", 2)
        cached = completed_results.get(job_id)
        if not cached:
            await query.answer("Results expired, please run a new scrape.", show_alert=True)
            return

        live_results, countries = cached
        country = countries[int(idx_str)]
        filtered = [r for r in live_results if r.country == country]

        filtered.sort(key=lambda r: r.ping_ms)  # fastest first
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_path = os.path.join(OUTPUT_DIR, f"live_proxies_{job_id}_{country}.txt")
        with open(output_path, "w") as f:
            f.writelines(f"{r.format_line()}\n" for r in filtered)
        with open(output_path, "rb") as f:
            await context.bot.send_document(
                chat_id, document=f, filename=os.path.basename(output_path),
                caption=f"🌍 {len(filtered)} live proxies in {country}",
            )


async def restore_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id not in pending_restore:
        return  # not something we're waiting on, ignore

    pending_restore.discard(chat_id)
    document = update.message.document
    if not document or not (document.file_name.endswith(".jsonl") or document.file_name.endswith(".json")):
        await update.message.reply_text("⚠️ That doesn't look like a backup file. Restore cancelled.", parse_mode=ParseMode.HTML)
        return

    try:
        file = await document.get_file()
        raw = await file.download_as_bytearray()
        text = bytes(raw).decode("utf-8", errors="replace")
        counts = await storage.import_backup_text(text)
    except Exception:
        logger.exception("Restore failed for chat %s", chat_id)
        await update.message.reply_text("⚠️ <b>Restore Failed</b>\nThe file may be corrupted or in the wrong format.", parse_mode=ParseMode.HTML)
        return

    details = _card([
        ("New dead", str(counts.get("dead_proxies", 0))),
        ("New active", str(counts.get("active_proxies", 0))),
        ("Already known", f"{counts.get('skipped_duplicates', 0)} (skipped)"),
    ])
    await update.message.reply_text(
        f"✅ <b>Restore Complete</b>\n{details}", parse_mode=ParseMode.HTML, reply_markup=_back_keyboard("menu:settings")
    )


# ---------- free-text input (custom limit / check-proxy flows) ----------

async def text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    if chat_id in pending_custom_limit:
        method, country = pending_custom_limit[chat_id]
        if not text.isdigit() or int(text) <= 0:
            await update.message.reply_text("⚠️ Please send a positive whole number (e.g. 29), or /start to go back to the menu.")
            return
        pending_custom_limit.pop(chat_id, None)
        await _submit_job(context.bot, chat_id, method, int(text), country)
        return

    if chat_id in pending_check:
        parsed = _parse_check_input(text)
        if not parsed:
            await update.message.reply_text(
                "⚠️ That doesn't look right. Send it as <code>ip:port</code>, e.g. <code>1.2.3.4:8080</code> "
                "(optionally add a method: <code>1.2.3.4:8080 socks5</code>).",
                parse_mode=ParseMode.HTML,
            )
            return
        pending_check.discard(chat_id)
        proxy, method = parsed
        await _run_check(context.bot, chat_id, proxy, method)
        return

    # not waiting on anything from this chat - ignore stray text


# ---------- background worker ----------

async def _on_progress(bot, job: Job, checked, live, total, label: str = "Checking proxies"):
    now = time.monotonic()
    if checked < total and now - job.last_edit_at < PROGRESS_EDIT_MIN_INTERVAL:
        return  # too soon since the last edit; the next batch (or the final message) will catch up
    job.last_edit_at = now

    bar = _progress_bar(checked, total)
    pct = 0 if total == 0 else int(100 * checked / total)
    text = (
        f"🔎 <b>{label}</b>\n"
        f"<code>{bar}</code>  {pct}%\n"
        f"Checked <b>{checked}/{total}</b>   •   Live <b>{live}</b>"
    )
    await _safe_edit(bot, job.chat_id, job.status_message_id, text, reply_markup=_stop_keyboard(job.id))


def _progress_bar(checked: int, total: int) -> str:
    filled = 0 if total == 0 else int(PROGRESS_BAR_WIDTH * checked / total)
    return "▓" * filled + "░" * (PROGRESS_BAR_WIDTH - filled)


def _build_summary(live_results, sources_ok, sources_total, elapsed_s, stopped_early, skipped_dead):
    countries = Counter(r.country for r in live_results)
    avg_ping = int(sum(r.ping_ms for r in live_results) / len(live_results))
    top_countries = ", ".join(f"{c} ({n})" for c, n in countries.most_common(5)) or "Unknown"

    fastest = sorted(live_results, key=lambda r: r.ping_ms)[:10]  # lowest ms first
    preview = "\n".join(html.escape(r.format_line()) for r in fastest)

    status = "⏹ Stopped Early" if stopped_early else "✅ Job Finished"
    stats = [
        ("Live proxies", str(len(live_results))),
        ("Avg ping", f"{avg_ping}ms"),
        ("Sources used", f"{sources_ok}/{sources_total}"),
        ("Time taken", f"{elapsed_s:.1f}s"),
    ]
    if skipped_dead:
        stats.append(("Skipped dead", str(skipped_dead)))

    return (
        f"📊 <b>{status}</b>\n"
        f"{_card(stats)}\n\n"
        f"🌍 <b>Top countries</b>\n{html.escape(top_countries)}\n\n"
        f"⚡ <b>Fastest {len(fastest)}</b> (lowest ms first)\n<pre>{preview}</pre>"
    )


def _order_by_source_score(proxies: list, proxy_sources: dict, scores: dict, priority: set | None = None) -> list:
    """Best-scored sources first (so historically-better sources get checked before the
    MAX_CHECK_PER_JOB cap potentially cuts the list off). `priority` proxies (e.g. country-boost
    candidates) always come first regardless of score, since they were fetched specifically to
    satisfy the request. Unknown/untested sources default to a neutral 0.5, so a brand-new
    source isn't buried under proven ones nor unfairly favored over them."""
    priority = priority or set()
    head = [p for p in proxies if p in priority]
    tail = [p for p in proxies if p not in priority]
    tail.sort(key=lambda p: -scores.get(proxy_sources.get(p, "unknown"), 0.5))
    return head + tail


async def job_worker(app: Application):
    global active_job
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    while True:
        job = await job_queue.get()
        active_job = job
        start_time = time.monotonic()
        try:
            status_msg = await app.bot.send_message(
                job.chat_id, f"🔍 <b>Starting {job.method.upper()} Search</b>\nWarming up…", parse_mode=ParseMode.HTML
            )
            job.status_message_id = status_msg.message_id

            live_results: list = []        # every live CheckResult found this job (any country)
            seen_proxies: set = set()      # proxies already checked this job - never re-check twice
            sources_ok, sources_total = 0, 0
            skipped_dead = 0

            def target_met() -> bool:
                if not job.live_limit:
                    return False
                if job.country_filter:
                    needle = job.country_filter.lower()
                    return sum(1 for r in live_results if needle in r.country.lower()) >= job.live_limit
                return len(live_results) >= job.live_limit

            async def on_progress(checked, live, total, dead_batch, live_batch, source_batch, _job=job, _label="Checking proxies"):
                if dead_batch:
                    await storage.mark_dead_bulk(dead_batch, _job.method)
                if live_batch:
                    await storage.mark_active_bulk(live_batch, _job.method)
                if source_batch:
                    await storage.bump_source_stats(source_batch)
                await _on_progress(app.bot, _job, checked, live, total, _label)

            # ---- Step 1: re-verify proxies already known active in the DB - no scraping needed ----
            db_active = await storage.get_active_proxies(job.method, job.country_filter)
            if db_active:
                seen_proxies.update(db_active)
                await _safe_edit(
                    app.bot, job.chat_id, job.status_message_id,
                    f"♻️ <b>Re-checking Database</b>\nVerifying {len(db_active)} previously-active proxies first…",
                    reply_markup=_stop_keyboard(job.id),
                )
                recheck_progress = lambda *a: on_progress(*a, _label="Re-checking known-active proxies")
                recheck_live = await check_proxies(db_active, job.method, job.stop_event, job.live_limit, recheck_progress)
                live_results.extend(recheck_live)

            # ---- Step 2: scrape + check fresh proxies, repeating rounds until the target is met ----
            round_num = 0
            gave_up_no_fresh = False
            while not job.stop_event.is_set() and not target_met() and round_num < MAX_SCRAPE_ROUNDS:
                round_num += 1
                await _safe_edit(
                    app.bot, job.chat_id, job.status_message_id,
                    f"🔍 <b>Scraping: Round {round_num}/{MAX_SCRAPE_ROUNDS}</b>\nPulling fresh proxies from all sources…",
                )

                proxies, proxy_sources, sources_ok, sources_total = await scrape_all(job.method)

                if round_num == 1 and not proxies:
                    if not live_results:
                        await _safe_edit(
                            app.bot, job.chat_id, job.status_message_id,
                            "❌ <b>No Proxies Found</b>\nEvery source came up empty. Please try again shortly.",
                        )
                        raise LookupError("no_sources")
                    break

                dead_set = await storage.get_dead_set(job.method)
                fresh = [p for p in proxies if p not in dead_set and p not in seen_proxies]

                boost_note = ""
                boosted_fresh = []
                if round_num == 1 and job.country_filter:
                    iso = COUNTRY_ISO.get(job.country_filter.lower())
                    if iso:
                        boosted = await scrape_country_boost(job.method, iso)
                        boosted_fresh = [p for p in boosted if p not in dead_set and p not in seen_proxies]
                        if boosted_fresh:
                            boosted_set = set(boosted_fresh)
                            fresh = boosted_fresh + [p for p in fresh if p not in boosted_set]
                            for p in boosted_fresh:
                                proxy_sources.setdefault(p, "country-boost")
                            boost_note = f"\n🎯 +{len(boosted_fresh)} candidates fetched specifically for {job.country_filter}"

                if not fresh:
                    gave_up_no_fresh = True
                    break  # nothing new left to try anywhere - stop looping instead of spinning forever

                skipped_dead += len(proxies) - len(fresh)
                seen_proxies.update(fresh)

                scores = await storage.get_source_scores()
                fresh = _order_by_source_score(fresh, proxy_sources, scores, priority=set(boosted_fresh))

                if len(fresh) > MAX_CHECK_PER_JOB:
                    fresh = fresh[:MAX_CHECK_PER_JOB]  # best-scored (and boosted) ones are already at the front

                await _safe_edit(
                    app.bot, job.chat_id, job.status_message_id,
                    f"✅ Scraped <b>{len(proxies)}</b> proxies from <b>{sources_ok}/{sources_total}</b> sources\n"
                    f"⏭ Checking <b>{len(fresh)}</b> fresh proxies, round {round_num}/{MAX_SCRAPE_ROUNDS}{boost_note}",
                    reply_markup=_stop_keyboard(job.id),
                )

                round_progress = lambda *a, _r=round_num: on_progress(*a, _label=f"Checking: Round {_r}/{MAX_SCRAPE_ROUNDS}")
                round_live = await check_proxies(
                    fresh, job.method, job.stop_event, job.live_limit, round_progress, proxy_sources
                )
                live_results.extend(round_live)

            elapsed = time.monotonic() - start_time

            if job.live_limit and len(live_results) > job.live_limit and not job.country_filter:
                live_results = live_results[:job.live_limit]  # never return more than what was ordered

            if not live_results:
                await _safe_edit(
                    app.bot, job.chat_id, job.status_message_id,
                    "❌ <b>No Live Proxies Found</b>\nEvery result was already known dead. Try again shortly, or a different country.",
                )
                continue

            all_live_results = live_results
            if job.country_filter:
                needle = job.country_filter.lower()
                live_results = [r for r in all_live_results if needle in r.country.lower()]
                if job.live_limit and len(live_results) > job.live_limit:
                    live_results = live_results[:job.live_limit]
                if not live_results:
                    await _safe_edit(
                        app.bot, job.chat_id, job.status_message_id,
                        f"❌ <b>No Matches in {html.escape(job.country_filter)}</b>\n"
                        f"{len(all_live_results)} live proxies found in other countries though. Try Random next time.",
                    )
                    continue

            unmet_note = ""
            if job.live_limit and len(live_results) < job.live_limit:
                reason = "no proxies left to try" if gave_up_no_fresh else f"stopped after {MAX_SCRAPE_ROUNDS} rounds"
                unmet_note = f"\n⚠️ Only found {len(live_results)}/{job.live_limit} requested ({reason})."

            await storage.bump_user_job(job.chat_id, len(live_results))

            await _safe_edit(
                app.bot, job.chat_id, job.status_message_id,
                f"✅ <b>Finished</b>: {len(live_results)} live proxies in {elapsed:.1f}s{unmet_note}",
            )

            summary = _build_summary(
                live_results, sources_ok, sources_total, elapsed, job.stop_event.is_set(), skipped_dead
            ) + unmet_note

            job_id_for_buttons = None
            countries: list[str] = []
            if not job.country_filter:
                countries = [c for c, _ in Counter(r.country for r in live_results).most_common(TOP_COUNTRY_BUTTONS)]
                if len(countries) > 1:
                    _remember_results(job.id, live_results, countries)
                    job_id_for_buttons = job.id
                    summary += "\n\n🔽 Filter by country, or start again:"
                else:
                    summary += "\n\nWhat's next?"
            else:
                summary += "\n\nWhat's next?"

            await app.bot.send_message(
                job.chat_id, summary, parse_mode=ParseMode.HTML,
                reply_markup=_finish_keyboard(job_id_for_buttons, countries),
            )

            output_path = os.path.join(OUTPUT_DIR, f"live_proxies_{job.chat_id}_{job.method}.txt")
            sorted_results = sorted(live_results, key=lambda r: r.ping_ms)  # fastest (lowest ms) first
            with open(output_path, "w") as f:
                f.writelines(f"{r.format_line()}\n" for r in sorted_results)
            with open(output_path, "rb") as f:
                await app.bot.send_document(
                    job.chat_id, document=f, filename=os.path.basename(output_path),
                    caption=f"📄 {len(live_results)} live proxies",
                )
        except LookupError:
            pass  # "no proxies found from any source" already reported to the user above
        except Exception:
            logger.exception("Job failed for chat %s", job.chat_id)
            try:
                await app.bot.send_message(job.chat_id, "⚠️ <b>Job Failed</b>\nAn internal error occurred, please try again.", parse_mode=ParseMode.HTML)
            except Exception:
                pass
        finally:
            active_job = None
            chats_with_job.discard(job.chat_id)
            job_queue.task_done()


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception while processing an update", exc_info=context.error)


async def post_init(app: Application):
    await app.bot.set_my_commands([
        ("start", "Open the main menu"),
        ("stop", "Stop the running job after its current batch"),
    ])
    asyncio.create_task(job_worker(app))


def main():
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    app.add_error_handler(error_handler)

    # Every command works in a private chat with the bot only - never in any group.
    app.add_handler(CommandHandler("start", start_cmd, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("stop", stop_cmd, filters=filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(button_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, text_input_handler))
    app.add_handler(MessageHandler(filters.Document.ALL & filters.ChatType.PRIVATE, restore_document_handler))

    # Python 3.14 removed implicit event-loop creation (PEP 719); PTB 21.x still calls
    # asyncio.get_event_loop() internally, so we create one up front if none exists.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    app.run_polling()


if __name__ == "__main__":
    main()
