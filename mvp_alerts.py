#!/usr/bin/env python3
"""
mvp_alerts.py

MarketAux-based news scanner for a watchlist of tickers.
- Batches queries (BATCH_SIZE tickers per MarketAux call)
- Weighted severity model (HIGH alerts -> Discord; MED -> logged)
- Balanced Filter Mode (2)
- Smart cooldowns and rate limiting
- Robust error handling, logging, and retries
"""

import os
import sys
import time
import json
import math
import logging
import random
import signal
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from typing import List, Dict, Any, Tuple

import requests

# -----------------------
# CONFIG (tweak these)
# -----------------------
MARKETAUX_API_KEY = "9Ydp4VNIm9zZ6WHmVcys40L9gUlUWOKW6ZYFxX2T"  # <-- your key (from message)
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"  # set your webhook url here to enable posting
WATCHLIST_FILE = "watchlist.txt"
COMPANY_MAP_FILE = "company_map.json"  # optional map ticker -> company name
LOGFILE = "bot.log"
#USER_AGENT = {"User-Agent": "StockAI-Bot/2.0 (ajakovski@yahoo.com)"}

BATCH_SIZE = 10  # max tickers per marketaux call (you asked for max efficiency)
CYCLE_SLEEP_SECONDS = 3600  # default time between cycles (seconds). Adjust later.
CYCLE_INFINITE = True  # keep looping; change to False for single run

MARKETAUX_URL = "https://api.marketaux.com/v1/news/all"
MARKETAUX_LIMIT_PER_CALL = 50  # marketaux page limit, we'll request enough articles per batch

# Filtering / severity
FILTER_MODE = 2  # Balanced Mode
ONLY_POST_HIGH = True  # only send HIGH to discord
MED_LOG_ONLY = True

# Cooldown & rate limiting
TICKER_COOLDOWN_MINUTES = 60  # per-ticker cooldown after posting HIGH
SMART_COOLDOWN_BASE = 60  # base in minutes; will increase if many posts in short time
DAILY_REQUEST_LIMIT = 100  # MarketAux free plan (requests/day)
REQUESTS_BUFFER = 5  # keep some buffer below limit

# Retry/backoff
MAX_RETRIES = 4
BACKOFF_BASE = 1.5

# Logging setup
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO,
                    format=LOG_FORMAT,
                    handlers=[
                        logging.FileHandler(LOGFILE, mode="a", encoding="utf-8"),
                        logging.StreamHandler(sys.stdout)
                    ])
logger = logging.getLogger("mvp_alerts")

# -----------------------
# Keyword weights (broader)
# Larger list, tuned for company-impact words.
# You can edit or expand this dictionary as you wish.
# -----------------------
keyword_weights = {
    # corporate actions / finance (high impact)
    "acquires": 5, "acquisition": 5, "merger": 5, "buyout": 5, "takeover": 5,
    "ipo": 5, "bankruptcy": 6, "filed for bankruptcy": 6, "ceo resign": 4, "ceo steps down": 4,
    "layoff": 5, "mass layoff": 6, "restructuring": 4, "divest": 4, "spin-off": 4,
    "earnings": 5, "beat expectations": 5, "missed expectations": 5, "guidance lowered": 5,
    "guidance raised": 5, "revenue miss": 5, "revenue beat": 5, "cut dividend": 5,
    "major contract": 5, "contract award": 4, "fraud": 7, "lawsuit": 5, "sec investigation": 6,
    "insider trading": 6, "shareholder": 3, "buyback": 4,
    # regulatory / macro (can be medium-high)
    "fined": 5, "settlement": 5, "regulator": 4, "recall": 6, "safety issue": 5,
    # product / technology / supply chain (medium-high)
    "outage": 5, "cyberattack": 6, "data breach": 6, "recall": 6, "launch": 3,
    "partnership": 3, "contract": 3, "supply chain": 4, "shortage": 4,
    # major market movers or sentiment terms (medium)
    "downgrades": 4, "upgrades": 4, "analytic upgrade": 4, "price target": 3,
    "merger talks": 5, "explores": 4, "plans to": 3,
    # geo/political keywords (filtered depending on mode)
    "sanction": 5, "trade war": 5, "tariff": 4, "government": 2,
    # crypto / fintech specific
    "token": 4, "exchange": 3, "insolvency": 6,
    # generic signals (low to medium)
    "interim ceo": 4, "management change": 4, "cut guidance": 5, "raise guidance": 5
}

# Lowercase & expand keywords (we'll treat keys lowercased)
keyword_weights = {k.lower(): v for k, v in keyword_weights.items()}

# -----------------------
# Helper utilities
# -----------------------
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def dt_from_iso(s: str) -> datetime:
    try:
        # Accept many ISO variants
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(tz=None)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return datetime.utcnow()

# -----------------------
# Request / Rate tracking
# -----------------------
class RequestTracker:
    def __init__(self, daily_limit: int):
        self.daily_limit = daily_limit
        self.window_start = datetime.utcnow().date()
        self.count = 0

    def increment(self, n=1) -> bool:
        today = datetime.utcnow().date()
        if today != self.window_start:
            self.window_start = today
            self.count = 0
        if self.count + n > self.daily_limit - REQUESTS_BUFFER:
            return False
        self.count += n
        return True

    def remaining(self) -> int:
        today = datetime.utcnow().date()
        if today != self.window_start:
            return self.daily_limit - REQUESTS_BUFFER
        return max(0, self.daily_limit - REQUESTS_BUFFER - self.count)

request_tracker = RequestTracker(DAILY_REQUEST_LIMIT)

# -----------------------
# Load watchlist & company map
# -----------------------
def load_watchlist(path: str) -> List[str]:
    if not os.path.exists(path):
        logger.error("watchlist.txt not found at %s", path)
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip().upper() for line in f if line.strip()]
    logger.info("Loaded %d personal tickers from %s.", len(lines), path)
    return lines

def load_company_map(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        logger.warning("company_map.json not found; continuing with tickers as names.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning("Failed to load company_map.json: %s", e)
        return {}

company_map = load_company_map(COMPANY_MAP_FILE)

def ticker_to_name(ticker: str) -> str:
    return company_map.get(ticker.upper(), ticker.upper())

# -----------------------
# MarketAux fetcher
# -----------------------
def build_marketaux_params(symbols: List[str], published_after_iso: str = None) -> Dict[str, str]:
    params = {
        "symbols": ",".join(symbols),
        "api_token": MARKETAUX_API_KEY,
        "language": "en",
        "limit": str(MARKETAUX_LIMIT_PER_CALL),
        "filter_entities": "true"
    }
    if published_after_iso:
        params["published_after"] = published_after_iso
    return params

def marketaux_fetch_batch(symbols: List[str], published_after_iso: str = None) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Returns (articles, success_flag). Articles is a list of dicts (raw MarketAux items).
    """
    params = build_marketaux_params(symbols, published_after_iso)
    url = MARKETAUX_URL
    attempt = 0
    while attempt <= MAX_RETRIES:
        # rate guard
        if not request_tracker.increment():
            logger.error("Daily request budget exhausted (or near limit). Skipping fetch.")
            return [], False
        try:
            r = requests.get(url, params=params, timeout=18)
            if r.status_code == 200:
                data = r.json()
                # MarketAux returns 'data' array (best-effort parsing)
                articles = data.get("data") if isinstance(data, dict) else []
                if articles is None:
                    articles = []
                logger.info("MarketAux: fetched %d articles for batch (%s).", len(articles), ",".join(symbols))
                return articles, True
            else:
                # parse helpful error
                try:
                    err = r.json()
                except Exception:
                    err = r.text
                logger.warning("NewsData/MarketAux HTTP %d: %s", r.status_code, err)
                # 422 unsupported filters -> likely bad param; bail this batch
                if r.status_code == 422:
                    return [], False
                # 401 unauthorized -> check key
                if r.status_code == 401:
                    logger.error("UNAUTHORIZED: check API key. Response: %s", err)
                    return [], False
                # 429 rate limit -> exponential backoff
                if r.status_code == 429:
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.warning("Rate-limited (429). Backing off %.1fs (attempt %d).", wait, attempt + 1)
                    time.sleep(wait)
                    attempt += 1
                    continue
                # other 5xx -> retry
                if 500 <= r.status_code < 600:
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.warning("Server error %d. Backing off %.1fs (attempt %d).", r.status_code, wait, attempt + 1)
                    time.sleep(wait)
                    attempt += 1
                    continue
                # otherwise don't retry
                return [], False
        except requests.RequestException as e:
            wait = BACKOFF_BASE ** (attempt + 1)
            logger.warning("Request exception: %s — backing off %.1fs (attempt %d).", e, wait, attempt + 1)
            time.sleep(wait)
            attempt += 1
    logger.error("Failed to fetch after %d attempts.", MAX_RETRIES)
    return [], False

# -----------------------
# Severity logic (weighted)
# -----------------------
def score_article_for_ticker(article: Dict[str, Any], ticker: str) -> Tuple[int, Dict[str,int]]:
    """
    Returns (score, matched_keywords_counter)
    Score is numeric; we later map to LOW/MED/HIGH thresholds.
    We'll examine title, description, and entities (if provided).
    """
    score = 0
    matches = Counter()

    # Combine searchable text
    parts = []
    title = (article.get("title") or "") or ""
    description = (article.get("description") or "") or ""
    content = (article.get("content") or "") or ""
    parts.append(title.lower())
    parts.append(description.lower())
    parts.append(content.lower())

    # Entities: MarketAux may provide 'entities' list of dicts with 'name' or 'type'
    entities = []
    for e in article.get("entities", []):
        if isinstance(e, dict):
            entities.append((e.get("name","") or "").lower())
        elif isinstance(e, str):
            entities.append(e.lower())

    full_text = " ".join(parts + entities)

    # Weight by keyword occurrences (broad matching)
    for kw, w in keyword_weights.items():
        if kw in full_text:
            # number of occurrences
            count = full_text.count(kw)
            matches[kw] += count
            score += w * count

    # Boost if ticker or company name appears in title (strong indicator)
    if ticker.lower() in title.lower():
        score += 2
        matches[f"ticker_in_title_{ticker}"] += 1
    name = ticker_to_name(ticker)
    if name.lower() != ticker.lower() and name.lower() in title.lower():
        score += 2
        matches["company_in_title"] += 1

    # Boost for 'source' types like 'press release' or 'official'
    source = (article.get("source") or {}).get("name") if isinstance(article.get("source"), dict) else (article.get("source") or "")
    if source:
        s = str(source).lower()
        if "press" in s or "prnewswire" in s or "businesswire" in s:
            score += 1
            matches["press_source"] += 1

    # Time relevance boost (very recent items get a small bump)
    pub = article.get("published_at") or article.get("published")
    if pub:
        try:
            published_dt = dt_from_iso(pub)
            age_seconds = (datetime.utcnow() - published_dt).total_seconds()
            if age_seconds < 3600:
                score += 1
                matches["recent_boost"] += 1
        except Exception:
            pass

    return score, dict(matches)

def map_score_to_severity(score: int) -> str:
    """
    Map numeric score to severity string.
    Tuned conservatively: only strong signals end as HIGH.
    """
    if score >= 8:
        return "HIGH"
    if score >= 4:
        return "MED"
    return "LOW"

# -----------------------
# Filter modes
# -----------------------
def passes_filter_mode(article: Dict[str, Any], mode: int) -> bool:
    """
    Balanced Mode (2): attempt to remove geopolitical macro noise and clickbait.
    We'll examine source, categories, and keywords.
    """
    if mode == 0:
        return True  # permissive: accept everything
    text = ((article.get("title") or "") + " " + (article.get("description") or "")).lower()
    source_name = ""
    if isinstance(article.get("source"), dict):
        source_name = (article["source"].get("name") or "").lower()
    else:
        source_name = (article.get("source") or "").lower()

    # Balanced: filter out generic market coverage, opinion pieces and non-company topics
    if mode == 2:
        # drop pure macro-only headlines (e.g., "Dow closes up 300 points")
        if any(phrase in text for phrase in ["stock market today", "dow", "s&p", "nasdaq", "market today", "wall street"]):
            return False
        # drop general economic reports
        if any(phrase in text for phrase in ["cpi", "interest rate", "fed minutes", "fed decision", "unemployment rate", "gdp"]):
            return False
        # drop clickbait sources that aren't company-specific if title is generic
        if ("opinion" in source_name or "forbes" in source_name) and len(text.split()) < 6:
            return False
    # Strict mode can be added later
    return True

# -----------------------
# Discord posting
# -----------------------
def post_to_discord(content: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        logger.info("Discord webhook not configured; skipping post. Content: %s", content[:200])
        return False
    payload = {"content": content}
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if r.status_code in (200, 204):
            logger.info("Discord posted: %s", content.splitlines()[0] if content else "n/a")
            return True
        else:
            logger.warning("Discord post failed %d: %s", r.status_code, r.text)
            return False
    except Exception as e:
        logger.warning("Discord post exception: %s", e)
        return False

# -----------------------
# Smart cooldown manager
# -----------------------
class CooldownManager:
    def __init__(self, base_minutes: int = TICKER_COOLDOWN_MINUTES):
        self.base = base_minutes
        self.last_post = {}  # ticker -> datetime
        self.recent_post_counts = []  # timestamps of any posts to detect bursts

    def allowed_to_post(self, ticker: str) -> bool:
        now = datetime.utcnow()
        last = self.last_post.get(ticker)
        if not last:
            return True
        elapsed = (now - last).total_seconds() / 60.0
        # dynamic cooldown: lengthen cooldown if many posts in last hour
        burst = self.count_posts_since(minutes=60)
        multiplier = 1 + (burst // 5)  # for every 5 posts in last hour, multiply cooldown
        effective_cd = self.base * multiplier
        if elapsed >= effective_cd:
            return True
        else:
            return False

    def record_post(self, ticker: str):
        now = datetime.utcnow()
        self.last_post[ticker] = now
        self.recent_post_counts.append(now)
        # prune to last 24h for memory
        cutoff = now - timedelta(hours=24)
        self.recent_post_counts = [t for t in self.recent_post_counts if t >= cutoff]

    def count_posts_since(self, minutes: int = 60) -> int:
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=minutes)
        return sum(1 for t in self.recent_post_counts if t >= cutoff)

cooldowns = CooldownManager()

# -----------------------
# Main loop flow
# -----------------------
def process_articles_for_batch(articles: List[Dict[str, Any]], tickers: List[str], published_after_iso: str = None) -> Tuple[int, int, int]:
    """
    Process a list of articles returned for the entire batch.
    We map articles to tickers (MarketAux returns 'entities' - we'll trust them).
    Returns counts: (fetched_count, kept_count, posted_count_high)
    """
    fetched = len(articles)
    kept = 0
    posted = 0

    # Group articles by ticker: check entities first; as fallback check title for ticker/name mention
    ticker_articles = defaultdict(list)
    for art in articles:
        mapped_tickers = set()
        # If entities provided, find tickers or company names in entities
        for ent in art.get("entities", []):
            if isinstance(ent, dict):
                name = (ent.get("name") or "").upper()
                # If entity equals ticker-like
                if name in tickers:
                    mapped_tickers.add(name)
                # Map company_map reverse lookup
                for t, nm in company_map.items():
                    if nm and nm.lower() in name.lower():
                        mapped_tickers.add(t.upper())
            elif isinstance(ent, str):
                e = ent.upper()
                if e in tickers:
                    mapped_tickers.add(e)
        # fallback: check title for ticker or company name mention
        title = (art.get("title") or "").upper()
        for t in tickers:
            if t in title:
                mapped_tickers.add(t)
            else:
                nm = ticker_to_name(t).upper()
                if nm and nm != t and nm in title:
                    mapped_tickers.add(t)
        # If still unmapped, we append to a "generic" bucket (we'll try to score for each ticker)
        if not mapped_tickers:
            # assign to every ticker in the batch as fallback (scored separately)
            for t in tickers:
                ticker_articles[t].append(art)
        else:
            for t in mapped_tickers:
                ticker_articles[t].append(art)

    # Now per ticker, score articles
    for t in tickers:
        arts = ticker_articles.get(t, [])
        for art in arts:
            # filter mode
            if not passes_filter_mode(art, FILTER_MODE):
                continue
            score, matches = score_article_for_ticker(art, t)
            severity = map_score_to_severity(score)
            # Log details for kept (MED or HIGH). LOW -> drop
            if severity == "LOW":
                continue
            kept += 1
            headline = art.get("title") or art.get("description") or "(no title)"
            source = art.get("source") or {}
            source_name = source.get("name") if isinstance(source, dict) else (source or "")
            published = art.get("published_at") or art.get("published") or ""
            url = art.get("link") or art.get("url") or art.get("source_url") or ""
            # Compose summary log line
            log_line = f"{t} | {severity} | score={score} | {ticker_to_name(t)} — {headline} {('- ' + url) if url else ''}"
            if severity == "MED":
                logger.info("MED KEPT: %s", log_line)
                # record MED info to separate log if needed
                # but don't post
            elif severity == "HIGH":
                # check cooldown
                if not cooldowns.allowed_to_post(t):
                    logger.info("%s: HIGH detected but cooldown active; skipping post. %s", t, log_line)
                    continue
                # send to discord if configured
                content = f"{t} | HIGH — {ticker_to_name(t)}\n{headline}\n{source_name} {published}\n{url}"
                posted_success = False
                if ONLY_POST_HIGH and DISCORD_WEBHOOK_URL:
                    posted_success = post_to_discord(content)
                else:
                    # still log if not posting
                    logger.info("HIGH (not posted): %s", log_line)
                    posted_success = True  # treat as posted for cooldown logic or count
                if posted_success:
                    posted += 1
                    cooldowns.record_post(t)
    return fetched, kept, posted

def chunk_list(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# -----------------------
# Entrypoint
# -----------------------
def main_loop():
    logger.info("========== Bot Startup ==========")
    watchlist = load_watchlist(WATCHLIST_FILE)
    if not watchlist:
        logger.error("No tickers in watchlist. Exiting.")
        return

    # make cycle tickers: user wants only the 28 personal tickers for now
    personal = watchlist
    # For hybrid/random picks, you can enable below
    # master_list = fetch_sp500_master_list()  # left out to keep simple
    # random_picks = random.sample(master_list, k=22) if master_list else []
    random_picks = []
    cycle_tickers = personal + random_picks
    logger.info("Cycle composition: %d personal + %d random = %d total.",
                len(personal), len(random_picks), len(cycle_tickers))

    # We will use 'published_after' to avoid re-fetching old articles.
    # Use a sliding window seen_time; start at (now - 6 hours) to catch recent items
    lookback_hours = 6
    published_after_dt = datetime.utcnow() - timedelta(hours=lookback_hours)
    published_after_iso = published_after_dt.isoformat() + "Z"

    cycle = 0
    try:
        while True:
            cycle += 1
            logger.info("=== Cycle #%d startup ===", cycle)
            logger.info("Loaded %d personal tickers from %s.", len(personal), WATCHLIST_FILE)
            # (Rebuild cycle tickers in case watchlist changed)
            cycle_tickers = personal + random_picks
            logger.info("Cycle tickers: %d total (%d personal + %d random).",
                        len(cycle_tickers), len(personal), len(random_picks))

            # split into batches
            batches = list(chunk_list(cycle_tickers, BATCH_SIZE))
            total_fetched = total_kept = total_posted = 0
            batch_index = 0
            for batch in batches:
                batch_index += 1
                logger.info("Fetching batch %d/%d: %s", batch_index, len(batches), batch)
                # Ensure daily request budget allows this batch
                if not request_tracker.increment(1):
                    logger.error("RequestTracker refused batch due to daily limit.")
                    break

                # Fetch articles for this batch
                articles, ok = marketaux_fetch_batch(batch, published_after_iso)
                if not ok:
                    # if the batch failed due to param or auth, move on
                    logger.warning("Batch fetch failed or returned no data; continuing to next batch.")
                    continue

                # process articles
                fetched, kept, posted = process_articles_for_batch(articles, batch, published_after_iso)
                logger.info("%s: %d fetched, %d kept, %d high posted", batch[0] if len(batch) == 1 else ",".join(batch[:1]) + "...", fetched, kept, posted)
                total_fetched += fetched
                total_kept += kept
                total_posted += posted

                # Smart cooldown increase if many posted recently
                recent_posts = cooldowns.count_posts_since(60)
                if recent_posts >= 10:
                    # make system quieter: extend per-ticker cooldown multiplier will reflect this
                    logger.info("High posting activity detected: %d posts in last 60m -> throttling further.", recent_posts)

                # small safety sleep between batches to avoid bursts (fast mode can set to 0)
                time.sleep(0.3)

            logger.info("Cycle summary: HIGH=%d MED/KEPT=%d LOW=?. posted=%d", total_posted, total_kept, total_posted)
            logger.info("Cycle %d completed. Fetched=%d Kept=%d Posted=%d", cycle, total_fetched, total_kept, total_posted)
            if not CYCLE_INFINITE:
                break
            logger.info("Sleeping %ds until next cycle.", CYCLE_SLEEP_SECONDS)
            time.sleep(CYCLE_SLEEP_SECONDS)

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down.")
    except Exception as e:
        logger.exception("Unhandled exception: %s", e)

if __name__ == "__main__":
    main_loop()
