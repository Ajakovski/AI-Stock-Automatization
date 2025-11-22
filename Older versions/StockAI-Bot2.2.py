#!/usr/bin/env python3
"""
mvp_alerts.py
Market news scanner (MarketAux) -> Discord
Features:
 - Batch fetch (10 tickers per request) using /news/all
 - Weighted severity model; only HIGH posted to Discord, MED logged/kept in SQLite
 - Balanced filter mode by default
 - Smart per-ticker cooldown to avoid spam
 - Robust timestamp formatting (Option D: use last cycle end timestamp)
 - Extended Matching (B) for ticker detection:
     1) MarketAux-provided tickers/symbols
     2) Company-name match in title/description/content
     3) URL token match
     4) Ticker substring match
     5) Fallback to first ticker in batch
 - Detailed logging to bot.log
"""

import os
import sys
import time
import json
import math
import logging
import traceback
import requests
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from dateutil import parser as dateutil_parser
from pathlib import Path

# ---------------------------------------------
# SQLite persistence for MED articles
# ---------------------------------------------
DB_PATH = "med_alerts.db"


def init_med_db():
    """Create the SQLite DB if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS med_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT,
            title TEXT,
            description TEXT,
            url TEXT,
            severity REAL,
            published_at TEXT,
            detected_at TEXT
        )
    """
    )
    conn.commit()
    conn.close()


def save_med_article(ticker, title, description, url, severity, published_at):
    """Store a MED article persistently."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO med_articles (ticker, title, description, url, severity, published_at, detected_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
    """,
        (ticker, title, description, url, severity, published_at),
    )
    conn.commit()
    conn.close()


# --------------------------
# Config / ENV
# --------------------------
MARKETAUX_API_KEY = os.getenv(
    "MARKETAUX_API_KEY", "9Ydp4VNIm9zZ6WHmVcys40L9gUlUWOKW6ZYFxX2T"
)  # set in env
DISCORD_WEBHOOK_URL = os.getenv(
    "DISCORD_WEBHOOK_URL",
    "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-",
)  # set in env if you want auto post
WATCHLIST_PATH = "watchlist.txt"
COMPANY_MAP_PATH = "company_map.json"  # optional mapping ticker->company name
LAST_TS_PATH = "last_timestamp.txt"
LOG_FILE = "bot.log"

# User-tunable parameters
BATCH_SIZE = 10  # 1 batch = 10 tickers (you requested this)
CYCLE_SECONDS = int(os.getenv("CYCLE_SECONDS", "3600"))  # default 3600s
COLD_START_HOURS = 12  # on first run, look back this many hours
SMART_COOLDOWN_MINUTES = 30  # cooldown after a posted HIGH alert (per-ticker)
MAX_RETRIES = 3  # retry for MarketAux requests
RETRY_BACKOFF_BASE = 1.5  # exponential backoff base
FILTER_MODE = int(os.getenv("FILTER_MODE", "2"))  # Balanced (2) by default
LOG_LEVEL = logging.INFO

# Severity thresholds (these can be tuned)
HIGH_THRESHOLD = 3.0
MED_THRESHOLD = 1.5

# If FILTER_MODE is Strict (3) we can bump thresholds; Balanced (2) uses defaults; Fast (1) lowers thresholds
if FILTER_MODE == 3:
    HIGH_THRESHOLD = 3.5
    MED_THRESHOLD = 2.0
elif FILTER_MODE == 1:
    HIGH_THRESHOLD = 2.5
    MED_THRESHOLD = 1.0

# --------------------------
# Expanded keyword weights (broader)
# --------------------------
keyword_weights: Dict[str, float] = {
    # Corporate actions / finance
    "acquir": 1.5,
    "acquisition": 1.5,
    "acquired": 1.5,
    "merger": 1.5,
    "takeover": 1.5,
    "lawsuit": 2.0,
    "settlement": 1.8,
    "investigation": 1.8,
    "charged": 2.5,
    "indict": 2.5,
    "bankrupt": 3.0,
    "bankruptcy": 3.0,
    "delist": 2.5,
    "insider": 1.2,
    "stake": 0.7,
    # Earnings / guidance
    "earnings": 2.0,
    "beats": 1.8,
    "misses": 1.8,
    "guidance": 1.7,
    "revenue": 1.1,
    "q1": 0.4,
    "q2": 0.4,
    "q3": 0.4,
    "q4": 0.4,
    # People / leadership
    "resign": 1.8,
    "resignation": 1.8,
    "stepping down": 1.8,
    "appoint": 1.4,
    "ceo": 1.0,
    "cfo": 1.0,
    # Market-moving terms
    "recall": 2.5,
    "accredit": 1.0,
    "regulator": 1.5,
    "sanction": 2.5,
    "fine": 2.0,
    # Macroeconomic and sector
    "offer": 0.8,
    "buyback": 1.5,
    "dividend": 1.2,
    "layoff": 2.0,
    "shutdown": 2.5,
    "bankrun": 3.0,
    "cyberattack": 2.5,
    "hack": 2.2,
    "data breach": 2.5,
    # Crypto / token / exchange relevant
    "token": 1.2,
    "suspend": 1.8,
    # Geopolitics / macro
    "tariff": 1.5,
    "embargo": 2.0,
    "regime": 0.8,
    "war": 3.0,
    # Short and sentiment
    "downgrade": 1.6,
    "upgrade": 1.6,
    "price target": 0.9,
    "fraud": 3.0,
    # Other common high-impact signals
    "ipo": 1.2,
    "filing": 0.8,
    # generic verbs / nouns that can appear frequently but assigned small weight
    "announce": 0.5,
    "launch": 0.6,
    "contract": 1.0,
    "agreement": 0.8,
    "partnership": 0.9,
}

# Pre-lowercase keys for faster matching
keyword_weights = {k.lower(): v for k, v in keyword_weights.items()}

# --------------------------
# Logging
# --------------------------
logger = logging.getLogger("mvp_alerts")
logger.setLevel(LOG_LEVEL)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)


# --------------------------
# Helpers / Utilities
# --------------------------


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def load_watchlist(path: str = WATCHLIST_PATH) -> List[str]:
    if not os.path.exists(path):
        logger.warning("watchlist.txt not found; creating empty watchlist.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.strip().upper() for line in f if line.strip()]
    logger.info(f"Loaded {len(lines)} personal tickers from {path}.")
    return lines


def load_company_map(path: str = COMPANY_MAP_PATH) -> Dict[str, str]:
    if not os.path.exists(path):
        logger.info("No company_map.json found; continuing with ticker-only names.")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Normalize keys to upper tickers
        data = {k.upper(): v for k, v in data.items()}
        logger.info(f"Loaded company map from {path}.")
        return data
    except Exception as e:
        logger.warning(f"Failed to load company_map.json: {e}")
        return {}


def read_last_timestamp(path: str = LAST_TS_PATH) -> Optional[str]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            ts = f.read().strip()
            if not ts:
                return None
            # Validate parse
            _ = dateutil_parser.parse(ts)
            return ts
    except Exception as e:
        logger.warning(f"Failed to read last timestamp: {e}")
        return None


def write_last_timestamp(ts: str, path: str = LAST_TS_PATH):
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(ts)
    except Exception as e:
        logger.warning(f"Failed to write last timestamp: {e}")


def format_timestamp_for_marketaux(dt: datetime) -> str:
    """
    MarketAux required format observed: 'YYYY-MM-DDTHH:MM:SS' (no trailing Z).
    We'll produce both variants when retrying.
    """
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "")


def try_multiple_ts_formats(dt: datetime) -> List[str]:
    # Return candidate published_after strings in order of preference
    candidates = []
    # 1) 'YYYY-MM-DDTHH:MM:SS' (no timezone)
    candidates.append(format_timestamp_for_marketaux(dt))
    # 2) 'YYYY-MM-DDTHH:MM:SSZ' (UTC with Z)
    candidates.append(dt.replace(microsecond=0).astimezone(timezone.utc).isoformat().replace("+00:00", "Z"))
    # 3) RFC3339-like (include timezone offset)
    candidates.append(dt.replace(microsecond=0).astimezone(timezone.utc).isoformat())
    return candidates


def safe_request_get(url: str, params: dict, headers: dict = None, max_retries: int = MAX_RETRIES) -> Tuple[Optional[requests.Response], Optional[dict]]:
    """
    Performs GET with retries and exponential backoff for 429/5xx.
    Returns (response, json_or_none)
    """
    attempt = 0
    backoff = 1.0
    headers = headers or {}
    while attempt < max_retries:
        attempt += 1
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=20)
            if resp.status_code == 200:
                try:
                    return resp, resp.json()
                except Exception:
                    return resp, None
            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning(f"MarketAux HTTP {resp.status_code}: {resp.text}")
                sleep_for = backoff * (RETRY_BACKOFF_BASE ** (attempt - 1))
                logger.info(f"Retrying after {sleep_for:.1f}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_for)
                continue
            # other status codes - return and let caller handle
            return resp, None
        except requests.RequestException as e:
            logger.warning(f"Request error: {e}. attempt {attempt}/{max_retries}")
            time.sleep(backoff * (RETRY_BACKOFF_BASE ** (attempt - 1)))
            continue
    logger.error("Exceeded max retries on request.")
    return None, None


def post_to_discord(content: str) -> bool:
    """
    POSTS to Discord webhook. Returns True on success.
    """
    if not DISCORD_WEBHOOK_URL:
        logger.warning("DISCORD_WEBHOOK_URL not set; skipping Discord post.")
        return False
    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": content}, timeout=10)
        if resp.status_code in (200, 204):
            logger.info("Discord posted.")
            return True
        else:
            logger.warning(f"Discord post failed: {resp.status_code} {resp.text}")
            return False
    except requests.RequestException as e:
        logger.warning(f"Discord post exception: {e}")
        return False


# --------------------------
# Severity / Filtering
# --------------------------


def score_text(text: str) -> float:
    """
    Score a piece of text using keyword_weights. Return aggregated weight.
    Simple approach: sum keyword matches (case-insensitive) * weight.
    Multi-occurrence increases the score.
    """
    if not text:
        return 0.0
    s = 0.0
    text_l = text.lower()
    # Count occurrences for each keyword - simple substring matching
    for kw, w in keyword_weights.items():
        start = 0
        count = 0
        while True:
            idx = text_l.find(kw, start)
            if idx == -1:
                break
            count += 1
            start = idx + len(kw)
        if count:
            s += count * w
    return s


def weighted_severity(article: Dict[str, Any]) -> Tuple[str, float]:
    """
    Given an article dict (with title, description, content), compute weighted score and return severity label.
    """
    title = article.get("title", "") or ""
    desc = article.get("description", "") or ""
    content = article.get("content", "") or ""
    text_blob = " ".join([title, desc, content])
    score = score_text(text_blob)
    if score >= HIGH_THRESHOLD:
        return "HIGH", score
    elif score >= MED_THRESHOLD:
        return "MED", score
    else:
        return "LOW", score


# --------------------------
# MarketAux fetch logic
# --------------------------


def marketaux_fetch_batch(symbols: List[str], published_after: str, page: int = 1) -> Tuple[List[dict], Optional[dict]]:
    """
    Fetch news for a batch of symbols via MarketAux /news/all
    Returns (articles_list, raw_response)
    """
    base_url = "https://api.marketaux.com/v1/news/all"
    params = {
        "symbols": ",".join(symbols),
        "published_after": published_after,
        "page": page,
        "language": "en",
    }
    headers = {"User-Agent": "mvp_alerts/1.0"}
    # API key in query param expected by MarketAux
    params["api_token"] = MARKETAUX_API_KEY
    resp, j = safe_request_get(base_url, params, headers=headers)
    if resp is None:
        logger.warning("marketaux_fetch_batch: No response (network error).")
        return [], None
    if resp.status_code != 200:
        # Some MarketAux errors return JSON body even on 400/422
        try:
            parsed = resp.json()
        except Exception:
            parsed = {"error": {"code": str(resp.status_code), "message": resp.text}}
        logger.warning(f"MarketAux HTTP {resp.status_code}: {parsed}")
        return [], parsed
    # expected structure: { "data": [...], "meta": {...} } or direct list - handle both
    try:
        if isinstance(j, dict) and "data" in j:
            articles = j["data"]
        elif isinstance(j, dict) and "news" in j:
            articles = j["news"]
        elif isinstance(j, list):
            articles = j
        else:
            # attempt common keys
            articles = j.get("articles") if isinstance(j, dict) else []
            if articles is None:
                articles = []
        return articles, j
    except Exception:
        logger.exception("Failed to parse MarketAux JSON response.")
        return [], j


# --------------------------
# Extended Ticker Detection (Option A - pass batch)
# --------------------------
def detect_tickers_extended(article: Dict[str, Any], batch: List[str], company_map: Dict[str, str]) -> List[str]:
    """
    Extended Matching (B):
    Steps (in order):
    1) MarketAux-provided tickers (fields: 'tickers', 'symbols') if present
    2) Company-name in title/description/content
    3) URL token match (tokens from URL compared to company name squashed)
    4) Ticker substring match in text blob
    5) Fallback -> first ticker of batch
    Returns unique uppercased tickers list.
    """
    matched = set()
    title = (article.get("title") or "").strip()
    desc = (article.get("description") or "") or ""
    content = (article.get("content") or "") or ""
    url = (article.get("url") or article.get("link") or "") or ""
    text_blob = " ".join([title, desc, content]).upper()

    # 1) MarketAux-provided tickers/symbols
    # MarketAux may include 'tickers' or 'symbols' keys with lists
    for key in ("tickers", "symbols"):
        if key in article and isinstance(article[key], (list, tuple)):
            for t in article[key]:
                try:
                    tt = str(t).strip().upper()
                    if tt:
                        matched.add(tt)
                except Exception:
                    continue
    if matched:
        return sorted(matched)

    # For steps 2-4, prepare company map lowercase for comparisons
    comp_lower_map = {k.upper(): (v or "").lower() for k, v in company_map.items()}

    # 2) Company name match in title/description/content
    title_l = title.lower()
    desc_l = desc.lower()
    content_l = content.lower()
    for ticker, cname_l in comp_lower_map.items():
        if not cname_l:
            continue
        if cname_l in title_l or cname_l in desc_l or cname_l in content_l:
            matched.add(ticker)

    # 3) URL token match
    if url:
        # split on common separators and remove empty tokens
        url_tokens = [tok.strip().lower() for tok in re.split(r"[-_/.\?=&:#]+", url) if tok.strip()]
        # Attempt exact token-to-name or squashed name match
        for tok in url_tokens:
            for ticker, cname_l in comp_lower_map.items():
                if not cname_l:
                    continue
                if tok == cname_l:
                    matched.add(ticker)
                # "paloalto" vs "palo alto"
                if tok == cname_l.replace(" ", ""):
                    matched.add(ticker)
                # token may equal company short name fragment (e.g., 'nasdaq' for 'Nasdaq, Inc.') - we avoid too many false positives by
                # requiring token length >=3
                if len(tok) >= 3 and tok in cname_l:
                    # require token to be reasonably informative (not 'the','and','com', etc.)
                    if tok not in ("the", "and", "com", "www", "http", "https", "amp"):
                        matched.add(ticker)

    # 4) Ticker substring match in title/desc/content - only for tickers present in the batch
    # This step helps catch cases like "AAPL reports earnings" etc.
    for t in batch:
        t_up = t.upper()
        if len(t_up) >= 1 and t_up in text_blob:
            matched.add(t_up)

    # 5) Fallback -> first ticker of batch (guarantee)
    if not matched:
        if batch and len(batch) > 0:
            fallback = batch[0].upper()
            logger.info(
                f"No tickers detected for article '{(title or '')[:80]}...' — assigning fallback {fallback}"
            )
            matched.add(fallback)
        else:
            logger.info(
                f"No tickers detected for article '{(title or '')[:80]}...' and batch empty; skipping."
            )

    return sorted(matched)


# --------------------------
# Smart Cool-Down tracking
# --------------------------
class CooldownManager:
    def __init__(self, cooldown_minutes: int = SMART_COOLDOWN_MINUTES):
        self.cooldown = timedelta(minutes=cooldown_minutes)
        self.last_posted: Dict[str, datetime] = {}  # ticker -> datetime

    def can_post(self, ticker: str) -> bool:
        t = self.last_posted.get(ticker)
        if t is None:
            return True
        if now_utc() - t >= self.cooldown:
            return True
        return False

    def mark_posted(self, ticker: str):
        self.last_posted[ticker] = now_utc()


# --------------------------
# Main flow
# --------------------------


def chunk_list(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def normalize_company_name(ticker: str, company_map: Dict[str, str]) -> str:
    name = company_map.get(ticker.upper())
    if not name:
        logger.info(f"Missing company name for ticker {ticker} — using ticker-only matching.")
        return ticker.upper()
    return name


def build_article_summary(ticker: str, company: str, severity: str, score: float, article: Dict[str, Any]) -> str:
    title = article.get("title") or ""
    url = article.get("url") or article.get("link") or ""
    # Format: TICKER | SEV | SCORE — COMPANY — TITLE (URL)
    return f"{ticker} | {severity} | {score:.2f} — {company} — {title} {url}"


def main_loop():
    if not MARKETAUX_API_KEY:
        logger.error("MARKETAUX_API_KEY not set. Export it to the environment and restart.")
        return
    # load resources
    watchlist = load_watchlist()
    if not watchlist:
        logger.error("Watchlist empty. Add tickers to watchlist.txt and restart.")
        return
    company_map = load_company_map()
    cooldown_mgr = CooldownManager()

    # Determine initial published_after timestamp (Option D: use last cycle timestamp if exists)
    last_ts = read_last_timestamp()
    if last_ts:
        try:
            last_dt = dateutil_parser.parse(last_ts)
            logger.info(f"Resuming from last_timestamp: {last_ts}")
        except Exception:
            last_dt = now_utc() - timedelta(hours=COLD_START_HOURS)
            logger.warning("Invalid last_timestamp; falling back to cold start window.")
    else:
        last_dt = now_utc() - timedelta(hours=COLD_START_HOURS)
        logger.info(f"No last_timestamp found. Cold start will use {COLD_START_HOURS} hours.")

    # Main infinite loop (user asked for infinite loop)
    cycle_count = 0
    while True:
        cycle_count += 1
        logger.info(f"========== Bot 2.2 Startup ==========" if cycle_count == 1 else f"=== Cycle #{cycle_count} startup ===")
        init_med_db()
        logger.info("SQLite MED DB ready.")
        # Refresh watchlist each cycle
        watchlist = load_watchlist()
        total_tickers = len(watchlist)
        logger.info(f"Cycle composition: {total_tickers} personal + 0 random = {total_tickers} total.")
        # compute published_after candidate(s) based on last_dt
        published_after_candidates = try_multiple_ts_formats(last_dt)
        # We'll try each candidate until one works
        batches = chunk_list(watchlist, BATCH_SIZE)
        fetched_total = 0
        kept_total = 0
        posted_total = 0
        # New cycle end timestamp - we will use this as last_timestamp for next cycle (Option D)
        cycle_end_ts = now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        logger.info(f"=== Cycle #{cycle_count} startup ===")
        logger.info(f"Starting news scan cycle. published_after candidates: {published_after_candidates[0]}")
        for batch_idx, batch in enumerate(batches, start=1):
            # Try different published_after string formats until success
            batch_articles: List[Dict[str, Any]] = []
            raw_response = None
            success = False
            for pa in published_after_candidates:
                # build URL for logging
                logger.info(f"Outbound URL: https://api.marketaux.com/v1/news/all?symbols={','.join(batch)}&published_after={pa}&page=1")
                articles, raw_response = marketaux_fetch_batch(batch, pa, page=1)
                # if raw_response came back with known malformed_parameters error, try next format
                if raw_response and isinstance(raw_response, dict) and raw_response.get("error"):
                    code = raw_response["error"].get("code")
                    msg = raw_response["error"].get("message", "")
                    if code in ("malformed_parameters", "malformed_parameter", "invalid_request") or ("published_after" in msg.lower()):
                        logger.warning(f"MarketAux HTTP 400-like: {raw_response} — trying next timestamp format.")
                        continue
                # success condition: list returned or empty list legitimately
                if articles is not None:
                    batch_articles = articles
                    success = True
                    break
            if not success:
                logger.warning("Batch fetch failed or returned no data; continuing to next batch.")
                continue

            fetched = len(batch_articles)
            fetched_total += fetched
            logger.info(f"Fetched {fetched} articles for batch ({','.join(batch)})")
            # Process each article: use Extended Matching to find tickers, compute severity, act accordingly
            kept_in_batch = 0
            posted_in_batch = 0
            for art in batch_articles:
                # Normalize a few fields
                title = art.get("title", "") or ""
                desc = art.get("description", "") or ""
                url = art.get("url", "") or art.get("link", "") or ""
                content = art.get("content", "") or ""

                # detect tickers using extended method (pass current batch)
                article_tickers = detect_tickers_extended(art, batch, company_map)

                # for each ticker connected to this article, compute severity and act
                for at in article_tickers:
                    company_name = normalize_company_name(at, company_map)
                    severity, score = weighted_severity(art)
                    kept = False
                    posted = False
                    # Balanced Mode: we post only HIGH and store MED as 'kept'
                    if severity == "HIGH":
                        # check cooldown
                        if cooldown_mgr.can_post(at):
                            summary = build_article_summary(at, company_name, "HIGH", score, art)
                            success_post = post_to_discord(summary)
                            if success_post:
                                posted = True
                                posted_in_batch += 1
                                cooldown_mgr.mark_posted(at)
                            # even if post fails, we still mark as kept for record
                            kept = True
                        else:
                            logger.info(f"Cooldown active for {at}; skipping Discord post but logging kept.")
                            kept = True
                    elif severity == "MED":
                        # MED persistence
                        published_at = art.get("published_at") or art.get("published_at_local") or ""
                        save_med_article(
                            at,  # ticker actually assigned
                            title,
                            desc,
                            url,
                            score,
                            published_at,
                        )
                        logger.info(f"MED saved to DB: {at} | {score:.2f} — {title}")
                        kept = True
                    else:
                        # LOW: ignore (but you may want to log)
                        logger.debug(f"LOW: {at} - {title[:120]}")
                    if kept:
                        kept_in_batch += 1
            kept_total += kept_in_batch
            posted_total += posted_in_batch
            logger.info(f"Batch {batch_idx}/{len(batches)} result: fetched={fetched} kept={kept_in_batch} posted={posted_in_batch}")
            # light rate-limit safety: small sleep (tunable). Keep small to preserve speed.
            time.sleep(0.2)

        # Cycle summary
        logger.info(f"Cycle summary: fetched={fetched_total} kept={kept_total} posted={posted_total}")
        # Save last timestamp as cycle_end_ts (Option D)
        write_last_timestamp(cycle_end_ts)
        logger.info(f"Cycle {cycle_count} completed in ... (stored last_timestamp={cycle_end_ts})")
        logger.info(f"Sleeping {CYCLE_SECONDS}s until next cycle.")
        time.sleep(CYCLE_SECONDS)


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down.")
    except Exception:
        logger.exception("Unhandled exception — shutting down.")
