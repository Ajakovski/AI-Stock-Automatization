#!/usr/bin/env python3
"""
mvp_alerts.py

- Loads personal tickers from watchlist.txt (one ticker per line)
- Fetches S&P500 master list from Wikipedia
- Fills up to 50 tickers per cycle by adding random non-repeating tickers
  (avoids repeats from the last 3 cycles)
- Queries NewsData.io **per ticker** (one request per ticker) every 100 minutes
- Scores articles and posts MED/HIGH alerts to Discord
- Deduplicates by article link stored in SQLite
- Logs full cycle composition and actions to bot.log
- Form-4 fetching is commented out (disabled)
"""

import os
import time
import json
import logging
import random
import sqlite3
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd

# ------------------- CONFIG -------------------
load_dotenv()

# You can override via .env or edit here (not recommended to hardcode tokens)
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL") or "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY") or "pub_f22ba9249c104a038d7e1b904b949e3a"

# Files
DB_PATH = "events.db"
LOG_FILE = "bot.log"
WATCHLIST_FILE = "watchlist.txt"
RECENT_RANDOMS_FILE = "recent_randoms.json"  # stores last 3 random groups

# Timing / sizes
PERSONAL_TARGET = 50          # final size of tickers to query each cycle
POLL_INTERVAL = 6000         # seconds (100 minutes)
DEDUPE_WINDOW_MINUTES = 60 * 24 * 7  # keep duplicates out for a week (links are unique, but keep window)
# (we dedupe by link so long dedupe is fine)

USER_AGENT = {"User-Agent": "StockAI-Bot/1.4 (your-email@example.com)"}

# News scoring keywords
HIGH_SIGNALS = [
    "merger", "acquisition", "acquires", "acquired", "buyout", "takeover",
    "lawsuit", "charged", "investigation", "sec investigation",
    "bankruptcy", "files for bankruptcy", "data breach", "fraud",
    "resignation", "steps down", "fined", "settlement", "layoffs",
    "mass layoff", "hack", "cyberattack", "guidance cut",
    "guidance lowered", "downgrade", "recall", "trading halted",
    "delisting", "collapse", "fire", "explosion", "plunge", "crash"
]
MED_SIGNALS = [
    "earnings", "quarterly results", "eps", "revenue", "forecast",
    "price target", "beats", "misses", "upgrade", "expands",
    "partnership", "restructuring", "ipo", "share offering",
    "dividend", "new product", "guidance", "price target raised",
    "price target lowered", "beats estimates", "misses estimates"
]
FIN_KEYWORDS = set(HIGH_SIGNALS + MED_SIGNALS + [
    "buyback", "sec", "investigation", "ipo", "split", "insider", "insider trading"
])

# ------------------- LOGGING -------------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
logging.getLogger("").addHandler(console)

logging.info("=== Bot startup ===")

# ------------------- DB (with simple migration) -------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        ticker TEXT,
        event_type TEXT,
        severity TEXT,
        title TEXT,
        link TEXT UNIQUE,
        raw TEXT,
        created_at TEXT
    )
    """)
    conn.commit()

    # migration guard: ensure 'link' exists (should from above)
    cur.execute("PRAGMA table_info(events)")
    cols = [row[1] for row in cur.fetchall()]
    required = {"id", "source", "ticker", "event_type", "severity", "title", "link", "raw", "created_at"}
    missing = required - set(cols)
    if missing:
        # try to add missing columns (best-effort)
        for col in missing:
            try:
                cur.execute(f"ALTER TABLE events ADD COLUMN {col} TEXT")
                logging.warning(f"DB migration: added missing column {col}")
            except Exception as e:
                logging.error(f"DB migration: failed to add {col}: {e}")
        conn.commit()
    conn.close()

def already_seen_link(link):
    if not link:
        return False
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM events WHERE link=? LIMIT 1", (link,))
        res = cur.fetchone()
        return bool(res)
    except Exception as e:
        logging.error(f"already_seen_link error: {e}")
        return False
    finally:
        conn.close()

def save_event(data):
    """
    data: dict with keys source,ticker,event_type,severity,title,link,raw
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR IGNORE INTO events (source,ticker,event_type,severity,title,link,raw,created_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            data.get("source"),
            data.get("ticker"),
            data.get("event_type"),
            data.get("severity"),
            data.get("title"),
            data.get("link"),
            data.get("raw"),
            datetime.utcnow().isoformat()
        ))
        conn.commit()
    except Exception as e:
        logging.error(f"save_event error: {e}")
    finally:
        conn.close()

# ------------------- Watchlist + S&P500 helpers -------------------
def load_personal_watchlist():
    if not os.path.exists(WATCHLIST_FILE):
        logging.warning(f"{WATCHLIST_FILE} not found. Personal list empty.")
        return []
    with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
        lines = [line.strip().upper() for line in f if line.strip()]
    # dedupe and keep order (preserve file order)
    seen = set()
    out = []
    for t in lines:
        if t not in seen:
            out.append(t)
            seen.add(t)
    logging.info(f"Loaded {len(out)} personal tickers from {WATCHLIST_FILE}.")
    return out

def fetch_sp500_tickers():
    """
    Fetch S&P500 list from Wikipedia (returns a list).
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", {"id": "constituents"})
        df = pd.read_html(str(table))[0]
        tickers = [s.replace('.', '-').upper() for s in df.Symbol.tolist()]
        logging.info(f"Fetched S&P500 master list: {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to fetch S&P500 list: {e}")
        return []

# ------------------- Random rotation persistence -------------------
def load_recent_randoms():
    """
    Returns list of recent random sets (list of lists). Keep up to 3.
    """
    if not os.path.exists(RECENT_RANDOMS_FILE):
        return []
    try:
        with open(RECENT_RANDOMS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                return []
    except Exception as e:
        logging.error(f"Failed loading recent_randoms: {e}")
        return []

def save_recent_randoms(recent):
    """
    recent: list of recent random lists (most recent first). We'll trim to 3.
    """
    try:
        with open(RECENT_RANDOMS_FILE, "w", encoding="utf-8") as f:
            json.dump(recent[:3], f)
    except Exception as e:
        logging.error(f"Failed saving recent_randoms: {e}")

# ------------------- NewsData single-ticker fetch -------------------
def fetch_news_for_ticker(ticker):
    """
    Query NewsData for a single ticker. Returns list of article dicts from API.
    """
    if not NEWSDATA_KEY:
        logging.warning("No NewsData API key found.")
        return []

    params = {
        "apikey": NEWSDATA_KEY,
        "q": ticker,
        "language": "en",
        "category": "business",
        "country": "us"
    }
    url = "https://newsdata.io/api/1/news"
    try:
        r = requests.get(url, params=params, headers=USER_AGENT, timeout=20)
        if r.status_code == 422:
            logging.warning(f"NewsData returned 422 for ticker {ticker}. Skipping.")
            return []
        r.raise_for_status()
        data = r.json()
        results = data.get("results", []) or []
        # Normalize fields
        out = []
        for item in results:
            out.append({
                "title": item.get("title"),
                "description": item.get("description") or "",
                "link": item.get("link") or item.get("url"),
                "source": item.get("source_id"),
                "published": item.get("pubDate")
            })
        return out
    except Exception as e:
        logging.error(f"fetch_news_for_ticker error ({ticker}): {e}")
        return []

# ------------------- Scoring & classification -------------------
def classify_article_by_ticker(article, ticker):
    """
    Return severity (LOW/MED/HIGH) and reason string
    Only escalate if article text mentions ticker or financial keywords.
    """
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()
    text = f"{title} {desc}"
    # check if ticker appears (common forms: 'NVDA', 'NVIDIA', 'NYSE:NVDA', '(NVDA)')
    ticker_lower = ticker.lower()
    mention_ticker = ticker_lower in text or f"({ticker_lower})" in text or f"nyse:{ticker_lower}" in text or f"nasdaq:{ticker_lower}" in text

    # check finance keywords
    fin_hits = [k for k in FIN_KEYWORDS if k in text]

    if not mention_ticker and not fin_hits:
        return "LOW", "no-match"

    # compute simple weights
    high_hits = sum(1 for w in HIGH_SIGNALS if w in text)
    med_hits = sum(1 for w in MED_SIGNALS if w in text)

    if high_hits >= 1:
        severity = "HIGH"
    elif med_hits >= 1:
        severity = "MED"
    else:
        # if ticker is mentioned and there are finance keywords, mark MED; else LOW
        severity = "MED" if mention_ticker or fin_hits else "LOW"

    # escalate negative MED to HIGH
    negative_cues = ["lawsuit", "fined", "downgrade", "layoff", "bankruptcy", "investigation", "charged", "recall"]
    if severity == "MED" and any(c in text for c in negative_cues):
        severity = "HIGH"

    reason = f"ticker_mentioned={mention_ticker}; hits={len(fin_hits)}; high={high_hits}; med={med_hits}"
    return severity, reason

# ------------------- Discord post (safe) -------------------
def post_to_discord(article, ticker, severity, reason):
    if not DISCORD_WEBHOOK:
        logging.warning("Discord webhook not configured.")
        return

    title = article.get("title") or "No title"
    link = article.get("link") or ""
    desc = article.get("description") or ""
    # safe truncate
    if len(desc) > 900:
        desc = desc[:897] + "..."

    embed = {
        "title": f"{severity} — {ticker} — {title}",
        "description": desc,
        "url": link,
        "fields": [
            {"name": "Ticker", "value": ticker, "inline": True},
            {"name": "Severity", "value": severity, "inline": True},
            {"name": "Reason", "value": reason, "inline": False},
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

    payload = {"content": None, "embeds": [embed]}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
        if r.status_code not in (200, 204):
            logging.error(f"Discord error {r.status_code}: {r.text}")
        else:
            logging.info(f"Discord posted: {ticker} | {severity} | {title}")
    except Exception as e:
        logging.error(f"Discord post failed: {e}")

# ------------------- Utilities -------------------
def build_cycle_tickers(personal_list, sp500_list):
    """
    personal_list: list of tickers from watchlist.txt (may be <50)
    sp500_list: full S&P500 list
    Returns list of 50 tickers (personal + random picks), and the random picks list.
    """
    personal = [t.upper() for t in personal_list]
    # keep order for personal
    personal_seen = set(personal)
    # available pool = sp500 minus personal
    pool = [t for t in sp500_list if t not in personal_seen]
    random.shuffle(pool)

    need = max(0, PERSONAl_TARGET := max(PERSONAL_TARGET - len(personal), 0))
    # we'll fill need from pool with no repeats from recent sets (handled above)
    # but this function just returns personal + first N from pool (caller will filter repeats)
    picks = []
    idx = 0
    while len(picks) < need and idx < len(pool):
        picks.append(pool[idx])
        idx += 1

    combined = personal + picks
    # safety: if not enough pool items, just return what we have
    return combined, picks

# ------------------- Main loop -------------------
def main():
    init_db()
    personal = load_personal_watchlist()
    sp500 = fetch_sp500_tickers()
    if not sp500:
        logging.error("S&P500 fetch failed — exiting.")
        return

    # Load recent randoms (most recent first)
    recent_randoms = load_recent_randoms()  # list of lists
    logging.info(f"Loaded recent_randoms (len={len(recent_randoms)})")

    # We'll run indefinitely
    while True:
        try:
            # Build the base set
            personal = load_personal_watchlist()  # reload every cycle in case user edits the file
            personal = [t for t in personal if t in sp500]  # keep only valid S&P tickers
            logging.info(f"Personal list size (valid S&P): {len(personal)}")

            # pool from S&P500 excluding personal
            available = [t for t in sp500 if t not in personal]
            # remove recent randoms (flatten last 3 cycles)
            recent_flat = set()
            for r in recent_randoms[:3]:
                recent_flat.update(r if isinstance(r, list) else [])
            # candidate pool excludes recent_flat
            candidate_pool = [t for t in available if t not in recent_flat]
            if len(candidate_pool) < (PERSONAL_TARGET - len(personal)):
                # if not enough, fallback to available (allow repeats)
                candidate_pool = [t for t in available]

            # choose random picks to fill to PERSONAL_TARGET
            need = max(0, PERSONAL_TARGET - len(personal))
            random_picks = random.sample(candidate_pool, k=min(need, len(candidate_pool)))
            combined = personal + random_picks
            # ensure unique and uppercase
            combined = list(dict.fromkeys([t.upper() for t in combined]))[:PERSONAL_TARGET]

            # update recent_randoms
            recent_randoms.insert(0, random_picks)
            recent_randoms = recent_randoms[:3]
            save_recent_randoms(recent_randoms)

            logging.info(f"Cycle tickers: {len(combined)} total ({len(personal)} personal + {len(random_picks)} random).")
            logging.info(f"Random picks this cycle: {random_picks}")

            # For each ticker, fetch news individually
            for ticker in combined:
                # polite delay to avoid hammering
                news = fetch_news_for_ticker(ticker)
                # log fetch result count
                logging.info(f"Fetched {len(news)} articles for {ticker}")

                for art in news:
                    link = art.get("link") or ""
                    if not link:
                        continue
                    # dedupe by link
                    if already_seen_link(link):
                        # skip duplicates
                        continue
                    severity, reason = classify_article_by_ticker(art, ticker)
                    if severity == "LOW":
                        # skip low severity
                        continue
                    # save & post
                    ev = {
                        "source": "news",
                        "ticker": ticker,
                        "event_type": "news",
                        "severity": severity,
                        "title": art.get("title"),
                        "link": link,
                        "raw": art.get("description") or ""
                    }
                    save_event(ev)
                    post_to_discord(art, ticker, severity, reason)

                # small delay between ticker calls (respect API limits)
                time.sleep(1.0)

            logging.info(f"Cycle complete. Sleeping {POLL_INTERVAL}s.")
        except Exception as e:
            logging.exception(f"Main loop error: {e}")

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
