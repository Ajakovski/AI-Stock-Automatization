#!/usr/bin/env python3
"""
mvp_alerts.py - corrected version

Improvements:
1) Robust S&P500 parsing
2) Keep all watchlist tickers; fill up to 50 with S&P randoms (no repeats last 3 cycles)
3) Handle missing title/description
4) NewsData filtered to US business news
5) Dedupe by title+url for 12 hours, DB migration logic included
6) Restored logging details and cycle summary
7) Discord rate-limit handling with retry_after backoff
"""

import os
import re
import time
import json
import random
import logging
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ----------------- CONFIG -----------------
load_dotenv()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-")
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY", "pub_f22ba9249c104a038d7e1b904b949e3a")

WATCHLIST_FILE = "watchlist.txt"
DB_PATH = "events.db"
LOG_FILE = "bot.log"
RECENT_RANDOMS_FILE = "recent_randoms.json"

PERSONAL_TARGET = 50
POLL_INTERVAL = 100 * 60  # 100 minutes
ROTATION_KEEP = 3  # avoid random repeats from last 3 cycles
DEDUPE_HOURS = 12  # dedupe window for title+url
USER_AGENT = {"User-Agent": "StockAI-Bot/1.5.1 (andrej@local)"}

# keyword_weights (expanded)
keyword_weights = {
    "bankruptcy": 5, "files for bankruptcy": 5,
    "lawsuit": 4, "sued": 4, "sec investigation": 4, "investigation": 4,
    "charged": 4, "indicted": 4, "fraud": 4,
    "recall": 4, "data breach": 4, "hack": 4, "cyberattack": 4,
    "layoff": 3, "job cuts": 3, "fired": 3, "resignation": 3, "steps down": 3,
    "delisting": 4, "trading halted": 4, "collapse": 4, "plunge": 3, "crash": 4,
    "merger": 3, "acquisition": 3, "acquires": 3, "buyout": 3, "takeover": 3,
    "ipo": 3, "spinoff": 3, "share offering": 3, "secondary offering": 3,
    "guidance cut": 3, "guidance lowered": 3, "downgrade": 3, "upgrade": 2,
    "earnings": 2, "quarterly results": 2, "eps": 2, "revenue": 2,
    "misses expectations": 3, "beats expectations": 3, "forecast": 2,
    "insider": 2, "buys shares": 2, "sells shares": 2,
    "chief executive": 2, "ceo": 2, "cfo": 2,
    "dividend": 2, "price target": 2, "partnership": 2,
    "restructuring": 2, "new product": 1, "record high": 2, "record low": 2,
    "all-time": 2, "massive": 2, "surge": 2, "drops": 2, "jumps": 2,
}

# ----------------- LOGGING -----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.getLogger("").addHandler(console)

logging.info("=== Bot startup ===")

# ----------------- DB + migration -----------------
def init_db_and_migrate():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    # create table if missing
    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        ticker TEXT,
        title TEXT,
        severity TEXT,
        url TEXT,
        created_at TEXT
    )
    """)
    conn.commit()

    # ensure columns exist (migration safe)
    cur.execute("PRAGMA table_info(events)")
    existing = {row[1] for row in cur.fetchall()}
    required = {"id", "source", "ticker", "title", "severity", "url", "created_at"}
    missing = required - existing
    for col in missing:
        try:
            # create text columns for missing ones
            if col != "id":
                cur.execute(f"ALTER TABLE events ADD COLUMN {col} TEXT")
                logging.info(f"DB migration: added missing column '{col}'")
        except Exception as e:
            logging.warning(f"DB migration: could not add {col}: {e}")
    conn.commit()
    return conn

def seen_recent(title, url, hours=DEDUPE_HOURS):
    if not url and not title:
        return False
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        if url:
            cur.execute("SELECT 1 FROM events WHERE url=? AND created_at>?", (url, cutoff))
            if cur.fetchone():
                return True
        # also check title-based dedupe if url missing or different
        if title:
            cur.execute("SELECT 1 FROM events WHERE title=? AND created_at>?", (title, cutoff))
            if cur.fetchone():
                return True
        return False
    except Exception as e:
        logging.error(f"seen_recent error: {e}")
        return False
    finally:
        conn.close()

def store_event_record(source, ticker, title, severity, url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO events (source,ticker,title,severity,url,created_at) VALUES (?,?,?,?,?,?)",
            (source, ticker, title, severity, url, datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    except Exception as e:
        logging.error(f"store_event_record error: {e}")
    finally:
        conn.close()

# ----------------- S&P500 parsing (robust) -----------------
def fetch_sp500_tickers():
    """
    Robustly fetch S&P500 tickers from Wikipedia.
    Tries multiple ways and falls back to searching for a plausible symbol column.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        r.raise_for_status()
        # try usual parsing first
        dfs = pd.read_html(r.text)
        # try to find a df that looks like the constituents table
        for df in dfs:
            # look for a column name that contains symbol-like strings
            for col in df.columns:
                sample = df[col].dropna().astype(str).head(10).tolist()
                # heuristic: contains uppercase letters and digits, reasonable length
                if all(re.match(r'^[A-Z0-9\.-]{1,6}$', s.replace('.', '-').upper()) for s in sample):
                    tickers = df[col].astype(str).str.replace('.', '-', regex=False).str.upper().tolist()
                    logging.info(f"Fetched S&P500 tickers from dataframe column '{col}' ({len(tickers)} tickers).")
                    return tickers
        # fallback: try parsing table with id 'constituents' via BeautifulSoup
        soup = BeautifulSoup(r.text, "lxml")
        tbl = soup.find("table", {"id": "constituents"})
        if tbl:
            df2 = pd.read_html(str(tbl))[0]
            # common column names: 'Symbol' or 'Ticker symbol'
            for name in ("Symbol", "Ticker symbol", "Ticker"):
                if name in df2.columns:
                    tickers = df2[name].astype(str).str.replace('.', '-', regex=False).str.upper().tolist()
                    logging.info(f"Fetched S&P500 tickers using column '{name}' ({len(tickers)} tickers).")
                    return tickers
        logging.error("S&P500 fetch: could not detect symbol column.")
        return []
    except Exception as e:
        logging.error(f"Failed to load S&P500 list: {e}")
        return []

# ----------------- Watchlist + rotation -----------------
def load_watchlist():
    if not os.path.exists(WATCHLIST_FILE):
        logging.warning("watchlist.txt not found; using empty watchlist.")
        return []
    with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
        lines = [l.strip().upper() for l in f.readlines() if l.strip()]
    # preserve order and dedupe
    seen = set()
    out = []
    for t in lines:
        if t not in seen:
            out.append(t)
            seen.add(t)
    logging.info(f"Loaded {len(out)} tickers from watchlist.txt.")
    return out

def load_recent_randoms():
    if not os.path.exists(RECENT_RANDOMS_FILE):
        return []
    try:
        with open(RECENT_RANDOMS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
        return []
    except Exception as e:
        logging.error(f"load_recent_randoms error: {e}")
        return []

def save_recent_randoms(recent):
    try:
        with open(RECENT_RANDOMS_FILE, "w", encoding="utf-8") as f:
            json.dump(recent[:ROTATION_KEEP], f, indent=2)
    except Exception as e:
        logging.error(f"save_recent_randoms error: {e}")

def build_cycle_tickers(personal_list, sp500_list):
    # personal_list: always keep all user tickers (even if not in sp500)
    personal = [t.upper() for t in personal_list]
    # pool = sp500 excluding personal tickers
    sp_pool = [t for t in sp500_list if t not in set(personal)]
    random.shuffle(sp_pool)

    recent = load_recent_randoms()
    recent_flat = set(sum([r if isinstance(r, list) else [] for r in recent], []))

    need = max(0, PERSONAL_TARGET - len(personal))
    # choose from sp_pool but avoid recently used
    candidates = [t for t in sp_pool if t not in recent_flat]
    if len(candidates) < need:
        # allow repeats if not enough
        candidates = sp_pool

    picks = candidates[:need]
    combined = personal + picks
    # persist random picks
    recent.insert(0, picks)
    save_recent_randoms(recent)
    logging.info(f"Cycle tickers: {len(combined)} total ({len(personal)} personal + {len(picks)} random).")
    logging.info(f"Random picks this cycle: {picks}")
    return combined, set(personal)

# ----------------- NewsData single-ticker fetch (US business) -----------------
def fetch_news_for_ticker(ticker):
    if not NEWSDATA_KEY:
        logging.warning("No NewsData API key configured.")
        return []
    params = {
        "apikey": NEWSDATA_KEY,
        "q": ticker,
        "language": "en",
        "category": "business",  # restrict to business
        "country": "us"          # restrict to US news
    }
    url = "https://newsdata.io/api/1/news"
    try:
        r = requests.get(url, params=params, headers=USER_AGENT, timeout=20)
        if r.status_code == 422:
            logging.warning(f"NewsData returned 422 for ticker {ticker}. Skipping.")
            return []
        r.raise_for_status()
        payload = r.json()
        results = payload.get("results", []) or []
        logging.info(f"Fetched {len(results)} articles for {ticker}")
        normalized = []
        for it in results:
            normalized.append({
                "ticker": ticker,
                "title": (it.get("title") or "").strip(),
                "description": (it.get("description") or "").strip(),
                "url": (it.get("link") or it.get("url") or "").strip(),
                "source": it.get("source_id"),
                "published": it.get("pubDate")
            })
        return normalized
    except Exception as e:
        logging.error(f"fetch_news_for_ticker error ({ticker}): {e}")
        return []

# ----------------- Weighted severity (robust) -----------------
def score_article(article, ticker):
    title = article.get("title") or ""
    desc = article.get("description") or ""
    text = f"{title} {desc}".lower()
    score = 0
    for k, w in keyword_weights.items():
        if k in text:
            score += w
    # boosts
    if ticker.lower() in text:
        score += 1
    # detect percent change patterns
    if re.search(r"\b-?\d+(\.\d+)?%\b", text):
        score += 2
    if any(x in text for x in ["ceo", "cfo", "chief", "executive"]):
        score += 1
    if any(x in text for x in ["mass", "record", "all-time"]):
        score += 1

    if score >= 4:
        return "HIGH"
    elif score >= 2:
        return "MED"
    else:
        return "LOW"

# ----------------- Discord posting with rate-limit handling -----------------
def post_to_discord_safe(title, description, link, ticker, source, severity):
    if not DISCORD_WEBHOOK:
        logging.warning("No Discord webhook configured.")
        return False
    # truncate description safely
    desc = description or ""
    if len(desc) > 900:
        desc = desc[:897] + "..."
    embed = {
        "title": title,
        "description": desc,
        "url": link,
        "fields": [
            {"name": "Ticker", "value": ticker or "N/A", "inline": True},
            {"name": "Source", "value": source or "NewsData.io", "inline": True},
            {"name": "Severity", "value": severity, "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    payload = {"content": None, "embeds": [embed]}
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, headers=headers, timeout=15)
        if r.status_code in (200, 204):
            logging.info(f"Discord posted: {ticker} | {severity} | {title}")
            return True
        if r.status_code == 429:
            # Respect retry_after if provided
            try:
                j = r.json()
                retry_after = float(j.get("retry_after", 1.0))
            except Exception:
                retry_after = 1.0
            logging.warning(f"Discord rate-limited. Backing off for {retry_after:.2f}s")
            time.sleep(retry_after + 0.2)
            return False
        else:
            logging.error(f"Discord error {r.status_code}: {r.text}")
            # small backoff to be safe
            time.sleep(1.0)
            return False
    except requests.RequestException as e:
        logging.error(f"Discord request failed: {e}")
        time.sleep(1.0)
        return False

# ----------------- Main loop -----------------
def main():
    init_db_and_migrate()
    sp500 = fetch_sp500_tickers()
    if not sp500:
        logging.warning("S&P500 list empty - random picks will be disabled until fetch succeeds.")
    # load once; daily refresh would be possible later
    while True:
        try:
            watchlist = load_watchlist()
            sp500 = fetch_sp500_tickers() or sp500  # refresh but fallback to previous
            tickers, watchlist_set = build_cycle_tickers(watchlist, sp500)
            logging.info("Starting news scan cycle...")
            counts = {"HIGH": 0, "MED": 0, "LOW": 0, "posted": 0}
            for ticker in tickers:
                articles = fetch_news_for_ticker(ticker)
                # small pause to avoid hammering NewsData
                time.sleep(0.7)
                for art in articles:
                    # safe fields
                    title = art.get("title") or ""
                    desc = art.get("description") or ""
                    url = art.get("url") or ""
                    # skip empty content
                    if not title and not desc:
                        continue
                    # dedupe by recent title+url
                    if seen_recent(title, url):
                        continue
                    severity = score_article(art, ticker)
                    counts[severity] = counts.get(severity, 0) + 1
                    # Balanced mode filtering
                    if ticker in watchlist_set:
                        if severity not in ("MED", "HIGH"):
                            # log but don't post
                            continue
                    else:
                        if severity != "HIGH":
                            continue

                    # store & post
                    store_event_record("news", ticker, title, severity, url)
                    sent = post_to_discord_safe(f"{severity} — {ticker} — {title}", f"{desc}", url, ticker, "NewsData.io", severity)
                    if sent:
                        counts["posted"] += 1
                    # polite delay between posts to reduce rate-limit risk
                    time.sleep(random.uniform(1.2, 2.2))
            # cycle summary
            logging.info(f"Cycle summary: HIGH={counts['HIGH']} MED={counts['MED']} LOW={counts['LOW']} posted={counts['posted']}")
            print(f"Cycle summary: HIGH={counts['HIGH']} MED={counts['MED']} LOW={counts['LOW']} posted={counts['posted']}")
        except Exception as e:
            logging.exception(f"Main loop exception: {e}")
        logging.info(f"Sleeping for {POLL_INTERVAL}s until next cycle.")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
