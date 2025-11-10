#!/usr/bin/env python3
"""
MVP: Weighted Stock News Alerts (Balanced Mode)
- Reads watchlist.txt + fills random S&P500 tickers (no repeat last 3 cycles)
- Fetches individual ticker news via NewsData.io
- Uses weighted keyword model for severity
- Posts MED/HIGH for watchlist, HIGH only for random
- Full logging + SQLite deduplication
"""

import os
import time
import random
import json
import logging
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pytz

# ------------------- CONFIG -------------------
load_dotenv()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL") or "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY") or "pub_f22ba9249c104a038d7e1b904b949e3a"

DB_PATH = "events.db"
LOG_FILE = "bot.log"
WATCHLIST_FILE = "watchlist.txt"
US_TZ = pytz.timezone("America/New_York")

# Poll every 100 minutes (100 * 60)
POLL_INTERVAL = 100 * 60

# Rotation memory
ROTATION_HISTORY_FILE = "rotation_history.json"
ROTATION_HISTORY_LIMIT = 3  # remember last 3 cycles

USER_AGENT = {"User-Agent": "StockAI-Bot/1.5 (andrej@stockai.local)"}

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

# ------------------- DATABASE -------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        source TEXT,
        ticker TEXT,
        title TEXT,
        severity TEXT,
        url TEXT,
        created_at TEXT
    )
    """)
    conn.commit()
    return conn


def is_duplicate(conn, url):
    if not url:
        return False
    cur = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(hours=6)).isoformat()
    cur.execute("SELECT 1 FROM events WHERE url=? AND created_at>?", (url, time_threshold))
    return cur.fetchone() is not None


def save_event(conn, article):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (source,ticker,title,severity,url,created_at) VALUES (?,?,?,?,?,?)",
        (
            "news",
            article.get("ticker"),
            article.get("title"),
            article.get("severity"),
            article.get("url"),
            datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()


# ------------------- WATCHLIST + ROTATION -------------------
def load_watchlist():
    if not os.path.exists(WATCHLIST_FILE):
        logging.warning("watchlist.txt not found, using empty list.")
        return []
    with open(WATCHLIST_FILE, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]
    logging.info(f"Loaded {len(tickers)} tickers from watchlist.txt.")
    return tickers


def fetch_sp500():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        df = pd.read_html(r.text)[0]
        tickers = df["Symbol"].str.replace(".", "-", regex=False).str.upper().tolist()
        logging.info(f"Fetched {len(tickers)} S&P500 tickers.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load S&P500 list: {e}")
        return []


def load_rotation_history():
    if os.path.exists(ROTATION_HISTORY_FILE):
        with open(ROTATION_HISTORY_FILE, "r") as f:
            return json.load(f)
    return []


def save_rotation_history(history):
    with open(ROTATION_HISTORY_FILE, "w") as f:
        json.dump(history[-ROTATION_HISTORY_LIMIT:], f, indent=2)


def build_ticker_pool():
    watchlist = load_watchlist()
    sp500 = fetch_sp500()

    random_pool = [t for t in sp500 if t not in watchlist]
    random.shuffle(random_pool)

    history = load_rotation_history()
    recently_used = set(sum(history, []))

    available = [t for t in random_pool if t not in recently_used]
    needed = max(0, 50 - len(watchlist))
    selected_random = available[:needed]
    combined = list(dict.fromkeys(watchlist + selected_random))

    history.append(selected_random)
    save_rotation_history(history)

    logging.info(f"Cycle composition: {len(watchlist)} watchlist + {len(selected_random)} random.")
    logging.info(f"Random tickers: {selected_random}")
    return combined, set(watchlist)


# ------------------- NEWSDATA FETCH -------------------
def fetch_news_for_ticker(ticker):
    try:
        params = {
            "apikey": NEWSDATA_KEY,
            "q": ticker,
            "language": "en",
        }
        r = requests.get("https://newsdata.io/api/1/news", params=params, headers=USER_AGENT, timeout=20)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        return [
            {
                "ticker": ticker,
                "title": n.get("title"),
                "description": n.get("description", ""),
                "url": n.get("link"),
                "source": n.get("source_id"),
                "published": n.get("pubDate"),
            }
            for n in results
        ]
    except Exception as e:
        logging.error(f"Error fetching news for {ticker}: {e}")
        return []


# ------------------- SEVERITY MODEL -------------------
keyword_weights = {
    # 🚨 Major negative events
    "bankruptcy": 5, "files for bankruptcy": 5,
    "lawsuit": 4, "sued": 4, "sec investigation": 4,
    "charged": 4, "indicted": 4, "fraud": 4,
    "recall": 4, "data breach": 4, "hack": 4, "cyberattack": 4,
    "layoff": 3, "job cuts": 3, "fired": 3, "resignation": 3,
    "delisting": 4, "trading halted": 4, "collapse": 4,

    # 📈 Big financial events
    "merger": 3, "acquisition": 3, "acquires": 3, "buyout": 3, "takeover": 3,
    "ipo": 3, "spinoff": 3, "share offering": 3, "secondary offering": 3,
    "guidance cut": 3, "guidance lowered": 3, "downgrade": 3, "upgrade": 2,

    # 💵 Earnings / financial results
    "earnings": 2, "quarterly results": 2, "eps": 2, "revenue": 2,
    "misses expectations": 3, "beats expectations": 3, "forecast": 2,

    # 🧑‍💼 Insider & leadership
    "insider": 2, "buys shares": 2, "sells shares": 2,
    "chief executive": 2, "ceo": 2, "cfo": 2, "steps down": 3,

    # 🔍 Other impactful events
    "dividend": 2, "price target": 2, "partnership": 2,
    "restructuring": 2, "expands": 1, "new product": 1,
    "record high": 2, "record low": 2, "all-time": 2,
    "massive": 2, "surge": 2, "plunge": 3, "drops": 2, "jumps": 2,
}


def score_news(article, ticker):
    text = (article.get("title", "") + " " + article.get("description", "")).lower()
    score = 0
    for word, weight in keyword_weights.items():
        if word in text:
            score += weight

    # Contextual boosts
    if ticker.lower() in text:
        score += 1
    if "%" in text:
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


# ------------------- DISCORD -------------------
def post_to_discord(payload):
    if not DISCORD_WEBHOOK:
        logging.warning("No Discord webhook configured.")
        return

    desc = payload.get("description", "") or ""
    if len(desc) > 950:
        desc = desc[:950] + "..."

    embed = {
        "title": payload.get("title"),
        "description": desc,
        "url": payload.get("link"),
        "fields": [
            {"name": "Ticker", "value": payload.get("ticker"), "inline": True},
            {"name": "Source", "value": payload.get("source"), "inline": True},
            {"name": "Severity", "value": payload.get("severity"), "inline": True},
        ],
        "timestamp": datetime.utcnow().isoformat(),
    }

    data = {"content": None, "embeds": [embed]}
    headers = {"Content-Type": "application/json"}

    try:
        r = requests.post(DISCORD_WEBHOOK, json=data, headers=headers, timeout=10)
        if r.status_code == 429:
            logging.warning("Rate limit hit (429). Pausing 5 sec.")
            time.sleep(5)
        elif r.status_code not in (200, 204):
            logging.error(f"Discord error {r.status_code}: {r.text}")
    except Exception as e:
        logging.error(f"Discord post failed: {e}")


# ------------------- MAIN LOOP -------------------
def main_loop():
    conn = init_db()
    while True:
        try:
            tickers, watchlist_set = build_ticker_pool()
            logging.info("Starting full news scan cycle.")

            for ticker in tickers:
                articles = fetch_news_for_ticker(ticker)
                for art in articles:
                    if not art.get("url") or is_duplicate(conn, art["url"]):
                        continue
                    severity = score_news(art, ticker)
                    art["severity"] = severity

                    # Balanced alert filter
                    if ticker in watchlist_set:
                        if severity not in ("MED", "HIGH"):
                            continue
                    else:
                        if severity != "HIGH":
                            continue

                    save_event(conn, art)
                    desc = f"**Headline:** {art.get('title')}\n\n{art.get('description')}"
                    post_to_discord({
                        "title": f"News Alert — {severity}",
                        "description": desc,
                        "link": art.get("url"),
                        "ticker": ticker,
                        "source": "NewsData.io",
                        "severity": severity
                    })
                    time.sleep(1.5)

            logging.info("Cycle completed. Sleeping for next 100 minutes.")
        except Exception as e:
            logging.error(f"Main loop error: {e}")

        time.sleep(POLL_INTERVAL)


# ------------------- ENTRY -------------------
if __name__ == "__main__":
    logging.info("========== Bot Startup ==========")
    main_loop()
