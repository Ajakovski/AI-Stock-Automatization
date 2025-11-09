#!/usr/bin/env python3
"""
MVP: Lightweight US Stock Alerts (Form4 + NewsData)
- Uses official SEC Atom feed (no scraping)
- News via NewsData.io API
- Posts only MEDIUM/HIGH alerts to Discord
- Logs everything to 'bot.log'
"""

import os
import re
import time
import json
import logging
import sqlite3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pandas as pd
import pytz

# ------------------- CONFIG -------------------
load_dotenv()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL") or "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY") or "pub_f22ba9249c104a038d7e1b904b949e3a"
DEDUPE_MINUTES = int(os.getenv("DEDUPE_WINDOW_MINUTES", "30"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "90"))
WATCHLIST_SOURCE = os.getenv("WATCHLIST_SOURCE", "SP500")

USER_AGENT = {"User-Agent": "StockAI-Bot/1.1 (ajakovski@yahoo.com)"}
DB_PATH = "events.db"
LOG_FILE = "bot.log"

US_TZ = pytz.timezone("America/New_York")

# keywords
BUY_KEYWORDS = ["buy", "purchase", "acquire", "purchased", "bought"]
SELL_KEYWORDS = ["sell", "sold", "disposition", "sold out"]
CEO_KEYWORDS = ["chief executive", "ceo", "chief executive officer", "president"]
CFO_KEYWORDS = ["chief financial", "cfo", "chief financial officer"]
NEWS_ALERT_KEYWORDS = [
    "acquire", "merger", "buyback", "earnings", "downgrade", "upgrade",
    "surprise", "fraud", "restat", "guidance", "lawsuit", "sec investigation"
]

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
        event_type TEXT,
        role_flag TEXT,
        severity TEXT,
        title TEXT,
        link TEXT,
        raw TEXT,
        created_at TEXT
    )
    """)
    conn.commit()
    return conn

def already_seen(conn, source, ticker, event_type):
    c = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(minutes=DEDUPE_MINUTES)).isoformat()
    c.execute("""
    SELECT 1 FROM events
    WHERE source=? AND ticker=? AND event_type=? AND created_at>?
    """, (source, ticker, event_type, time_threshold))
    return c.fetchone() is not None

def save_event(conn, data):
    c = conn.cursor()
    c.execute("""
    INSERT INTO events (source,ticker,event_type,role_flag,severity,title,link,raw,created_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        data.get("source"),
        data.get("ticker"),
        data.get("event_type"),
        data.get("role_flag"),
        data.get("severity"),
        data.get("title"),
        data.get("link"),
        data.get("raw"),
        datetime.utcnow().isoformat()
    ))
    conn.commit()

# ------------------- SEC FORM 4 -------------------
def fetch_recent_form4_entries():
    url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&output=atom"
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=20)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = []
        for entry in root.findall('atom:entry', ns):
            title = entry.find('atom:title', ns).text
            link = entry.find('atom:link', ns).attrib.get('href')
            summary = entry.find('atom:summary', ns)
            summary_text = summary.text if summary is not None else ""
            m = re.search(r"\(([A-Z0-9.-]{1,6})\)", title or "")
            ticker = m.group(1).upper() if m else None
            entries.append({
                "title": title,
                "link": link,
                "summary": summary_text,
                "ticker": ticker
            })
        logging.info(f"Fetched {len(entries)} Form4 entries from SEC.")
        return entries
    except Exception as e:
        logging.error(f"SEC Form4 fetch error: {e}")
        return []

def score_form4(entry_text):
    t = entry_text.lower()
    severity = "LOW"
    if any(kw in t for kw in BUY_KEYWORDS):
        severity = "MED"
    if any(kw in t for kw in CEO_KEYWORDS + CFO_KEYWORDS):
        severity = "HIGH"
    return severity

# ------------------- NEWSDATA -------------------
def fetch_news_newsdata(tickers):
    results = []
    if not NEWSDATA_KEY:
        logging.warning("No NewsData API key found.")
        return []

    try:
        for i in range(0, len(tickers), 8):  # smaller batches = safer
            subset = tickers[i:i + 8]
            # Use spaces, not plus signs, and keep "OR" uppercased
            query = " OR ".join(subset)
            params = {
                "apikey": NEWSDATA_KEY,
                "q": query,
                "language": "en",
            }
            url = "https://newsdata.io/api/1/news"
            r = requests.get(url, params=params, headers=USER_AGENT, timeout=20)

            if r.status_code == 422:
                logging.warning(f"NewsData skipped bad query batch: {subset}")
                time.sleep(1)
                continue

            r.raise_for_status()
            data = r.json()
            for item in data.get("results", []):
                results.append({
                    "title": item.get("title"),
                    "description": item.get("description") or "",
                    "url": item.get("link"),
                    "source": item.get("source_id"),
                    "published": item.get("pubDate")
                })
            logging.info(f"NewsData batch ok ({len(results)} total so far)")
            time.sleep(1.5)

        logging.info(f"Fetched {len(results)} total news articles from NewsData.io.")
        return results

    except Exception as e:
        logging.error(f"NewsData fetch error: {e}")
        return []


def score_news(article):
    """
    Enhanced severity ranking for NewsData articles.
    Returns: LOW / MED / HIGH
    """
    text = (article.get("title", "") + " " + article.get("description", "")).lower()

    # --- High-impact triggers ---
    high_signals = [
        "merger", "acquisition", "acquires", "acquired", "buyout", "takeover",
        "lawsuit", "charged", "investigation", "sec investigation",
        "bankruptcy", "files for bankruptcy", "data breach", "fraud",
        "resignation", "steps down", "fined", "settlement", "layoffs",
        "mass layoff", "hack", "cyberattack", "guidance cut",
        "guidance lowered", "downgrade", "recall", "trading halted",
        "delisting", "collapse", "fire", "explosion"
    ]

    # --- Medium-impact triggers ---
    med_signals = [
        "earnings", "quarterly results", "eps", "revenue", "forecast",
        "price target", "beats", "misses", "upgrade", "expands",
        "partnership", "restructuring", "ipo", "share offering",
        "dividend", "new product", "guidance"
    ]

    high_weight = sum(1 for w in high_signals if w in text)
    med_weight = sum(1 for w in med_signals if w in text)

    severity = "LOW"
    if high_weight >= 1 or ("miss" in text and "expectation" in text):
        severity = "HIGH"
    elif med_weight >= 1:
        severity = "MED"

    # Escalate MED to HIGH if clearly negative
    if severity == "MED" and any(word in text for word in [
        "lawsuit", "fined", "downgrade", "layoff", "resignation", "bankruptcy"
    ]):
        severity = "HIGH"

    return severity


# ------------------- DISCORD -------------------
def post_to_discord(payload):
    if not DISCORD_WEBHOOK:
        logging.warning("No Discord webhook configured.")
        return

    desc = payload.get("description", "") or ""
    # Truncate long messages to prevent 400 errors
    if len(desc) > 950:
        desc = desc[:950] + "..."

    embed = {
        "title": payload.get("title"),
        "description": desc,
        "url": payload.get("link"),
        "fields": [
            {"name": "Ticker", "value": payload.get("ticker") or "N/A", "inline": True},
            {"name": "Source", "value": payload.get("source"), "inline": True},
            {"name": "Severity", "value": payload.get("severity"), "inline": True}
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

    data = {"content": None, "embeds": [embed]}
    headers = {"Content-Type": "application/json"}

    try:
        r = requests.post(DISCORD_WEBHOOK, json=data, headers=headers, timeout=10)
        if r.status_code not in (200, 204):
            logging.error(f"Discord error {r.status_code}: {r.text}")
    except Exception as e:
        logging.error(f"Discord post failed: {e}")

# ------------------- HELPERS -------------------
def load_sp500():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", {"id": "constituents"})
        df = pd.read_html(str(table))[0]
        tickers = set(df.Symbol.str.replace('.', '-', regex=False).str.upper().tolist())
        logging.info(f"Loaded {len(tickers)} S&P500 tickers.")
        return tickers
    except Exception as e:
        logging.error(f"S&P500 load failed: {e}")
        return set()

# ------------------- MAIN LOOP -------------------
def main_loop():
    conn = init_db()
    watchlist = load_sp500() if WATCHLIST_SOURCE == "SP500" else set()
    logging.info("Bot started successfully.")

    while True:
        try:
            # --- SEC Form 4 ---
            form4_entries = fetch_recent_form4_entries()
            for entry in form4_entries:
                if not entry.get("ticker") or entry["ticker"] not in watchlist:
                    continue
                text = entry.get("summary", "")
                severity = score_form4(text)
                if severity == "LOW":
                    continue
                if already_seen(conn, "form4", entry["ticker"], severity):
                    continue
                data = {
                    "source": "form4",
                    "ticker": entry["ticker"],
                    "event_type": "insider_tx",
                    "role_flag": None,
                    "severity": severity,
                    "title": f"Form4 Alert — {entry['ticker']} — {severity}",
                    "link": entry["link"],
                    "raw": text
                }
                save_event(conn, data)
                desc = f"**Event:** Insider transaction\n\n{entry['summary'][:600]}..."
                post_to_discord({
                    "title": data["title"],
                    "description": desc,
                    "link": data["link"],
                    "ticker": data["ticker"],
                    "source": "SEC Form 4",
                    "severity": severity
                })
                logging.info(f"Posted Form4 alert: {entry['ticker']} ({severity})")

            # --- NewsData ---
            news_items = fetch_news_newsdata(list(watchlist)[:100])
            for art in news_items:
                severity = score_news(art)
                if severity == "LOW":
                    continue
                if already_seen(conn, "news", art.get("title", "")[:100], severity):
                    continue
                data = {
                    "source": "news",
                    "ticker": "MULTI",
                    "event_type": "news",
                    "role_flag": None,
                    "severity": severity,
                    "title": art.get("title"),
                    "link": art.get("url"),
                    "raw": art.get("description", "")
                }
                save_event(conn, data)
                desc = f"**Headline:** {art.get('title')}\n\n{art.get('description')}"
                post_to_discord({
                    "title": f"News Alert — {severity}",
                    "description": desc,
                    "link": art.get("url"),
                    "ticker": data["ticker"],
                    "source": "NewsData.io",
                    "severity": severity
                })
                logging.info(f"Posted news alert: {art.get('title')} ({severity})")

        except Exception as e:
            logging.error(f"Main loop error: {e}")

        time.sleep(POLL_INTERVAL)

# ------------------- ENTRY -------------------
if __name__ == "__main__":
    main_loop()
