import os
import time
import logging
import requests
import sqlite3
from datetime import datetime, timedelta
import re
from urllib.parse import urlencode

# ========== CONFIG ==========
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_API_KEY = "pub_f22ba9249c104a038d7e1b904b949e3a"

DB_PATH = "events.db"
LOG_PATH = "bot.log"

POLL_INTERVAL = 180  # seconds (3 minutes)
DEDUPE_WINDOW_MINUTES = 30

# ========== LOGGER SETUP ==========
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("========== Bot Startup ==========")

# ========== SECTOR DICTIONARY ==========
SECTOR_TICKERS = {
    "Technology": ["AAPL", "MSFT", "NVDA", "AMD", "INTC", "GOOGL", "META", "CRM"],
    "Financials": ["JPM", "BAC", "GS", "MS", "C", "WFC", "BLK"],
    "Healthcare": ["UNH", "LLY", "JNJ", "PFE", "MRK", "TMO", "ABBV"],
    "Consumer Discretionary": ["AMZN", "TSLA", "HD", "NKE", "MCD", "SBUX"],
    "Energy": ["XOM", "CVX", "COP", "SLB", "EOG"],
    "Industrials": ["GE", "CAT", "UPS", "BA", "HON", "UNP"],
    "Materials": ["LIN", "SHW", "ECL", "NEM"],
    "Real Estate": ["O", "PLD", "SPG", "EQIX"],
    "Utilities": ["NEE", "DUK", "SO", "AEP"],
    "Communication Services": ["NFLX", "T", "VZ", "CMCSA"],
}

FIN_KEYWORDS = [
    "earnings", "forecast", "price target", "guidance",
    "acquisition", "merger", "downgrade", "upgrade",
    "buyback", "ipo", "sec", "investigation",
    "plunge", "surge", "profit", "loss",
    "beats expectations", "misses estimates", "dividend"
]

# ========== DATABASE SETUP ==========
# ========== DATABASE SETUP WITH MIGRATION ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Create table if it doesn't exist
    cur.execute(
        """CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            url TEXT UNIQUE,
            timestamp TEXT
        )"""
    )

    # --- Schema migration check ---
    # Get all current columns
    cur.execute("PRAGMA table_info(events)")
    cols = [c[1] for c in cur.fetchall()]

    # Ensure required columns exist
    required = {"id", "title", "url", "timestamp"}
    missing = required - set(cols)

    if missing:
        for col in missing:
            if col == "url":
                cur.execute("ALTER TABLE events ADD COLUMN url TEXT UNIQUE")
                logging.warning("Database migrated: added 'url' column.")
            elif col == "timestamp":
                cur.execute("ALTER TABLE events ADD COLUMN timestamp TEXT")
                logging.warning("Database migrated: added 'timestamp' column.")
            elif col == "title":
                cur.execute("ALTER TABLE events ADD COLUMN title TEXT")
                logging.warning("Database migrated: added 'title' column.")
        conn.commit()

    conn.close()


def is_duplicate(url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(minutes=DEDUPE_WINDOW_MINUTES)).isoformat()
    try:
        cur.execute("SELECT 1 FROM events WHERE url=? AND timestamp>?", (url, time_threshold))
        found = cur.fetchone()
    except sqlite3.OperationalError as e:
        logging.error(f"DB query failed: {e}")
        found = None
    conn.close()
    return bool(found)


def store_event(title, url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO events (title, url, timestamp) VALUES (?, ?, ?)",
            (title, url, datetime.utcnow().isoformat())
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        logging.error(f"DB insert failed: {e}")
    conn.close()

# ========== NEWS FETCH ==========
def fetch_news_for_sector(sector_name, tickers):
    query = " OR ".join(tickers)
    params = {
        "apikey": NEWSDATA_API_KEY,
        "q": query,
        "category": "business",
        "language": "en",
        "country": "us",
    }
    url = f"https://newsdata.io/api/1/news?{urlencode(params)}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        logging.info(f"Fetched {len(results)} articles for sector {sector_name}.")
        return results
    except Exception as e:
        logging.error(f"NewsData fetch error ({sector_name}): {e}")
        return []

# ========== RELEVANCE & SEVERITY ==========
def classify_article(article, tickers):
    title = (article.get("title") or "").lower()
    desc = (article.get("description") or "").lower()
    text = f"{title} {desc}"

    matched_tickers = [t for t in tickers if t.lower() in text]
    matched_keywords = [k for k in FIN_KEYWORDS if k in text]

    if not matched_tickers and not matched_keywords:
        return None, "Filtered (irrelevant)"

    severity = "LOW"
    if any(w in text for w in ["plunge", "surge", "downgrade", "acquisition", "investigation", "ipo"]):
        severity = "HIGH"
    elif any(w in text for w in ["earnings", "forecast", "price target", "guidance", "upgrade", "buyback", "dividend"]):
        severity = "MED"

    return severity, ", ".join(matched_tickers or matched_keywords)

# ========== DISCORD ALERT ==========
def post_to_discord(article, severity, sector, match_info):
    title = article.get("title", "No title")
    url = article.get("link", "No URL")
    desc = article.get("description", "No description")

    if len(desc) > 1024:
        desc = desc[:1021] + "..."

    embed = {
        "title": f"{severity} — {title}",
        "url": url,
        "color": 0xFF0000 if severity == "HIGH" else 0xFFA500,
        "fields": [
            {"name": "Sector", "value": sector, "inline": True},
            {"name": "Match", "value": match_info or "N/A", "inline": True},
            {"name": "Description", "value": desc},
        ],
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        r = requests.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=10)
        if r.status_code != 204:
            logging.error(f"Discord error {r.status_code}: {r.text}")
        else:
            logging.info(f"Posted alert: {title} ({severity})")
    except Exception as e:
        logging.error(f"Discord post failed: {e}")

# ========== MAIN LOOP ==========
def main():
    logging.info("Initializing database...")
    init_db()
    logging.info(f"Loaded {sum(len(v) for v in SECTOR_TICKERS.values())} tickers across {len(SECTOR_TICKERS)} sectors.")
    logging.info("Bot started successfully. (Form-4 fetching disabled for now.)")

    while True:
        total_articles = 0
        for sector, tickers in SECTOR_TICKERS.items():
            articles = fetch_news_for_sector(sector, tickers)
            total_articles += len(articles)

            for a in articles:
                url = a.get("link")
                if not url or is_duplicate(url):
                    continue
                severity, match_info = classify_article(a, tickers)
                if severity in ("MED", "HIGH"):
                    store_event(a.get("title", "No title"), url)
                    post_to_discord(a, severity, sector, match_info)
                else:
                    logging.info(f"Skipped ({severity or 'NONE'}) — {a.get('title', 'No title')} [{match_info}]")

        logging.info(f"Cycle complete. Total processed: {total_articles}. Sleeping {POLL_INTERVAL}s.")
        time.sleep(POLL_INTERVAL)

# ========== DISABLED: SEC FORM-4 FETCH ==========
# def fetch_sec_form4_entries():
#     pass  # Placeholder – temporarily disabled

# ========== STARTUP ==========
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
