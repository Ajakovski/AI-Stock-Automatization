#!/usr/bin/env python3
"""
Corrected MVP alerts:
- Robust S&P500 parsing (BeautifulSoup, no html5lib)
- NewsData: avoid unsupported filters causing 422; filter locally for ticker-in-title
- DB migration + dedupe
- HIGH-only posting (threshold adjustable)
- Per-ticker logging and cycle summary
"""

import os
import time
import json
import random
import logging
import sqlite3
import requests
import re
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL") or "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY") or "pub_f22ba9249c104a038d7e1b904b949e3a"
WATCHLIST_FILE = "watchlist.txt"
RECENT_RANDOMS_FILE = "recent_randoms.json"
DB_PATH = "events.db"
LOG_FILE = "bot.log"

PERSONAL_TARGET = 50
ROTATION_KEEP = 3
DEDUPE_HOURS = 12
POLL_INTERVAL = 100 * 60  # 100 minutes
HIGH_THRESHOLD = 5  # score >= this becomes HIGH

USER_AGENT = {"User-Agent": "StockAI-Bot/1.5.2 (andrej@local)"}
#Too strict thresholds can lead to no posts; adjust as needed

# expanded keyword weights (same idea as before)
keyword_weights = {
    "bankruptcy": 5, "files for bankruptcy": 5,
    "lawsuit": 4, "sued": 4, "sec investigation": 4, "investigation": 4,
    "charged": 4, "indicted": 4, "fraud": 4,
    "recall": 4, "data breach": 4, "hack": 4, "cyberattack": 4,
    "layoff": 3, "job cuts": 3, "resignation": 3,
    "delisting": 4, "trading halted": 4, "collapse": 4, "plunge": 3, "crash": 4,
    "merger": 3, "acquisition": 3, "buyout": 3, "takeover": 3,
    "ipo": 3, "spinoff": 3, "share offering": 3,
    "guidance cut": 3, "downgrade": 3, "upgrade": 2,
    "earnings": 2, "eps": 2, "revenue": 2, "misses expectations": 3, "beats expectations": 3,
    "insider": 2, "buys shares": 2, "sells shares": 2,
    "ceo": 2, "cfo": 2,
    "dividend": 2, "price target": 2, "partnership": 2,
    "restructuring": 2, "new product": 1, "record high": 2, "record low": 2,
}

# ---------------- LOGGING ----------------
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

# ---------------- DB / migration ----------------
def init_db_and_migrate():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            source TEXT,
            ticker TEXT,
            title TEXT,
            severity TEXT,
            url TEXT UNIQUE,
            created_at TEXT
        )
    """)
    conn.commit()
    # ensure columns exist (safe)
    c.execute("PRAGMA table_info(events)")
    cols = {r[1] for r in c.fetchall()}
    needed = {"id","source","ticker","title","severity","url","created_at"}
    missing = needed - cols
    for m in missing:
        try:
            if m != "id":
                c.execute(f"ALTER TABLE events ADD COLUMN {m} TEXT")
                logging.info(f"DB migration: added column {m}")
        except Exception as e:
            logging.debug(f"DB migration: could not add {m}: {e}")
    conn.commit()
    return conn

def seen_recent(title, url, hours=DEDUPE_HOURS):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        if url:
            cur.execute("SELECT 1 FROM events WHERE url=? AND created_at>?", (url, cutoff))
            if cur.fetchone():
                return True
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

def store_event(source, ticker, title, severity, url):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO events (source,ticker,title,severity,url,created_at) VALUES (?,?,?,?,?,?)",
            (source,ticker,title,severity,url,datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
    except Exception as e:
        logging.error(f"store_event error: {e}")
    finally:
        conn.close()

# ---------------- S&P500 parsing (BeautifulSoup) ----------------
def fetch_sp500():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        tbl = soup.find("table", {"id": "constituents"})
        tickers = []
        if tbl:
            for row in tbl.find_all("tr")[1:]:
                cells = row.find_all(["td","th"])
                if not cells:
                    continue
                # first column is the ticker symbol in that Wikipedia table
                sym = cells[0].get_text(strip=True)
                if sym:
                    sym = sym.replace(".", "-").upper()
                    tickers.append(sym)
        if len(tickers) >= 100:
            logging.info(f"Fetched S&P500 master list: {len(tickers)} tickers.")
            return tickers
        # fallback attempt: try to find any <table> that looks like the constituents table
        # try scanning all tables for a column with ticker-like values
        all_tables = soup.find_all("table")
        for t in all_tables:
            rows = t.find_all("tr")
            if not rows or len(rows) < 5:
                continue
            # check first data row cell count and content
            first_cells = rows[1].find_all("td")
            if not first_cells:
                continue
            candidate = rows[1].find_all("td")[0].get_text(strip=True)
            if re.match(r'^[A-Za-z0-9\.-]{1,6}$', candidate):
                # parse table column 0
                cand = []
                for r in rows[1:]:
                    c = r.find_all("td")
                    if c:
                        s = c[0].get_text(strip=True).replace(".", "-").upper()
                        cand.append(s)
                if len(cand) >= 100:
                    logging.info(f"Fetched S&P500 (fallback) list: {len(cand)} tickers.")
                    return cand
        logging.error("S&P500 fetch: insufficient tickers found; returning empty list.")
        return []
    except Exception as e:
        logging.error(f"Failed to load S&P500 list: {e}")
        return []

# ---------------- watchlist and rotation ----------------
def load_watchlist():
    if not os.path.exists(WATCHLIST_FILE):
        logging.warning("watchlist.txt missing.")
        return []
    with open(WATCHLIST_FILE, "r", encoding="utf-8") as f:
        toks = [l.strip().upper() for l in f if l.strip()]
    # preserve order and dedupe
    out = []
    seen = set()
    for t in toks:
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
    except Exception as e:
        logging.error(f"load_recent_randoms error: {e}")
    return []

def save_recent_randoms(recent):
    try:
        with open(RECENT_RANDOMS_FILE, "w", encoding="utf-8") as f:
            json.dump(recent[:ROTATION_KEEP], f, indent=2)
    except Exception as e:
        logging.error(f"save_recent_randoms error: {e}")

def build_cycle(personal_list, sp500_list):
    personal = [t.upper() for t in personal_list]
    sp_pool = [t for t in sp500_list if t not in set(personal)]
    random.shuffle(sp_pool)
    recent = load_recent_randoms()
    recent_flat = set(sum([r if isinstance(r, list) else [] for r in recent], []))
    need = max(0, PERSONAL_TARGET - len(personal))
    candidates = [t for t in sp_pool if t not in recent_flat]
    if len(candidates) < need:
        candidates = sp_pool
    picks = candidates[:need]
    combined = personal + picks
    recent.insert(0, picks)
    save_recent_randoms(recent)
    logging.info(f"Cycle tickers: {len(combined)} total ({len(personal)} personal + {len(picks)} random).")
    logging.info(f"Random picks this cycle: {picks}")
    return combined, set(personal)

# ---------------- NewsData fetch (single ticker) ----------------
def fetch_news_for_ticker(ticker):
    if not NEWSDATA_KEY:
        logging.warning("No NewsData API key.")
        return []
    url = "https://newsdata.io/api/1/news"
    params = {
        "apikey": NEWSDATA_KEY,
        "q": ticker,
        "language": "en",
        "category": "business",
        "country": "us"
    }
    try:
        r = requests.get(url, params=params, headers=USER_AGENT, timeout=18)
        # NewsData can return 422 for unsupported filters; handle gracefully
        if r.status_code == 422:
            logging.error(f"NewsData fetch error 422: {r.text}")
            return []
        r.raise_for_status()
        j = r.json()
        results = j.get("results", []) or []
        logging.info(f"Fetched {len(results)} articles for {ticker}")
        normalized = []
        for it in results:
            normalized.append({
                "title": (it.get("title") or "").strip(),
                "description": (it.get("description") or "").strip(),
                "url": (it.get("link") or it.get("url") or "").strip(),
                "source": it.get("source_id"),
                "published": it.get("pubDate")
            })
        return normalized
    except Exception as e:
        logging.error(f"NewsData fetch error: {e}")
        return []

# ---------------- severity scoring ----------------
def score_article(article, ticker):
    title = (article.get("title") or "")
    desc = (article.get("description") or "")
    text = (title + " " + desc).lower()
    score = 0
    for k, w in keyword_weights.items():
        if k in text:
            score += w
    # boost if ticker appears in title (strong signal)
    if ticker.lower() in title.lower():
        score += 2
    # numeric pct moves boost
    if re.search(r"\b-?\d+(\.\d+)?%\b", text):
        score += 1
    # final classification
    if score >= HIGH_THRESHOLD:
        return "HIGH"
    elif score >= (HIGH_THRESHOLD - 2):
        return "MED"
    else:
        return "LOW"

# ---------------- Discord posting with rate-limit respect ----------------
def post_to_discord(payload_embed):
    if not DISCORD_WEBHOOK:
        logging.warning("No webhook configured.")
        return False
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload_embed, headers=headers, timeout=12)
        if r.status_code in (200, 204):
            return True
        if r.status_code == 429:
            # respect retry_after if present
            try:
                js = r.json()
                ra = float(js.get("retry_after", 1.0))
            except Exception:
                ra = 1.0
            logging.warning(f"Discord rate limited, sleeping {ra:.2f}s")
            time.sleep(ra + 0.2)
            return False
        logging.error(f"Discord error {r.status_code}: {r.text}")
        return False
    except Exception as e:
        logging.error(f"Discord post failed: {e}")
        return False

def post_news_to_discord(ticker, title, description, url, severity):
    # build truncated embed-like payload (simple webhook message to avoid embed size issues)
    msg = f"**{ticker} | {severity}** — {title}\n{url}"
    payload = {"content": msg}
    ok = post_to_discord(payload)
    if ok:
        logging.info(f"Discord posted: {ticker} | {severity} | {title}")
    return ok

# ---------------- MAIN LOOP ----------------
def main_loop():
    init_db_and_migrate()
    sp500 = fetch_sp500()
    if not sp500:
        logging.warning("S&P500 empty; randoms disabled until next fetch.")
    while True:
        try:
            watch = load_watchlist()
            sp500 = fetch_sp500() or sp500
            tickers, personal_set = build_cycle(watch, sp500)
            logging.info("Starting news scan cycle...")
            counts = {"HIGH":0,"MED":0,"LOW":0,"posted":0}
            for t in tickers:
                articles = fetch_news_for_ticker(t)
                time.sleep(0.7)  # small spacing
                fetched = len(articles)
                kept = 0
                posted_for_ticker = 0
                for a in articles:
                    title = a.get("title") or ""
                    if not title:
                        continue
                    url = a.get("url") or ""
                    # LOCAL filter: ensure ticker is in the article title to avoid generic results
                    if t.lower() not in title.lower():
                        # optional: if in description, still accept but weaker — for now skip
                        continue
                    if seen_recent(title, url):
                        continue
                    severity = score_article(a, t)
                    counts[severity] += 1
                    kept += 1
                    # Only HIGH posts (balanced/quiet)
                    if severity == "HIGH":
                        store_event("news", t, title, severity, url)
                        sent = post_news_to_discord(t, title, a.get("description",""), url, severity)
                        if sent:
                            counts["posted"] += 1
                            posted_for_ticker += 1
                        # backoff between posts
                        time.sleep(random.uniform(1.0,2.0))
                logging.info(f"{t}: {fetched} fetched, {kept} kept, {posted_for_ticker} posted")
            logging.info(f"Cycle summary: HIGH={counts['HIGH']} MED={counts['MED']} LOW={counts['LOW']} posted={counts['posted']}")
        except Exception as e:
            logging.exception(f"Main loop error: {e}")
        logging.info(f"Sleeping {POLL_INTERVAL}s until next cycle.")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main_loop()
