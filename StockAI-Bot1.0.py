#!/usr/bin/env python3
"""
MVP: Lightweight News + Insider Alerts (US only, S&P500 watchlist)
Uses NewsData.io API for news ingestion (you provided key).
Run: pip install -r requirements.txt
Then: python mvp_alerts.py
"""

import os
import time
import sqlite3
import re
import json
from datetime import datetime, timedelta
import requests #pip install x
import feedparser #pip install x
from bs4 import BeautifulSoup #pip install x
from dotenv import load_dotenv #pip install x
import pytz #pip install x
import pandas as pd #pip install x
#download lxml parser if needed: pip install lxml

# Load env
load_dotenv()
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL") or "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-"
NEWSDATA_KEY = os.getenv("NEWSDATA_API_KEY") or "pub_f22ba9249c104a038d7e1b904b949e3a"
DEDUPE_MINUTES = int(os.getenv("DEDUPE_WINDOW_MINUTES", "30"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "90"))
WATCHLIST_SOURCE = os.getenv("WATCHLIST_SOURCE", "SP500")

DB_PATH = "events.db"
US_TZ = pytz.timezone("America/New_York")
USER_AGENT = {"User-Agent": "Lightweight-Insider-Bot/1.0 (ajakovski@yahoo.com)"} #my email

# keywords
BUY_KEYWORDS = ["buy", "purchase", "acquire", "purchased", "bought"]
SELL_KEYWORDS = ["sell", "sold", "disposition", "sold out"]
CEO_KEYWORDS = ["chief executive", "ceo", "chief executive officer", "president"]
CFO_KEYWORDS = ["chief financial", "cfo", "chief financial officer"]
NEWS_ALERT_KEYWORDS = ["acquire", "merger", "buyback", "earnings", "downgrade", "upgrade", "surprise", "fraud", "restat", "guidance"]

# DB init
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

def already_seen(conn, source, ticker, event_type, window_minutes=DEDUPE_MINUTES):
    c = conn.cursor()
    time_threshold = (datetime.utcnow() - timedelta(minutes=window_minutes)).isoformat()
    #datetime.datetime.now(tz=datetime.UTC)
    c.execute("""
    SELECT 1 FROM events WHERE source=? AND ticker=? AND event_type=? AND created_at>?
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

# Load S&P500 watchlist from Wikipedia
def load_sp500():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.find("table", {"id": "constituents"})
        df = pd.read_html(str(table))[0] # soon depricated
        tickers = set(df.Symbol.str.replace('.', '-', regex=False).str.upper().tolist())
        print(f"Loaded S&P500 watchlist: {len(tickers)} tickers")
        return tickers
    except Exception as e:
        print("Failed to load S&P500 from Wikipedia:", e)
        return set()

# SEC Form4 recent
SEC_BASE = "https://www.sec.gov"
SEC_RECENT_4 = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&count=200"

def fetch_recent_form4_links():
    r = requests.get(SEC_RECENT_4, headers=USER_AGENT, timeout=15)
    soup = BeautifulSoup(r.text, "lxml")
    rows = soup.find_all("tr")
    links = []
    for tr in rows:
        a = tr.find("a")
        if not a: 
            continue
        href = a.get("href")
        if href:
            full = href if href.startswith("http") else SEC_BASE + href
            links.append(full)
    uniq = []
    [uniq.append(x) for x in links if x not in uniq]
    return uniq

def parse_form4_page(url):
    try:
        r = requests.get(url, headers=USER_AGENT, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        text = soup.get_text(separator=" ", strip=True).lower()
        title = soup.title.string if soup.title else url
        # try to extract ticker from title like (AAPL)
        ticker_match = re.search(r"\(([A-Z0-9\.-]{1,6})\)", title)
        ticker = ticker_match.group(1).upper().replace('.', '-') if ticker_match else None
        # determine buy/sell
        event_type = "unknown"
        for kw in BUY_KEYWORDS:
            if kw in text:
                event_type = "buy"
                break
        for kw in SELL_KEYWORDS:
            if kw in text and event_type != "buy":
                event_type = "sell"
                break
        # role detection
        role_flag = None
        for kw in CEO_KEYWORDS:
            if kw in text:
                role_flag = "CEO"
                break
        for kw in CFO_KEYWORDS:
            if kw in text and role_flag is None:
                role_flag = "CFO"
                break
        raw_snip = " ".join(text.split()[:300])
        return {"title": title, "link": url, "ticker": ticker, "event_type": event_type, "role_flag": role_flag, "raw": raw_snip}
    except Exception as e:
        print("Error parsing Form4:", e)
        return None

# News ingestion via NewsData.io
def fetch_news_newsdata(query_terms, page=1, pagesize=20):
    """
    Query NewsData.io API with query_terms (string or OR-separated tickers).
    Docs: https://newsdata.io
    """
    if not NEWSDATA_KEY:
        return []
    endpoint = "https://newsdata.io/api/1/news"
    params = {
        "apikey": NEWSDATA_KEY,
        "q": query_terms,
        "language": "en",
        "page": page
    }
    try:
        r = requests.get(endpoint, params=params, headers=USER_AGENT, timeout=15)
        data = r.json()
        results = []
        for item in data.get("results", []):
            results.append({
                "title": item.get("title"),
                "description": item.get("description") or "",
                "url": item.get("link"),
                "publishedAt": item.get("pubDate") or item.get("pubDateTime"),
                "source": item.get("source_id")
            })
        return results
    except Exception as e:
        print("NewsData fetch error:", e)
        return []

# RSS fallback
RSS_FEEDS = [
    "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC&lang=en-US"
]

def fetch_news_rss():
    out = []
    for feed in RSS_FEEDS:
        try:
            d = feedparser.parse(feed)
            for entry in d.entries[:20]:
                out.append({
                    "title": entry.get("title"),
                    "description": entry.get("summary", ""),
                    "url": entry.get("link"),
                    "publishedAt": entry.get("published"),
                    "source": feed
                })
        except Exception as e:
            print("RSS fetch error", e)
    return out

# scoring
def score_form4(parsed):
    score = 0
    severity = "INFO"
    if parsed["event_type"] == "buy":
        score += 3
    if parsed["event_type"] == "sell":
        score += 1
    if parsed["role_flag"] == "CEO":
        score += 4
    if parsed["role_flag"] == "CFO":
        score += 3
    if score >= 6:
        severity = "HIGH"
    elif score >= 3:
        severity = "MED"
    else:
        severity = "LOW"
    return severity

def detect_tickers_in_text(text, watchlist):
    found = set()
    tokenized = re.findall(r"\$?[A-Z]{1,5}\b", text)
    for t in tokenized:
        t2 = t.replace("$", "")
        if t2.upper() in watchlist:
            found.add(t2.upper())
    return list(found)

def score_news(article, watchlist):
    txt = (article.get("title","") + " " + article.get("description","")).lower()
    tickers = detect_tickers_in_text(txt.upper(), watchlist)
    severity = "LOW"
    for kw in NEWS_ALERT_KEYWORDS:
        if kw in txt:
            severity = "MED"
            break
    if any(word in txt for word in ["merger", "acquisition", "buyback", "delisting"]):
        severity = "HIGH"
    return {"tickers": tickers, "severity": severity}

# Discord post
def post_to_discord(payload: dict):
    if not DISCORD_WEBHOOK:
        print("No DISCORD_WEBHOOK set. Would alert:", payload)
        return
    data = {
        "content": None,
        "embeds": [
            {
                "title": payload.get("title"),
                "description": payload.get("description"),
                "url": payload.get("link"),
                "fields": [
                    {"name": "Ticker", "value": payload.get("ticker") or "N/A", "inline": True},
                    {"name": "Source", "value": payload.get("source"), "inline": True},
                    {"name": "Severity", "value": payload.get("severity"), "inline": True}
                ],
                "timestamp": datetime.utcnow().isoformat()
            }
        ]
    }
    headers = {"Content-Type": "application/json"}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=data, headers=headers, timeout=10)
        if r.status_code not in (200,204):
            print("Discord webhook error", r.status_code, r.text)
    except Exception as e:
        print("Discord post error", e)

# Main loop
def main_loop():
    conn = init_db()
    watchlist = load_sp500() if WATCHLIST_SOURCE == "SP500" else set()
    print("Starting poll loop. Poll interval:", POLL_INTERVAL)
    seen_links = set()
    while True:
        try:
            # Form 4
            links = fetch_recent_form4_links()
            for link in links[:150]:
                if link in seen_links:
                    continue
                seen_links.add(link)
                parsed = parse_form4_page(link)
                if not parsed:
                    continue
                if not parsed["ticker"]:
                    m = re.search(r"ticker[:\s]+([A-Z]{1,5})", parsed["raw"], re.I)
                    if m:
                        parsed["ticker"] = m.group(1).upper()
                if parsed["ticker"] and parsed["ticker"] not in watchlist:
                    continue
                if already_seen(conn, "form4", parsed["ticker"] or "UNKNOWN", parsed["event_type"]):
                    print("Duplicate event suppressed:", parsed["ticker"], parsed["event_type"])
                    continue
                severity = score_form4(parsed)
                data = {
                    "source": "form4",
                    "ticker": parsed["ticker"],
                    "event_type": parsed["event_type"],
                    "role_flag": parsed["role_flag"],
                    "severity": severity,
                    "title": f"Form4: {parsed['title']}",
                    "link": parsed["link"],
                    "raw": parsed["raw"]
                }
                save_event(conn, data)
                desc = f"**Event:** {parsed['event_type'].upper()}  \n**Role:** {parsed['role_flag'] or 'Unknown'}\n\nSnippet:\n{parsed['raw'][:600]}..."
                post_to_discord({
                    "title": f"Form4 Alert — {parsed.get('ticker') or 'Unknown'} — {severity}",
                    "description": desc,
                    "link": parsed["link"],
                    "ticker": parsed.get("ticker"),
                    "source": "SEC Form 4",
                    "severity": severity
                })
                print("Posted Form4 alert:", parsed.get("ticker"), parsed["event_type"], severity)

            # News ingestion: construct a short query using a subset of watchlist (avoid giant queries)
            watch_subset = list(watchlist)[:200]  # limit to 200 tickers for query size
            query = " OR ".join(watch_subset)
            news_items = []
            if NEWSDATA_KEY:
                news_items = fetch_news_newsdata(query_terms=query, page=1)
            else:
                news_items = fetch_news_rss()
            for art in news_items:
                txt = (art.get("title","") + " " + art.get("description","")).lower()
                tickers = detect_tickers_in_text(txt.upper(), watchlist)
                if not tickers:
                    continue
                score = score_news(art, watchlist)
                for tk in tickers:
                    if already_seen(conn, "news", tk, score["severity"]):
                        continue
                    payload = {
                        "source": "news",
                        "ticker": tk,
                        "event_type": "news",
                        "role_flag": None,
                        "severity": score["severity"],
                        "title": f"News: {art.get('title')}",
                        "link": art.get("url"),
                        "raw": art.get("description","")
                    }
                    save_event(conn, payload)
                    desc = f"**Headline:** {art.get('title')}\n**Source:** {art.get('source')}\n\n{art.get('description')}"
                    post_to_discord({
                        "title": f"News Alert — {tk} — {score['severity']}",
                        "description": desc,
                        "link": art.get("url"),
                        "ticker": tk,
                        "source": art.get("source") or "NewsData/RSS",
                        "severity": score["severity"]
                    })
                    print("Posted news alert", tk, score["severity"])

        except Exception as e:
            print("Main loop error:", e)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main_loop()
