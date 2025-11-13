import os
import time
import random
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta

# === Setup Logging ===
logging.basicConfig(
    filename="bot.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.info("=== Bot startup ===")

# === Configuration ===
NEWS_API_KEY = "pub_f22ba9249c104a038d7e1b904b949e3a"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-")
BATCH_SIZE = 8
BATCH_DELAY = 1.2
COOLDOWN_BASE = 30  # seconds, exponential backoff starts here
HIGH_THRESHOLD = 3
MED_THRESHOLD = 2

# === Keyword Weights (broad set) ===
keyword_weights = {
    "earnings": 3, "profit": 2, "loss": 2, "revenue": 3,
    "guidance": 3, "forecast": 2, "downgrade": 3, "upgrade": 3,
    "merger": 3, "acquisition": 3, "lawsuit": 2, "sec": 2,
    "ceo": 1, "cfo": 1, "resigns": 3, "fired": 3, "partnership": 2,
    "ipo": 3, "buyback": 2, "dividend": 2, "split": 2,
    "bankruptcy": 4, "layoffs": 3, "launch": 2, "expansion": 2,
    "regulation": 3, "investigation": 3, "fine": 2, "approval": 3,
    "market share": 2, "ai": 2, "chip": 2, "semiconductor": 2,
    "inflation": 2, "interest rates": 2, "fed": 2, "treasury": 1,
    "supply chain": 2, "innovation": 2, "contract": 2, "fda": 3,
    "growth": 2, "competition": 1, "earnings call": 3
}

# === Utility Functions ===
def get_watchlist():
    if not os.path.exists("watchlist.txt"):
        logging.warning("watchlist.txt not found.")
        return []
    with open("watchlist.txt", "r") as f:
        return [line.strip().upper() for line in f if line.strip()]

def fetch_sp500():
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        df = pd.read_html(requests.get(url, timeout=15).text)[0]
        tickers = df["Symbol"].tolist()
        logging.info(f"Fetched S&P500 master list: {len(tickers)} tickers.")
        return tickers
    except Exception as e:
        logging.error(f"Failed to load S&P500 list: {e}")
        return []

def batch_tickers(tickers, batch_size):
    for i in range(0, len(tickers), batch_size):
        yield tickers[i:i + batch_size]

def fetch_news_batch(tickers):
    query = " OR ".join(tickers)
    url = (
        f"https://newsdata.io/api/1/news?apikey={NEWS_API_KEY}"
        f"&q={query}&language=en&category=business&country=us"
    )
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 429:
            raise requests.exceptions.HTTPError("429")
        data = r.json()
        return data.get("results", [])
    except requests.exceptions.HTTPError as e:
        if "429" in str(e):
            raise
        else:
            logging.error(f"HTTP error on {query}: {e}")
            return []
    except Exception as e:
        logging.error(f"Batch fetch failed for {query}: {e}")
        return []

def evaluate_severity(article, ticker, watchlist):
    text = f"{article.get('title', '')} {article.get('description', '')}".lower()
    score = sum(weight for kw, weight in keyword_weights.items() if kw in text)
    if ticker in watchlist:
        score += 1  # watchlist bonus
    if score >= HIGH_THRESHOLD:
        return "HIGH"
    elif score >= MED_THRESHOLD:
        return "MED"
    return "LOW"

def send_discord_alert(article, ticker, severity):
    if not DISCORD_WEBHOOK_URL:
        logging.warning("No Discord webhook configured.")
        return
    title = article.get("title", "Untitled")
    url = article.get("link", "")
    msg = f"**{severity} ALERT** – {ticker}\n{title}\n{url}"
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})
        if r.status_code == 429:
            logging.warning("Discord rate limit reached (429).")
        elif r.status_code >= 300:
            logging.error(f"Discord error {r.status_code}: {r.text}")
    except Exception as e:
        logging.error(f"Discord post failed: {e}")

# === Main Bot Logic ===
def main():
    watchlist = get_watchlist()
    sp500 = fetch_sp500()

    # Maintain all watchlist tickers even if not in S&P500
    combined = list(set(watchlist))
    if sp500:
        combined += random.sample(sp500, max(0, 50 - len(watchlist)))
    combined = list(set(combined))
    logging.info(f"Cycle tickers: {len(combined)} total ({len(watchlist)} personal + {len(combined) - len(watchlist)} random).")

    cooldown_time = COOLDOWN_BASE
    high_count = med_count = low_count = posted = 0

    for batch in batch_tickers(combined, BATCH_SIZE):
        logging.info(f"Fetching batch: {batch}")
        try:
            articles = fetch_news_batch(batch)
        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                logging.warning(f"NewsData 429 detected. Cooling down {cooldown_time}s...")
                time.sleep(cooldown_time)
                cooldown_time = min(cooldown_time * 2, 300)  # exponential backoff
                continue
            else:
                logging.error(f"Batch failed {batch}: {e}")
                continue

        cooldown_time = COOLDOWN_BASE  # reset cooldown after successful batch

        # Assign articles to their tickers
        ticker_articles = {t: [] for t in batch}
        for art in articles:
            title = art.get("title", "").upper()
            for t in batch:
                if t in title:
                    ticker_articles[t].append(art)

        # Process
        for ticker, arts in ticker_articles.items():
            kept = []
            for art in arts:
                sev = evaluate_severity(art, ticker, watchlist)
                if sev == "HIGH":
                    send_discord_alert(art, ticker, sev)
                    posted += 1
                kept.append(sev)
            high_count += kept.count("HIGH")
            med_count += kept.count("MED")
            low_count += kept.count("LOW")
            logging.info(f"{ticker}: {len(arts)} fetched, {len(kept)} kept, {kept.count('HIGH')} high")

        time.sleep(BATCH_DELAY)

    logging.info(f"Cycle summary: HIGH={high_count} MED={med_count} LOW={low_count} posted={posted}")
    logging.info("Sleeping 6000s until next cycle.")

if __name__ == "__main__":
    main()
