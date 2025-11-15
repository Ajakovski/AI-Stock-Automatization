#!/usr/bin/env python3
"""
mvp_alerts.py
Final corrected MVP:
- Hybrid company name map with FAST Yahoo scraping (profile page title/h1)
- Weighted severity model (broader keyword_weights)
- Only HIGH alerts are immediately posted to Discord (per your request)
- Filtered Alert Strategy: Balanced Mode by default (FILTER_MODE = 2)
- Smart Cool-Down System (per-article dedupe / cooldown by ticker + title/url)
- Pagination and NewsData error handling + exponential backoff for 429 and transient errors
- Batch fetching with small sleeps to avoid rate-limits
- Full logging of fetched/kept/posted counts (bot.log style)
- Infinite loop (LOOP_MODE=1) default; can be set to single-run for testing
"""

import os
import json
import time
import logging
import hashlib
import random
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# --------------------------
# Configuration (tweak here)
# --------------------------
API_KEY = os.getenv("NEWSDATA_API_KEY", "pub_f22ba9249c104a038d7e1b904b949e3a")  # set via env or replace
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK", "https://discord.com/api/webhooks/1435654882710519888/ZDx_dGG22dknR4hGrENapdaG1Cm-VyUCUvrXmI6kGxcw0KLILP5AJKmNB14L9TzD65J-")  # set if you want Discord posting
WATCHLIST_FILE = os.getenv("WATCHLIST_FILE", "watchlist.txt")
COMPANY_MAP_FILE = os.getenv("COMPANY_MAP_FILE", "company_map.json")
SNP500_CACHE_FILE = os.getenv("SNP500_CACHE_FILE", "sp500_list.json")
LOOP_MODE = int(os.getenv("LOOP_MODE", "1"))  # 1 = infinite loop, 0 = single run
LOOP_SLEEP = int(os.getenv("LOOP_SLEEP", "6000"))  # seconds between cycles
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "8"))  # how many tickers to fetch concurrently (sequentially in a batch)
REQUEST_PAUSE = float(os.getenv("REQUEST_PAUSE", "0.9"))  # pause between requests (seconds)
MAX_PAGE = int(os.getenv("MAX_PAGE", "1"))  # number pages to fetch per query (1 is default)
TICKER_MODE = os.getenv("TICKER_MODE", "A")  # "A" = ticker-only messages, future options possible
SEARCH_MODE = int(os.getenv("SEARCH_MODE", "1"))  # 1 = use company name (Hybrid will map), 0 = search by ticker
FILTER_MODE = int(os.getenv("FILTER_MODE", "2"))  # 0: Permissive, 1: Strict, 2: Balanced (default)
SMART_COOLDOWN_SECONDS = int(os.getenv("SMART_COOLDOWN_SECONDS", str(30 * 60)))  # avoid reposting same content within this seconds
DEDUP_DB_FILE = os.getenv("DEDUP_DB_FILE", "posted_cache.json")
LOGFILE = os.getenv("BOT_LOGFILE", "bot.log")

# Weighted severity threshold (total score)
# Tune these to control how many High/Med/Low
SEVERITY_THRESHOLDS = {
    "HIGH": 6.0,   # score >= HIGH threshold => HIGH (we post HIGH only)
    "MED": 3.0,    # score >= MED threshold => MED (kept but not posted)
    "LOW": 0.0
}

# Balanced/Strict/Permissive adjustments
FILTER_MODE_ADJUST = {
    # multiplicative factors applied to keywords weight sum to make it easier/harder to reach thresholds.
    0: 0.8,   # Permissive: easier to trigger
    1: 1.4,   # Strict: harder to trigger
    2: 1.0    # Balanced: default
}

# Broader keyword_weights dictionary (expanded)
# This dictionary should be tuned over time. It covers corporate actions, earnings, legal,
# macro, management, product, M&A, downgrades/upgrades, insider, guidance, layoffs, partnerships, recall, regulator, fraud.
# Positive and negative words both matter; we'll treat them as signals for severity (not direction).
keyword_weights = {
    # Corporate actions & capital markets
    "acquir": 2.5, "acquisition": 2.5, "merger": 2.5, "buyout": 2.5,
    "secondary offering": 2.0, "ipo": 2.0, "dividend": 1.5, "split": 1.2,
    "bankruptcy": 5.0, "chapter 11": 5.0, "delist": 4.5, "delisting": 4.5,
    # Earnings & guidance
    "quarter": 1.5, "q1": 1.5, "q2": 1.5, "q3": 1.5, "q4": 1.5,
    "earnings": 3.0, "beat": 2.5, "miss": 2.5, "guidance": 3.0, "forecast": 2.0,
    # Legal & regulatory
    "lawsuit": 3.5, "sued": 3.5, "charged": 4.0, "investigation": 3.0,
    "regulator": 2.0, "fine": 3.0, "settlement": 2.5,
    # Management & insider
    "resign": 2.5, "resignation": 2.5, "appointed": 1.5, "named": 1.0,
    "insider": 2.5, "acquires": 2.0, "buys shares": 2.0, "sells shares": 2.0,
    # Market-moving events
    "layoff": 3.5, "restructure": 2.5, "recall": 3.0, "cyber": 3.0, "breach": 3.5, "outage": 2.0,
    "merger agreement": 3.5, "deal": 2.0, "bid": 2.0, "offer": 1.5,
    # Ratings & price targets
    "downgrade": 2.5, "upgrade": 2.0, "price target": 1.5, "price target to": 1.5,
    "analyst": 1.2,
    # Macro and sector items that may still be relevant
    "interest rate": 2.0, "inflation": 2.0, "fed": 2.0, "bank run": 5.0,
    # Product / tech / supply chain
    "launch": 1.5, "recall": 3.0, "shortage": 2.0, "supply chain": 1.5,
    # Fraud & restatements
    "fraud": 5.0, "restat": 4.5, "fraudulent": 5.0, "accounting irregular": 4.0,
    # Mergers & rumors
    "rumor": 1.5, "speculation": 1.0, "talks": 1.2,
    # Other high-signal events
    "credit rating": 2.5, "credit watch": 3.0, "loan": 1.0, "default": 4.5,
    # Geography / sanction-level items
    "sanction": 4.0, "tariff": 2.0, "embargo": 3.5,
}

# Make keyword keys lowercase for safe matching
keyword_weights = {k.lower(): float(v) for k, v in keyword_weights.items()}

# --------------------------
# Logging setup
# --------------------------
logger = logging.getLogger("mvp_alerts")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOGFILE)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)
sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)

# --------------------------
# Utilities
# --------------------------
def now_iso() -> str:
    return datetime.utcnow().isoformat()

def safe_read_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def safe_write_json(path: str, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# --------------------------
# Company name hybrid system
# --------------------------
company_map: Dict[str, str] = safe_read_json(COMPANY_MAP_FILE, {})

def yahoo_fast_name_scrape(ticker: str) -> Optional[str]:
    """
    FAST Yahoo scraping: use profile page title / h1 to extract company name.
    Returns None on failure.
    """
    profile_url = f"https://finance.yahoo.com/quote/{ticker}/profile"
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; mvp_alerts/1.6; +https://example.com/bot)"
    }
    try:
        r = requests.get(profile_url, headers=headers, timeout=8)
        if r.status_code != 200:
            logger.warning("Yahoo scrape non-200 for %s: %s", ticker, r.status_code)
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        # Try multiple strategies: h1, title, meta
        h1 = soup.find("h1")
        if h1 and h1.text.strip():
            # Often like "Alphabet Inc. (GOOG) Company Profile & Facts - Yahoo Finance"
            name = re.sub(r"\s+Company.*$", "", h1.text.strip())
            name = re.sub(r"\s*\(\w+\)\s*$", "", name).strip()
            return name
        # fallback: title
        title = soup.title.string if soup.title else None
        if title:
            # try to extract leading name before '-' or '|'
            parts = re.split(r"-|\|", title)
            cand = parts[0].strip()
            cand = re.sub(r"\(\w+\)$", "", cand).strip()
            if len(cand) > 2:
                return cand
        return None
    except Exception as e:
        logger.warning("Yahoo scrape failed for %s: %s", ticker, e)
        return None

def get_company_name_for_ticker(ticker: str) -> str:
    ticker = ticker.upper()
    if ticker in company_map and company_map[ticker]:
        return company_map[ticker]
    # Fast scrape
    name = yahoo_fast_name_scrape(ticker)
    if name:
        company_map[ticker] = name
        safe_write_json(COMPANY_MAP_FILE, company_map)
        logger.info("Mapped %s -> %s (saved)", ticker, name)
        return name
    # fallback: return ticker
    company_map[ticker] = ticker
    safe_write_json(COMPANY_MAP_FILE, company_map)
    logger.warning("BAD_NAME: %s (saved fallback ticker as name). Please correct %s in %s", ticker, ticker, COMPANY_MAP_FILE)
    return ticker

# --------------------------
# Dedupe / cooldown system
# --------------------------
posted_cache = safe_read_json(DEDUP_DB_FILE, {})  # structure: {hash: timestamp_iso}

def make_article_hash(article: Dict) -> str:
    # Hash title + url + pubDate if present
    s = (article.get("title") or "") + "|" + (article.get("link") or article.get("url") or "") + "|" + (article.get("pubDate") or "")
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def is_recently_posted(article_hash: str) -> bool:
    ts_iso = posted_cache.get(article_hash)
    if not ts_iso:
        return False
    try:
        ts = datetime.fromisoformat(ts_iso)
    except Exception:
        return False
    if datetime.utcnow() - ts < timedelta(seconds=SMART_COOLDOWN_SECONDS):
        return True
    return False

def mark_posted(article_hash: str):
    posted_cache[article_hash] = datetime.utcnow().isoformat()
    # persist
    safe_write_json(DEDUP_DB_FILE, posted_cache)

def cleanup_posted_cache():
    """Remove old entries to keep file small."""
    cutoff = datetime.utcnow() - timedelta(seconds=SMART_COOLDOWN_SECONDS * 3)
    keys_to_delete = [k for k, v in posted_cache.items() if datetime.fromisoformat(v) < cutoff]
    for k in keys_to_delete:
        del posted_cache[k]
    if keys_to_delete:
        safe_write_json(DEDUP_DB_FILE, posted_cache)

# --------------------------
# Severity scoring
# --------------------------
word_split_re = re.compile(r"\W+")

def article_score(article_text: str) -> float:
    """Compute weighted sum based on keyword_weights (case-insensitive, tokenized)."""
    text = article_text.lower()
    # simple presence and count approach: if a keyword is multi-word, search substring; else token count
    score = 0.0
    for kw, w in keyword_weights.items():
        if " " in kw:
            if kw in text:
                score += w
        else:
            # token count
            count = len(re.findall(r"\b" + re.escape(kw) + r"\b", text))
            if count:
                score += w * count
    # apply filter mode adjustment
    factor = FILTER_MODE_ADJUST.get(FILTER_MODE, 1.0)
    return score * factor

def severity_from_score(score: float) -> str:
    if score >= SEVERITY_THRESHOLDS["HIGH"]:
        return "HIGH"
    if score >= SEVERITY_THRESHOLDS["MED"]:
        return "MED"
    return "LOW"

# --------------------------
# NewsData API fetch logic
# --------------------------
NEWSDATA_BASE = "https://newsdata.io/api/1/news"

def fetch_news_for_query(query: str, page: int = 1) -> Tuple[List[Dict], Optional[dict]]:
    """
    Query NewsData API.
    Returns list of articles and the raw JSON (if returned).
    Defensive: handles 422, 429, 401, other errors and retries with backoff.
    """
    params = {
        "apikey": API_KEY,
        "q": query,
        "language": "en",
        "category": "business",
        "country": "us",
        "page": page
    }
    headers = {"User-Agent": "mvp_alerts/1.0"}
    attempt = 0
    backoff = 1.2
    while attempt < 5:
        attempt += 1
        try:
            r = requests.get(NEWSDATA_BASE, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                # NewsData returns "results": [] or "results" containing array? docs vary; handle both
                articles = data.get("results") or data.get("articles") or data.get("data") or []
                # Normalize article fields to 'title' and 'link' etc.
                normalized = []
                for a in articles:
                    title = a.get("title") or a.get("headline") or ""
                    link = a.get("link") or a.get("url") or a.get("source_url") or a.get("original_url") or a.get("source")
                    description = a.get("description") or a.get("summary") or a.get("content") or ""
                    pubDate = a.get("pubDate") or a.get("pubDateTime") or a.get("pubDateUtc") or a.get("pubDateLocal") or ""
                    source = a.get("source_id") or a.get("source") or ""
                    normalized.append({
                        "title": title,
                        "link": link,
                        "description": description,
                        "pubDate": pubDate,
                        "source": source,
                        "raw": a
                    })
                return normalized, data
            elif r.status_code == 401:
                logger.error("NewsData HTTP error 401: UNAUTHORIZED - check API_KEY")
                return [], None
            elif r.status_code == 422:
                # UnsupportedFilter or pagination issue; return empty and let caller handle logic
                try:
                    body = r.json()
                except Exception:
                    body = {"status": "error", "content": r.text}
                logger.warning("NewsData 422 for query %r: %s", query, body)
                return [], body
            elif r.status_code == 429:
                # rate limited
                logger.warning("NewsData 429 TOO MANY REQUESTS - backing off %.1fs (attempt %d)", backoff, attempt)
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                logger.warning("NewsData HTTP error %s for query %r: %s", r.status_code, query, r.text[:200])
                time.sleep(backoff)
                backoff *= 2
                continue
        except requests.RequestException as e:
            logger.warning("NewsData fetch exception for query %r: %s (attempt %d)", query, e, attempt)
            time.sleep(backoff)
            backoff *= 2
            continue
    logger.error("NewsData fetch ultimately failed for query %r after retries", query)
    return [], None

# --------------------------
# Discord posting (HIGH only)
# --------------------------
def post_to_discord(ticker: str, company_name: str, severity: str, score: float, article: Dict) -> bool:
    if not DISCORD_WEBHOOK:
        logger.info("Discord webhook not configured; skipping post for %s", ticker)
        return False
    title = article.get("title") or "No title"
    link = article.get("link") or ""
    content = f"{ticker} | {severity} | score:{score:.1f} — {company_name} — {title}\n{link}"
    payload = {"content": content}
    try:
        r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=8)
        if r.status_code in (200, 204):
            logger.info("Discord posted: %s | %s — %s", ticker, severity, title)
            return True
        else:
            logger.warning("Discord post failed %s: %s", r.status_code, r.text[:200])
            return False
    except Exception as e:
        logger.warning("Discord post exception: %s", e)
        return False

# --------------------------
# Main processing for a single ticker
# --------------------------
def process_ticker(ticker: str) -> Tuple[int, int, int]:
    """
    Returns: (fetched_count, kept_count, posted_high_count)
    Kept = articles with severity MED or HIGH (we keep them in internal metrics)
    Posted_high_count = number of HIGH articles posted to Discord (deduped by cooldown)
    """
    fetched_total = 0
    kept_total = 0
    posted_total = 0

    # Decide query based on SEARCH_MODE and Hybrid names
    if SEARCH_MODE == 1:
        company_name = get_company_name_for_ticker(ticker)
        query = company_name
    else:
        query = ticker

    # Try up to MAX_PAGE pages (note: NewsData free tiers might not support pages)
    for page in range(1, MAX_PAGE + 1):
        # surround query with quotes to search exact company phrase if it's a full name (helps relevance)
        qparam = query
        # If query is a single ticker, don't quote
        if len(query) > 1 and not re.fullmatch(r"^[A-Z]{1,5}$", query):
            # add quotes only if we have spaces (company full name) or if it's not a pure ticker
            if " " in query:
                qparam = f'"{query}"'
        articles, raw = fetch_news_for_query(qparam, page=page)
        fetched_total += len(articles)
        # If we got a 422 body as raw and no articles, break; nothing to do
        if raw and isinstance(raw, dict) and raw.get("status") == "error" and not articles:
            # unsupported filter/pagination or other; avoid further paging
            break

        for art in articles:
            # Compose search text to score
            text_for_score = " ".join([art.get("title") or "", art.get("description") or ""])
            score = article_score(text_for_score)
            severity = severity_from_score(score)
            # Keep MED and HIGH internally; only post HIGH
            if severity in ("MED", "HIGH"):
                kept_total += 1
            # Dedupe check for HIGH
            if severity == "HIGH":
                art_hash = make_article_hash(art)
                if is_recently_posted(art_hash):
                    logger.info("Skipped duplicate (cooldown) HIGH article for %s: %s", ticker, art.get("title"))
                    continue
                # Post to discord
                posted_ok = post_to_discord(ticker, get_company_name_for_ticker(ticker), severity, score, art)
                if posted_ok:
                    mark_posted(art_hash)
                    posted_total += 1
            # Small sleep to avoid triggering API provider unusual behaviour on long loops
            # (not per-article heavy, but prevents bursts to downstream services like Discord)
            time.sleep(0.03)

        # Rate limit between pages
        time.sleep(REQUEST_PAUSE)

    return fetched_total, kept_total, posted_total

# --------------------------
# Watchlist & S&P500 utilities
# --------------------------
def load_watchlist(file_path: str) -> List[str]:
    out = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                ln = line.strip()
                if not ln or ln.startswith("#"):
                    continue
                out.append(ln.split()[0].upper())
    except FileNotFoundError:
        logger.warning("Watchlist file not found: %s", file_path)
    return out

def fetch_sp500_master_list() -> List[str]:
    """
    Attempt to fetch S&P500 master tickers and cache them.
    Uses Wikipedia page scraping (basic) as a fallback. If fails, attempts to read cache.
    """
    # try cached first
    cached = safe_read_json(SNP500_CACHE_FILE, None)
    if cached and isinstance(cached, list) and len(cached) > 100:
        return cached
    # fetch wiki
    wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        r = requests.get(wiki_url, timeout=10)
        if r.status_code == 200:
            # Use regex to find table and parse with BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table", {"id": "constituents"})
            if not table:
                table = soup.find("table", {"class": "wikitable sortable"})
            if table:
                tickers = []
                for row in table.find_all("tr")[1:]:
                    cols = row.find_all("td")
                    if cols:
                        t = cols[0].text.strip()
                        if t:
                            t = t.replace(".", "-").upper()
                            tickers.append(t)
                if tickers:
                    safe_write_json(SNP500_CACHE_FILE, tickers)
                    logger.info("Fetched S&P500 master list: %d tickers.", len(tickers))
                    return tickers
    except Exception as e:
        logger.warning("Failed to fetch S&P500 list: %s", e)
    # fallback to cache (maybe None)
    if cached:
        logger.info("Using cached S&P500 master list from file.")
        return cached
    logger.warning("Unable to load S&P500 master list; returning empty list.")
    return []

# --------------------------
# Main loop
# --------------------------
def run_cycle(personal_watch: List[str], sp500_master: List[str]):
    # Compose cycle list: personal (priority) + random picks from sp500 to reach desired cycle size (50)
    personal = [t.upper() for t in personal_watch]
    desired_total = 50
    random_count = max(0, desired_total - len(personal))
    random_picks = []
    if sp500_master:
        random_picks = random.sample(sp500_master, min(random_count, len(sp500_master)))
    cycle_tickers = personal + random_picks
    logger.info("Cycle tickers: %d total (%d personal + %d random).", len(cycle_tickers), len(personal), len(random_picks))
    if random_picks:
        logger.info("Random picks this cycle: %s", random_picks)

    total_fetched = 0
    total_kept = 0
    total_posted = 0

    # Process in batches to avoid bursts
    for i in range(0, len(cycle_tickers), BATCH_SIZE):
        batch = cycle_tickers[i:i+BATCH_SIZE]
        logger.info("Fetching batch: %s", batch)
        # sequential processing within batch (we could parallelize, but that increases rate-limit risk)
        for ticker in batch:
            try:
                logger.info("[{}/{}] Fetching articles for %s | %s", cycle_tickers.index(ticker)+1, len(cycle_tickers), ticker, get_company_name_for_ticker(ticker))
            except Exception:
                logger.info("Fetching articles for %s", ticker)
            fetched, kept, posted = process_ticker(ticker)
            logger.info("%s: %d fetched, %d kept, %d high posted", ticker, fetched, kept, posted)
            total_fetched += fetched
            total_kept += kept
            total_posted += posted
            # gentle pause between tickers
            time.sleep(REQUEST_PAUSE)
        # after each batch, clean posted cache older entries
        cleanup_posted_cache()

    logger.info("Cycle summary: HIGH=%d MED=%d LOW=%d posted=%d", total_posted, max(0, total_kept - total_posted), max(0, total_fetched - total_kept), total_posted)
    return total_fetched, total_kept, total_posted

def main():
    logger.info("========== Bot Startup ==========")
    # load watchlist
    watchlist = load_watchlist(WATCHLIST_FILE)
    logger.info("Loaded %d personal tickers from %s.", len(watchlist), WATCHLIST_FILE)
    # load sp500
    sp500 = fetch_sp500_master_list()
    # If hybrid, ensure company_map saved
    safe_write_json(COMPANY_MAP_FILE, company_map)
    logger.info("Bot started; entering %s loop.", "infinite" if LOOP_MODE == 1 else "single-run")

    cycle_num = 0
    try:
        while True:
            cycle_num += 1
            logger.info("=== Cycle #%d startup ===", cycle_num)
            start = time.time()
            # Compose and run cycle
            fetched, kept, posted = run_cycle(watchlist, sp500)
            elapsed = time.time() - start
            logger.info("Cycle %d completed in %.1fs. Fetched=%d Kept=%d Posted=%d", cycle_num, elapsed, fetched, kept, posted)
            if LOOP_MODE == 0:
                break
            logger.info("Sleeping %ds until next cycle.", LOOP_SLEEP)
            time.sleep(LOOP_SLEEP)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — shutting down.")
    except Exception as e:
        logger.exception("Unhandled exception in main loop: %s", e)
    finally:
        # persist caches
        safe_write_json(COMPANY_MAP_FILE, company_map)
        safe_write_json(DEDUP_DB_FILE, posted_cache)
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    main()
