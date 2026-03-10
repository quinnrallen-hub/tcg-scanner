#!/usr/bin/env python3
"""
GitHub Actions scanner — runs one rotated query per invocation.
Appends new deals to data/listings.json, deduplicates by URL, prunes old entries.
"""
import re, json, time, statistics, requests, sys, os, traceback
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus

DB_FILE       = "data/listings.json"
MAX_AGE_DAYS  = 7       # Drop listings older than this
MAX_DB_BYTES  = 1 * 1024 * 1024 * 1024  # 1 GB hard cap
DEAL_THRESH   = 0.85    # Flag as deal if price < 85% of median sold
SIM_THRESH    = 0.50    # Jaccard similarity to count as "same card"
MIN_COMPS     = 5       # Min sold comps required
COMP_DAYS     = 90     # Only count sold listings within this many days

SCAN_QUERIES = [
    "pokemon PSA 10 holo rare",
    "pokemon PSA 9 holo rare",
    "pokemon BGS 9.5",
    "pokemon CGC 10",
    "pokemon PSA 10 vintage base",
    "pokemon PSA 9 vintage base",
    "pokemon PSA 10 ex gx vmax vstar",
]

GRADE_KEYWORDS = [
    "PSA 10","PSA 9.5","PSA 9","PSA 8","PSA 7",
    "BGS 10","BGS 9.5","BGS 9","BGS 8.5",
    "CGC 10","CGC 9.5","CGC 9","SGC 10","SGC 9.5","SGC 9",
]
GRADE_COMPANIES = ["PSA","BGS","CGC","SGC"]
STOP_WORDS = {
    "pokemon","card","graded","holo","rare","ultra","full","art",
    "psa","bgs","cgc","sgc","gem","mint","near","nm","lp",
    "the","a","an","and","or","of","in","on","set","lot",
    "vintage","japanese","english","1st","edition","foil",
    "secret","alternate","rainbow","trainer","gallery","promo",
    "special","collection","classic",
}

HEADERS = {
    "User-Agent":   "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer":      "https://130point.com/sales/",
    "Content-Type": "application/x-www-form-urlencoded",
}

TOR_PROXIES = {
    "http":  "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050",
}

USE_TOR = os.environ.get("USE_TOR", "0") == "1"

# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg, level="INFO"):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {level:5s} {msg}", flush=True)

def dbg(msg):  log(msg, "DEBUG")
def info(msg): log(msg, "INFO")
def warn(msg): log(msg, "WARN")
def err(msg):  log(msg, "ERROR")

# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch(query, sold):
    kind = "sold_items" if sold else "for_sale"
    payload = {
        "query": query, "type": kind,
        "subcat": "", "tab_id": "1", "tz": "America/New_York", "sort": "best_match",
    }
    url = "https://back.130point.com/sales/"
    proxies = TOR_PROXIES if USE_TOR else None
    info(f"POST {url}  type={kind}  via={'Tor' if USE_TOR else 'direct'}  query={query!r}")
    t0 = time.time()
    try:
        r = requests.post(url, data=payload, headers=HEADERS, proxies=proxies, timeout=30)
    except requests.RequestException as e:
        err(f"Request failed: {e}")
        raise

    elapsed = time.time() - t0
    info(f"Response: HTTP {r.status_code}  {len(r.text)} bytes  {elapsed:.2f}s")

    if r.status_code == 429:
        retry = r.headers.get("Retry-After", "?")
        warn(f"Rate limited — Retry-After: {retry}s — exiting cleanly")
        sys.exit(0)

    if not r.ok:
        err(f"Unexpected status {r.status_code}: {r.text[:300]}")
        r.raise_for_status()

    # Spot-check: does response look like real data?
    has_data = "data-price" in r.text
    dbg(f"Contains 'data-price': {has_data}")
    if not has_data:
        warn(f"Response has no item data. First 500 chars: {r.text[:500]!r}")

    return r.text

# ── Parse ─────────────────────────────────────────────────────────────────────

def parse(html, label=""):
    items = []
    rows_found = 0
    skipped_currency = 0
    skipped_no_title = 0

    for m in re.finditer(
        r'<tr[^>]+data-price="([\d.]+)"[^>]+data-currency="([^"]+)"[^>]*>(.*?)</tr>',
        html, re.DOTALL
    ):
        rows_found += 1
        price, currency, cell = float(m.group(1)), m.group(2), m.group(3)

        if currency != "USD":
            skipped_currency += 1
            dbg(f"  skip non-USD row: {currency} ${price}")
            continue

        tm = re.search(
            r'id=[\'"]titleText[\'"][^>]*>.*?<a href=[\'"]([^\'"]+)[\'"][^>]*>([^<]+)</a>',
            cell, re.DOTALL
        )
        if not tm:
            skipped_no_title += 1
            dbg(f"  skip row — titleText anchor not found (price=${price})")
            continue

        url, title = tm.group(1), tm.group(2).strip()
        ship = 0.0
        sm = re.search(r'Shipping Price:</b>\s*\$([\d,.]+)', cell)
        if sm:
            ship = float(sm.group(1).replace(",", ""))
            dbg(f"  shipping ${ship:.2f} added for: {title[:50]!r}")

        g = grade(title)
        total = round(price + ship, 2)

        # Parse sale date — format: "Mon 15 Dec 2025 07:59:40 EST"
        sold_at = None
        dm = re.search(r'<b>Date:</b>\s*([^<]+)', cell)
        if dm:
            try:
                # Reformat to something parsedate_to_datetime can handle
                raw = dm.group(1).strip()
                # Strip day-of-week prefix if present: "Mon 15 Dec 2025 07:59:40 EST"
                parts = raw.split()
                if len(parts) == 6:          # has weekday prefix
                    raw = " ".join(parts[1:])  # "15 Dec 2025 07:59:40 EST"
                sold_at = datetime.strptime(raw, "%d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
            except Exception as e:
                dbg(f"  could not parse date {dm.group(1)!r}: {e}")

        dbg(f"  parsed [{label}]: grade={g}  price=${total}  date={sold_at}  title={title[:50]!r}")
        items.append({"title": title, "price": total, "url": url, "grade": g, "sold_at": sold_at})

    info(f"parse({label}): {rows_found} rows found → {len(items)} kept "
         f"(skipped: {skipped_currency} non-USD, {skipped_no_title} no-title)")
    return items

# ── Helpers ───────────────────────────────────────────────────────────────────

def grade(title):
    u = title.upper()
    for g in GRADE_KEYWORDS:
        if g.upper() in u:
            return g
    for c in GRADE_COMPANIES:
        if c in u:
            return c
    return "Unknown"


def tokens(title):
    words = re.findall(r'\b[a-z0-9/]+\b', title.lower())
    return frozenset(w for w in words if w not in STOP_WORDS and len(w) > 1)


def jaccard(a, b):
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


LOT_RE = re.compile(
    r'\b(lot|bundle|set|collection|pack)\b'
    r'|\(\s*\d+\s*\)'       # (10), (5), etc.
    r'|\bx\s*\d+\b'         # x5, x 10
    r'|\b\d+\s*cards?\b',   # 10 cards, 5 card
    re.IGNORECASE
)

def is_lot(title):
    return bool(LOT_RE.search(title))


def iqr_filter(prices):
    """Remove outliers beyond 1.5×IQR; return cleaned list (or original if too small)."""
    if len(prices) < 4:
        return prices
    s = sorted(prices)
    q1 = statistics.median(s[:len(s)//2])
    q3 = statistics.median(s[(len(s)+1)//2:])
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    filtered = [p for p in prices if lo <= p <= hi]
    return filtered if len(filtered) >= MIN_COMPS else prices


def ebay_search(title, g):
    clean = re.sub(r'\b(PSA|BGS|CGC|SGC)\b.*', '', title, flags=re.IGNORECASE).strip()
    q = quote_plus(f"{clean} {g} pokemon")
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_BIN=1&LH_ItemCondition=3000&_sop=15"

# ── DB ────────────────────────────────────────────────────────────────────────

def load_db():
    if not os.path.exists(DB_FILE):
        warn(f"{DB_FILE} not found — starting fresh")
        return {"listings": [], "last_scan": None, "scan_count": 0}
    try:
        size = os.path.getsize(DB_FILE)
        info(f"Loading {DB_FILE} ({size/1024:.1f} KB)")
        with open(DB_FILE) as f:
            db = json.load(f)
        info(f"Loaded {len(db.get('listings',[]))} existing listings")
        return db
    except Exception as e:
        err(f"Failed to load DB: {e} — starting fresh")
        return {"listings": [], "last_scan": None, "scan_count": 0}


def save_db(db):
    size_before = os.path.getsize(DB_FILE) if os.path.exists(DB_FILE) else 0
    with open(DB_FILE, "w") as f:
        json.dump(db, f, separators=(",", ":"))
    size_after = os.path.getsize(DB_FILE)
    info(f"Saved {DB_FILE}: {size_before/1024:.1f} KB → {size_after/1024:.1f} KB  "
         f"({len(db['listings'])} listings)")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    info("=" * 60)
    info(f"TCG Scanner — Python {sys.version.split()[0]}  pid={os.getpid()}  tor={'yes' if USE_TOR else 'no'}")

    query_idx = (int(time.time()) // 600) % len(SCAN_QUERIES)
    query     = SCAN_QUERIES[query_idx]
    info(f"Query rotation: [{query_idx}/{len(SCAN_QUERIES)-1}] {query!r}")

    # Fetch
    info("--- Fetching sold listings ---")
    sold_html = fetch(query, sold=True)
    info("Sleeping 2s between requests...")
    time.sleep(2)
    info("--- Fetching active listings ---")
    active_html = fetch(query, sold=False)

    # Sanity-check: 130point sometimes returns identical HTML for both types (Tor quirk)
    if active_html == sold_html:
        warn("sold_html == active_html — server returned same response for both types; aborting run")
        sys.exit(0)

    # Parse
    active = parse(active_html, "active")
    sold   = parse(sold_html,   "sold")

    if not active:
        warn("No active listings parsed — nothing to add this run")
    if not sold:
        warn("No sold listings parsed — cannot compute market comps")

    # Filter out lots/bundles from both sets
    before_a, before_s = len(active), len(sold)
    active = [i for i in active if not is_lot(i["title"])]
    sold   = [i for i in sold   if not is_lot(i["title"])]
    info(f"Lot filter: active {before_a}→{len(active)}  sold {before_s}→{len(sold)}")

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MAX_AGE_DAYS)

    # Filter sold items to last COMP_DAYS days
    comp_cutoff = now - timedelta(days=COMP_DAYS)
    sold_recent = []
    skipped_old = 0
    for s in sold:
        if s["sold_at"] is None:
            sold_recent.append(s)   # no date = keep (can't filter what we can't read)
            dbg(f"  sold date unknown, keeping: {s['title'][:50]!r}")
        elif s["sold_at"] >= comp_cutoff:
            sold_recent.append(s)
        else:
            skipped_old += 1
            dbg(f"  sold {s['sold_at'].date()} — older than {COMP_DAYS}d, excluding from comps")
    info(f"Sold comps: {len(sold)} total → {len(sold_recent)} within {COMP_DAYS} days "
         f"({skipped_old} too old)")

    # Token-index recent sold items for similarity matching
    sold_tok = [(s, tokens(s["title"])) for s in sold_recent]
    dbg(f"Token-indexed {len(sold_tok)} recent sold items")

    db = load_db()

    # Prune
    before = len(db["listings"])
    db["listings"] = [
        l for l in db["listings"]
        if datetime.fromisoformat(l["found_at"]) > cutoff
    ]
    pruned = before - len(db["listings"])
    info(f"Pruned {pruned} listings older than {MAX_AGE_DAYS} days")

    existing_urls = {l["url"] for l in db["listings"]}
    info(f"Existing URLs in DB: {len(existing_urls)}")

    new_count   = 0
    skip_dup    = 0
    skip_comps  = 0

    for item in active:
        if item["url"] in existing_urls:
            skip_dup += 1
            dbg(f"  dup skip: {item['title'][:50]!r}")
            continue

        a_tok = tokens(item["title"])
        comps = []
        for s, st in sold_tok:
            if s["grade"] != item["grade"]:
                continue
            sim = jaccard(a_tok, st)
            if sim >= SIM_THRESH:
                comps.append(s["price"])
                date_str = s["sold_at"].date() if s["sold_at"] else "?"
                dbg(f"  comp match sim={sim:.2f}  ${s['price']}  {date_str}  {s['title'][:40]!r}")

        if len(comps) < MIN_COMPS:
            skip_comps += 1
            dbg(f"  skip (only {len(comps)} comps < {MIN_COMPS}): {item['title'][:50]!r}")
            continue

        comps = iqr_filter(comps)
        med   = statistics.median(comps)
        ratio = item["price"] / med
        deal  = ratio < DEAL_THRESH

        info(f"  {'DEAL' if deal else 'item'}: ${item['price']:.2f} vs med ${med:.2f} "
             f"({ratio*100:.0f}%)  {item['grade']}  {item['title'][:45]!r}")

        entry = {
            "url":      item["url"],
            "title":    item["title"],
            "grade":    item["grade"],
            "price":    item["price"],
            "med_sold": round(med, 2),
            "ratio":    round(ratio, 4),
            "savings":  round(med - item["price"], 2),
            "deal":     deal,
            "n_comps":  len(comps),
            "live_url": ebay_search(item["title"], item["grade"]),
            "found_at": now.isoformat(),
            "query":    query,
        }
        db["listings"].append(entry)
        existing_urls.add(item["url"])
        new_count += 1

    info(f"New: {new_count}  Skipped duplicates: {skip_dup}  Skipped low-comps: {skip_comps}")

    # Cap by file size
    db["listings"] = sorted(db["listings"], key=lambda l: l["found_at"], reverse=True)
    raw_size = len(json.dumps(db["listings"], separators=(",", ":")).encode())
    info(f"Pre-cap size: {raw_size/1024/1024:.2f} MB  ({len(db['listings'])} listings)")
    dropped = 0
    while True:
        size = len(json.dumps(db["listings"], separators=(",", ":")).encode())
        if size <= MAX_DB_BYTES:
            break
        db["listings"].pop()
        dropped += 1
    if dropped:
        info(f"Dropped {dropped} listings to stay under size cap")

    db["last_scan"]  = now.isoformat()
    db["scan_count"] = db.get("scan_count", 0) + 1

    save_db(db)

    deals = sum(1 for l in db["listings"] if l["deal"])
    info(f"Done — total: {len(db['listings'])}  deals: {deals}  scan #{db['scan_count']}")
    info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        err("Unhandled exception:")
        traceback.print_exc()
        sys.exit(1)
