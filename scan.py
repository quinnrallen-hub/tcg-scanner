#!/usr/bin/env python3
"""
GitHub Actions scanner — runs one rotated query per invocation.
Appends new deals to data/listings.json, deduplicates by URL, prunes old entries.
"""
import re, json, time, statistics, requests, sys
from datetime import datetime, timezone, timedelta
from urllib.parse import quote_plus

DB_FILE       = "data/listings.json"
MAX_AGE_DAYS  = 3       # Drop listings older than this
MAX_LISTINGS  = 800     # Cap total stored listings
DEAL_THRESH   = 0.85    # Flag as deal if price < 85% of median sold
SIM_THRESH    = 0.32    # Jaccard similarity to count as "same card"
MIN_COMPS     = 2       # Min sold comps required

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


def fetch(query, sold):
    payload = {
        "query": query, "type": "sold_items" if sold else "for_sale",
        "subcat": "", "tab_id": "1", "tz": "America/New_York", "sort": "best_match",
    }
    r = requests.post("https://back.130point.com/sales/", data=payload,
                      headers=HEADERS, timeout=20)
    if r.status_code == 429:
        print("Rate limited — skipping this run")
        sys.exit(0)
    r.raise_for_status()
    return r.text


def parse(html):
    items = []
    for m in re.finditer(r'<tr[^>]+data-price="([\d.]+)"[^>]+data-currency="([^"]+)"[^>]*>(.*?)</tr>', html, re.DOTALL):
        price, currency, cell = float(m.group(1)), m.group(2), m.group(3)
        if currency != "USD":
            continue
        tm = re.search(r'id=[\'"]titleText[\'"][^>]*>.*?<a href=[\'"]([^\'"]+)[\'"][^>]*>([^<]+)</a>', cell, re.DOTALL)
        if not tm:
            continue
        url, title = tm.group(1), tm.group(2).strip()
        ship = 0.0
        sm = re.search(r'Shipping Price:</b>\s*\$([\d,.]+)', cell)
        if sm:
            ship = float(sm.group(1).replace(",", ""))
        items.append({"title": title, "price": round(price + ship, 2),
                      "url": url, "grade": grade(title)})
    return items


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


def ebay_search(title, g):
    clean = re.sub(r'\b(PSA|BGS|CGC|SGC)\b.*', '', title, flags=re.IGNORECASE).strip()
    q = quote_plus(f"{clean} {g} pokemon")
    return f"https://www.ebay.com/sch/i.html?_nkw={q}&LH_BIN=1&LH_ItemCondition=3000&_sop=15"


def load_db():
    try:
        with open(DB_FILE) as f:
            return json.load(f)
    except Exception:
        return {"listings": [], "last_scan": None, "scan_count": 0}


def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, separators=(",", ":"))


def main():
    # Rotate query based on current 10-minute bucket
    query_idx = (int(time.time()) // 600) % len(SCAN_QUERIES)
    query = SCAN_QUERIES[query_idx]
    print(f"Query [{query_idx}]: {query}")

    print("Fetching sold listings...")
    sold_html = fetch(query, sold=True)
    time.sleep(2)
    print("Fetching active listings...")
    active_html = fetch(query, sold=False)

    active = parse(active_html)
    sold   = parse(sold_html)
    print(f"Active: {len(active)}  Sold: {len(sold)}")

    # Build sold comps with tokens
    sold_tok = [(s, tokens(s["title"])) for s in sold]

    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=MAX_AGE_DAYS)

    db = load_db()

    # Prune old listings
    before = len(db["listings"])
    db["listings"] = [
        l for l in db["listings"]
        if datetime.fromisoformat(l["found_at"]) > cutoff
    ]
    print(f"Pruned {before - len(db['listings'])} old listings")

    existing_urls = {l["url"] for l in db["listings"]}
    new_count = 0

    for item in active:
        if item["url"] in existing_urls:
            continue
        a_tok = tokens(item["title"])
        comps = [s["price"] for s, st in sold_tok
                 if s["grade"] == item["grade"] and jaccard(a_tok, st) >= SIM_THRESH]
        if len(comps) < MIN_COMPS:
            continue
        med   = statistics.median(comps)
        ratio = item["price"] / med
        entry = {
            "url":       item["url"],
            "title":     item["title"],
            "grade":     item["grade"],
            "price":     item["price"],
            "med_sold":  round(med, 2),
            "ratio":     round(ratio, 4),
            "savings":   round(med - item["price"], 2),
            "deal":      ratio < DEAL_THRESH,
            "n_comps":   len(comps),
            "live_url":  ebay_search(item["title"], item["grade"]),
            "found_at":  now.isoformat(),
            "query":     query,
        }
        db["listings"].append(entry)
        existing_urls.add(item["url"])
        new_count += 1

    # Cap total size
    db["listings"] = sorted(db["listings"], key=lambda l: l["found_at"], reverse=True)[:MAX_LISTINGS]

    db["last_scan"]  = now.isoformat()
    db["scan_count"] = db.get("scan_count", 0) + 1

    save_db(db)

    deals = sum(1 for l in db["listings"] if l["deal"])
    print(f"New: {new_count}  Total: {len(db['listings'])}  Deals: {deals}")


if __name__ == "__main__":
    main()
