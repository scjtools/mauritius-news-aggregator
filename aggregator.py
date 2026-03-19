import feedparser
import requests
import yaml
import json
import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}
MAX_AGE_HOURS = 24
MAX_SUMMARY_CHARS = 500


def load_sources(path="sources/sources.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def is_recent(dt):
    if dt is None:
        return True  # include if we can't determine age
    cutoff = datetime.now(timezone.utc) - timedelta(hours=MAX_AGE_HOURS)
    return dt >= cutoff


def parse_date(entry):
    for field in ("published_parsed", "updated_parsed"):
        t = getattr(entry, field, None)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    return None


def item_id(title, url):
    return hashlib.md5(f"{title}{url}".encode()).hexdigest()


# ── RSS sources ──────────────────────────────────────────────────────────────

def fetch_rss(source):
    items = []
    try:
        feed = feedparser.parse(source["url"])
        for entry in feed.entries:
            dt = parse_date(entry)
            if not is_recent(dt):
                continue
            summary = BeautifulSoup(
                getattr(entry, "summary", "") or "", "html.parser"
            ).get_text()[:MAX_SUMMARY_CHARS]
            items.append({
                "id":       item_id(entry.get("title", ""), entry.get("link", "")),
                "title":    entry.get("title", "").strip(),
                "url":      entry.get("link", ""),
                "summary":  summary.strip(),
                "source":   source["name"],
                "language": source["language"],
                "category": source["category"],
                "published": dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
            })
    except Exception as e:
        print(f"[RSS ERROR] {source['name']}: {e}")
    return items


# ── Generic homepage scraper ─────────────────────────────────────────────────

def scrape_homepage(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Collect all article links with meaningful text
        seen = set()
        for tag in soup.find_all("a", href=True):
            title = tag.get_text(" ", strip=True)
            url = tag["href"]

            # Basic quality filters
            if len(title) < 20 or len(title) > 200:
                continue
            if not url.startswith("http"):
                base = source["url"].rstrip("/")
                url = base + ("" if url.startswith("/") else "/") + url.lstrip("/")
            if url in seen:
                continue
            seen.add(url)

            # Try to grab first paragraph from article page
            summary = ""
            try:
                ar = requests.get(url, headers=HEADERS, timeout=10)
                ar.raise_for_status()
                asoup = BeautifulSoup(ar.text, "html.parser")
                for p in asoup.find_all("p"):
                    text = p.get_text(strip=True)
                    if len(text) > 80:
                        summary = text[:MAX_SUMMARY_CHARS]
                        break
            except Exception:
                pass

            now = datetime.now(timezone.utc)
            items.append({
                "id":        item_id(title, url),
                "title":     title,
                "url":       url,
                "summary":   summary,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })

            if len(items) >= 20:  # cap per source
                break

    except Exception as e:
        print(f"[SCRAPE ERROR] {source['name']}: {e}")
    return items


# ── Met Service bulletin scraper ─────────────────────────────────────────────

def scrape_bulletin(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Extract visible text blocks, skip navigation/headers
        paragraphs = []
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if len(text) > 40:
                paragraphs.append(text)

        if paragraphs:
            bulletin_text = " ".join(paragraphs[:4])[:MAX_SUMMARY_CHARS]
            now = datetime.now(timezone.utc)
            items.append({
                "id":        item_id("Met bulletin", now.strftime("%Y-%m-%d")),
                "title":     f"Mauritius weather bulletin – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   bulletin_text,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })
    except Exception as e:
        print(f"[BULLETIN ERROR] {source['name']}: {e}")
    return items


# ── Deduplicate ───────────────────────────────────────────────────────────────

def deduplicate(items):
    seen = {}
    for item in items:
        if item["id"] not in seen:
            seen[item["id"]] = item
    return list(seen.values())


# ── Build RSS output ──────────────────────────────────────────────────────────

def build_rss(items):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = "Mauritius News Aggregator"
    SubElement(channel, "link").text = "https://github.com"
    SubElement(channel, "description").text = "Aggregated Mauritius news – last 24 hours"
    SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    for item in sorted(items, key=lambda x: x["published"], reverse=True):
        entry = SubElement(channel, "item")
        SubElement(entry, "title").text = item["title"]
        SubElement(entry, "link").text = item["url"]
        SubElement(entry, "description").text = item["summary"]
        SubElement(entry, "pubDate").text = item["published"]
        SubElement(entry, "guid").text = item["id"]
        SubElement(entry, "source").text = item["source"]
        SubElement(entry, "category").text = item["category"]
        # Custom fields for the LLM filter stage
        SubElement(entry, "language").text = item["language"]

    raw = tostring(rss, encoding="unicode")
    return minidom.parseString(raw).toprettyxml(indent="  ")


# ── Exchange rates ────────────────────────────────────────────────────────────

def fetch_exchange_rates(source):
    items = []
    try:
        r = requests.get(source["url"], headers=HEADERS, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        target_currencies = source.get("currencies", [])
        rates = {}
        for item in root.findall("item"):
            title = item.findtext("title", "")
            for currency in target_currencies:
                if title.endswith(currency):
                    parts = title.split("=")
                    if len(parts) == 2:
                        rates[currency] = parts[1].strip()
        if rates:
            now = datetime.now(timezone.utc)
            summary = " | ".join(f"1 MUR = {rates[c]} {c}" for c in target_currencies if c in rates)
            items.append({
                "id":        item_id("MUR rates", now.strftime("%Y-%m-%d")),
                "title":     f"MUR exchange rates – {now.strftime('%d %B %Y')}",
                "url":       source["url"],
                "summary":   summary,
                "source":    source["name"],
                "language":  source["language"],
                "category":  source["category"],
                "published": now.isoformat(),
            })
    except Exception as e:
        print(f"[RATES ERROR] {source['name']}: {e}")
    return items


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sources = load_sources()
    all_items = []

    print("Fetching RSS feeds...")
    for source in sources.get("rss_feeds", []):
        items = fetch_rss(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Scraping homepages...")
    for source in sources.get("scrapers", []):
        if source.get("type") == "bulletin":
            items = scrape_bulletin(source)
        else:
            items = scrape_homepage(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)

    print("Fetching exchange rates...")
    for source in sources.get("exchange_rates", []):
        items = fetch_exchange_rates(source)
        print(f"  {source['name']}: {len(items)} items")
        all_items.extend(items)
    
    all_items = deduplicate(all_items)
    print(f"\nTotal unique items: {len(all_items)}")

    rss_output = build_rss(all_items)
    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_output)
    print("Written to feed.xml")


if __name__ == "__main__":
    main()
