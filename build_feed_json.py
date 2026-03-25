import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path


INPUT_FILE = Path("feed.xml")
OUTPUT_FILE = Path("feed.json")


def get_text(elem, tag):
    child = elem.find(tag)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def get_list(elem, tag):
    child = elem.find(tag)
    if child is None or child.text is None:
        return []

    values = []
    for line in child.text.splitlines():
        value = line.strip()
        if value:
            values.append(value)

    return values


def make_stable_id(link, title, prefix):
    base = (link or title or "").strip().lower()
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def parse_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def dedupe_preserve_order(values):
    seen = set()
    result = []

    for value in values:
        if not value:
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)

    return result


def ensure_lead_url_first(urls, lead_url):
    if not lead_url:
        return dedupe_preserve_order(urls)

    ordered = [lead_url] + (urls or [])
    return dedupe_preserve_order(ordered)


def ensure_lead_source_first(sources, lead_source):
    if not lead_source:
        return dedupe_preserve_order(sources)

    ordered = [lead_source] + (sources or [])
    return dedupe_preserve_order(ordered)


def parse_feed(feed_path):
    tree = ET.parse(feed_path)
    root = tree.getroot()

    items = root.findall(".//item")

    feed_items = []

    for item in items:
        title = get_text(item, "title")
        link = get_text(item, "link")
        description = get_text(item, "description")
        source = get_text(item, "source")
        category = get_text(item, "category")
        cluster_time = get_text(item, "cluster_time")

        cluster_size = parse_int(get_text(item, "cluster_size"), 1)
        source_count = parse_int(get_text(item, "source_count"), 1)

        cluster_id = get_text(item, "cluster_id")

        sources = get_list(item, "sources")
        urls = get_list(item, "urls")

        is_singleton = cluster_size <= 1

        if is_singleton:
            cluster_id = cluster_id or make_stable_id(link, title, "singleton")
            cluster_size = 1
            source_count = 1 if source else max(source_count, 1)
        else:
            cluster_id = cluster_id or make_stable_id(link, title, "cluster")

        sources = ensure_lead_source_first(sources, source)
        urls = ensure_lead_url_first(urls, link)

        if not source_count:
            source_count = len(sources) if sources else 1

        feed_items.append({
            "id": cluster_id,
            "cluster_time": cluster_time,
            "category": category,
            "headline": title,
            "summary": description,
            "cluster_size": cluster_size,
            "source_count": source_count,
            "sources": sources,
            "urls": urls
        })

    return {
        "generated_from": "feed.xml",
        "item_count": len(feed_items),
        "items": feed_items
    }


def main():
    if not INPUT_FILE.exists():
        raise FileNotFoundError("feed.xml not found")

    data = parse_feed(INPUT_FILE)

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Wrote feed.json with {data['item_count']} items")


if __name__ == "__main__":
    main()
