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
        titles = get_list(item, "titles")
        urls = get_list(item, "urls")
        languages = get_list(item, "languages")

        is_singleton = cluster_size <= 1

        if is_singleton:
            cluster_id = cluster_id or make_stable_id(link, title, "singleton")

            sources = sources or ([source] if source else [])
            titles = titles or ([title] if title else [])
            urls = urls or ([link] if link else [])

            languages = []
            cluster_size = 1

        else:
            cluster_id = cluster_id or make_stable_id(link, title, "cluster")

            if not sources and source:
                sources = [source]

            if not titles and title:
                titles = [title]

            if not urls and link:
                urls = [link]

        feed_items.append({
            "id": cluster_id,
            "cluster_time": cluster_time,
            "category": category,
            "lead": {
                "title": title,
                "source": source,
                "url": link,
                "summary": description
            },
            "cluster_size": cluster_size,
            "source_count": source_count,
            "sources": sources,
            "titles": titles,
            "urls": urls,
            "languages": languages
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
