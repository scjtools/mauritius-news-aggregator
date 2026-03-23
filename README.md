# MNA v1.1

Automated daily feed aggregating local, regional, and global news. Outputs a structured (basic deduplication and clustering) RSS feed (`feed.xml`).

- Fetches from ~15 online sources
- Enriches articles with verified publication dates and full descriptions
- Deduplicates using URL canonicalisation, ID hashing, and semantic embedding similarity
- Clusters related stories from different sources into single feed items
- Includes live market data, power outage alerts and weather bulletin

## Output

`feed.xml` — standard RSS with extended fields:
- `<cluster_size>` — number of sources covering this story
- `<all_sources>` — semicolon-separated source list for clustered items  
- `<all_urls>` — all source URLs for clustered items
- `<description>` — structured multi-source summary for clustered items, formatted for LLM ingestion

## Schedule

Runs at 02:00 local time (22:00 UTC) via GitHub Actions (queue delays expected up to ~4 hours). Can be run manually.

## Stack

Python · feedparser · BeautifulSoup · sentence-transformers (all-MiniLM-L6-v2) · GitHub Actions
