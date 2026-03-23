# Mauritius News Aggregator

Automated daily feed aggregating local, regional, and global news relevant to Mauritius. Outputs a structured (basic deduplication and clustering) RSS feed (`feed.xml`).

## What it does

- Fetches from ~15 sources: Le Mauricien, L'Express, Defimedia, ION News, BBC, Al Jazeera, France 24, AllAfrica, TechCabal, and others
- Enriches articles with verified publication dates and full descriptions
- Deduplicates using URL canonicalisation, ID hashing, and semantic embedding similarity
- Clusters related stories from different sources into single feed items
- Includes live market data: SEMDEX, MUR/USD, gold, bitcoin, Brent crude
- Includes CEB power outage alerts and Mauritius Met Service weather bulletins

## Output

`feed.xml` — standard RSS with extended fields:
- `<cluster_size>` — number of sources covering this story
- `<all_sources>` — semicolon-separated source list for clustered items  
- `<all_urls>` — all source URLs for clustered items
- `<description>` — structured multi-source summary for clustered items, formatted for LLM ingestion

## Schedule

Runs at 02:00 Mauritius time (22:00 UTC) via GitHub Actions (queue delays expected up to ~4 hours). Can be run manually.

## Stack

Python · feedparser · BeautifulSoup · sentence-transformers (all-MiniLM-L6-v2) · GitHub Actions
