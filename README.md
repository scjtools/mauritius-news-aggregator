# Mauritius News Aggregator

A custom built automated daily feed aggregating local, regional, and global news solely for personal consumption. Outputs a structured RSS feed (`feed.xml`).

- Fetches from ~15 online sources
- Enriches articles with verified publication dates and full descriptions
- Deduplicates using URL canonicalisation, ID hashing, and semantic embedding similarity
- Clusters related stories from different sources into single feed items
- Includes live market data, power outage alerts and weather bulletin

## Schedule

Runs at 02:00 local time (22:00 UTC) via GitHub Actions (queue delays expected up to ~4 hours). Can be run manually.

## Disclaimer, ToS etc.

This project is solely for personal use and is not used for any other purpose.
