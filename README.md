# Mauritius News Aggregator

A custom built automated daily feed aggregating local, regional, and global news solely for personal consumption. Outputs a structured RSS feed (`feed.xml`).

- Fetches headlines and descriptions (up to 800 characters) from ~15 online sources, automatically categorised (local, regional or global)
- Allows manual injection of articles (headline/title, description, category)
- Deduplicates and clusters (count included) related stories from different sources into single feed items
- Includes live market data (MURUSD, MUREUR, Gold, Bitcoin), CEB power outage alerts and MET weather bulletin

## Schedule

Runs at 02:00 local time (22:00 UTC) via GitHub Actions (queue delays expected up to ~4 hours). Can be run manually.

## Disclaimer, ToS etc.

This project is solely for personal use and is not used for any other purpose.
