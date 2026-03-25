# Mauritius News Aggregator

An automated daily feed aggregating local, regional, and global news:
- Fetches from ~15 online sources (including live market data, CEB power outage alerts, MET weather bulletin) plus manual injection
- Includes live market data, CEB power outage alerts and MET weather bulletin
- Performs basic deduplication and clustering
- Scheduled runs at 22:00 MUT (18:00 UTC) and 02:00 MUT (22:00 UTC)
- Optimised for downstream editorial processing by LLM
