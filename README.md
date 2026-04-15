# Mauritius News Aggregator

A simple tool used to aggregate news (local, regional, and global) and selected data (financial etc.) for use by the 4minit downstream pipeline:
- Fetches from multiple online sources (aged within 24 hours) plus manual injection (inject.yaml)
- Performs basic deduplication and clustering
- Output files: feed.xml and feed.json
