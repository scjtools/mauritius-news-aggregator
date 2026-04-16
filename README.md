# Mauritius News Aggregator

A simple tool used to aggregate news (local, regional, and global) and selected data (financial etc.) every 24 hours:
- Fetches from multiple online sources (sources.yaml) plus manual injection (inject.yaml)
- Performs basic deduplication and clustering (aggregator.py, cluster.py)
- Primary output file (feed.xml), converted to json format (build_feed_json.py) for LLM processing (feed.json)

Plugins and tools used: see requirements.txt
