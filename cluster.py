"""
cluster.py — Deduplication and semantic clustering for the Mauritius news aggregator.

WHAT THIS MODULE DOES
─────────────────────
1. DEDUPLICATION  — removes items that are the same article:
   • Pass 1: canonical URL match (strips UTM params, /amp suffixes)
   • Pass 2: ID hash match (aggregator re-ingestion)
   • Pass 3: semantic cosine similarity on sentence embeddings (threshold 0.92)
             catches same article with reworded headline across sources

2. CLUSTERING — groups items that cover the same news event:
   • Uses sentence embeddings + cosine similarity (threshold 0.72)
   • Language-aware: fr and en items cluster separately (different reader segments)
   • Each cluster is COLLAPSED into a single rich feed item containing all
     source URLs, titles, and descriptions — cleaner input for downstream LLM

3. EMBEDDING MODEL
   • all-MiniLM-L6-v2 (80MB, MIT licence)
   • Cached at ~/.cache/huggingface between runs via GitHub Actions cache key
   • Falls back to hybrid title similarity if sentence-transformers unavailable

USAGE
─────
    from cluster import deduplicate_items, cluster_and_collapse, deduplicate_bulletin_text

    items = deduplicate_items(items)
    items = cluster_and_collapse(items)   # returns collapsed list; fewer items
"""

from __future__ import annotations

import re
import unicodedata
import logging
from collections import defaultdict
from typing import Any

log = logging.getLogger(__name__)

# ── Model configuration ──────────────────────────────────────────────────────

_MODEL_NAME = "all-MiniLM-L6-v2"   # 80 MB, fast, good multilingual coverage
_DEDUP_THRESHOLD   = 0.92           # cosine sim above this → same article
_CLUSTER_THRESHOLD = 0.72           # cosine sim above this → same event

# ── Lazy model loader ────────────────────────────────────────────────────────

_model = None
_USE_EMBEDDINGS = None  # None = not yet tried; True/False after first attempt


def _get_model():
    """Load the SentenceTransformer model once; cache in-process."""
    global _model, _USE_EMBEDDINGS
    if _USE_EMBEDDINGS is False:
        return None
    if _model is not None:
        return _model
    try:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(_MODEL_NAME)
        _USE_EMBEDDINGS = True
        log.info(f"[cluster] Loaded embedding model: {_MODEL_NAME}")
        return _model
    except Exception as e:
        log.warning(f"[cluster] sentence-transformers unavailable ({e}), using title similarity fallback")
        _USE_EMBEDDINGS = False
        return None


def _embed(texts: list[str]) -> "list | None":
    """Encode texts to normalised embeddings. Returns None on failure."""
    model = _get_model()
    if model is None:
        return None
    try:
        vecs = model.encode(
            texts,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,   # L2-normalise so dot product == cosine sim
        )
        return vecs  # numpy array; indexable by [i]
    except Exception as e:
        log.warning(f"[cluster] Embedding encode failed: {e}")
        return None


def _cosine(a, b) -> float:
    """Dot product of two pre-normalised vectors (works for both list and numpy)."""
    try:
        return float((a * b).sum())          # numpy fast path
    except Exception:
        return sum(x * y for x, y in zip(a, b))   # pure-python fallback


# ── Text utilities ───────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace, strip punctuation."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _canonical_url(url: str) -> str:
    """Strip query strings, fragments, /amp for URL-based dedup."""
    url = re.sub(r"[?#].*$", "", url)
    url = re.sub(r"/amp/?$", "", url)
    return url.rstrip("/").lower()


def _ngrams(tokens: list[str], n: int) -> set[str]:
    return {" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)}


def _title_similarity(a: str, b: str) -> float:
    """
    Hybrid fallback similarity (bigram Jaccard + unigram containment).
    Active only when sentence-transformers is unavailable.
    """
    ta, tb = _normalise(a).split(), _normalise(b).split()
    if not ta or not tb:
        return 0.0
    ng_a = _ngrams(ta, 2) or set(ta)
    ng_b = _ngrams(tb, 2) or set(tb)
    bigram_j = len(ng_a & ng_b) / len(ng_a | ng_b) if (ng_a | ng_b) else 0.0
    shorter = set(ta) if len(ta) <= len(tb) else set(tb)
    longer  = set(tb) if len(ta) <= len(tb) else set(ta)
    containment = len(shorter & longer) / len(shorter) if shorter else 0.0
    return 0.4 * bigram_j + 0.6 * containment


# ── Source priority ──────────────────────────────────────────────────────────

_SOURCE_PRIORITY: dict[str, int] = {
    "Le Mauricien": 10,
    "L'Express (via mega.mu)": 9,
    "Defimedia": 9,
    "ION News": 8,
    "News Moris": 7,
    "BBC News": 6,
    "BBC Africa": 6,
    "Al Jazeera": 5,
    "France 24": 5,
    "AllAfrica": 4,
    "Africanews": 4,
    "Business Insider Africa": 4,
    "TechCabal": 4,
}


def _priority(item: dict) -> int:
    return _SOURCE_PRIORITY.get(item.get("source", ""), 3)


def _best_item(candidates: list[dict]) -> dict:
    """Pick the highest-quality item: source priority, then verified date, then summary length."""
    return max(candidates, key=lambda i: (
        _priority(i),
        int(i.get("date_verified", False)),
        len(i.get("summary", "")),
    ))


# ── Union-Find helper ────────────────────────────────────────────────────────

def _make_uf(n: int):
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(x, y, items):
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if _priority(items[rx]) >= _priority(items[ry]):
            parent[ry] = rx
        else:
            parent[rx] = ry
    return parent, find, union


def _candidate_pairs(items: list[dict], min_word_len: int = 5) -> set[tuple[int,int]]:
    """Bucket items by shared significant words to generate candidate pairs."""
    buckets: dict[str, list[int]] = defaultdict(list)
    for idx, item in enumerate(items):
        for word in _normalise(item.get("title", "")).split():
            if len(word) >= min_word_len:
                buckets[word].append(idx)
    pairs: set[tuple[int,int]] = set()
    for indices in buckets.values():
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                a, b = indices[i], indices[j]
                pairs.add((min(a,b), max(a,b)))
    return pairs


# ═══════════════════════════════════════════════════════════════════════════
# 1. DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════

def deduplicate_items(items: list[dict]) -> list[dict]:
    """
    Three-pass deduplication. Surviving items are unchanged.

    Pass 1 — canonical URL
    Pass 2 — ID hash
    Pass 3 — semantic similarity (same article, reworded headline)
    """
    # Pass 1
    url_groups: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        url_groups[_canonical_url(item.get("url", ""))].append(item)
    deduped = [_best_item(g) for g in url_groups.values()]

    # Pass 2
    seen: dict[str, dict] = {}
    for item in deduped:
        iid = item.get("id", "")
        if iid not in seen:
            seen[iid] = item
        else:
            seen[iid] = _best_item([seen[iid], item])
    deduped = list(seen.values())

    # Pass 3
    before = len(deduped)
    deduped = _semantic_dedup(deduped)
    removed = before - len(deduped)
    if removed:
        log.info(f"[cluster] Semantic dedup removed {removed} near-identical items")

    return deduped


def _semantic_dedup(items: list[dict]) -> list[dict]:
    """Remove near-identical items using embedding cosine similarity."""
    if len(items) < 2:
        return items

    titles = [item.get("title", "") for item in items]
    embeddings = _embed(titles)

    if embeddings is not None:
        threshold = _DEDUP_THRESHOLD
        def sim(i, j):
            return _cosine(embeddings[i], embeddings[j])
    else:
        threshold = 0.72
        def sim(i, j):
            return _title_similarity(items[i].get("title",""), items[j].get("title",""))

    n = len(items)
    parent, find, union = _make_uf(n)

    for a, b in _candidate_pairs(items, min_word_len=5):
        if find(a) == find(b):
            continue
        if items[a].get("language") != items[b].get("language"):
            continue
        if sim(a, b) >= threshold:
            union(a, b, items)

    groups: dict[int, list[dict]] = defaultdict(list)
    for idx in range(n):
        groups[find(idx)].append(items[idx])

    return [_best_item(g) for g in groups.values()]


# ═══════════════════════════════════════════════════════════════════════════
# 2. CLUSTERING + COLLAPSE
# ═══════════════════════════════════════════════════════════════════════════

# Categories that should never be clustered together
_NO_CLUSTER_CATEGORIES = {"finance", "weather", "utilities"}


def cluster_and_collapse(items: list[dict]) -> list[dict]:
    """
    Groups items covering the same news event and collapses each cluster into
    a single feed item containing all sources, titles, and descriptions.

    Finance / weather / utilities items pass through unclustered (they are
    point-in-time data, not narratives).

    Cluster isolation:
    • French and English items cluster independently
    • Category compatibility: local+regional ok; global+global ok;
      weather/finance/utilities never cluster

    The collapsed item summary uses this format (for LLM readability):

        [SOURCE A] Title from source A
        Description from source A.
        URL: https://...

        [SOURCE B] Title from source B
        Description from source B.
        URL: https://...
    """
    passthrough = [i for i in items if i.get("category","") in _NO_CLUSTER_CATEGORIES]
    clusterable = [i for i in items if i.get("category","") not in _NO_CLUSTER_CATEGORIES]

    for item in passthrough:
        item.setdefault("cluster_size", 1)

    if not clusterable:
        return sorted(passthrough, key=lambda i: i.get("published",""), reverse=True)

    titles = [i.get("title","") for i in clusterable]
    embeddings = _embed(titles)

    if embeddings is not None:
        threshold = _CLUSTER_THRESHOLD
        def sim(i, j):
            return _cosine(embeddings[i], embeddings[j])
    else:
        threshold = 0.40
        def sim(i, j):
            return _title_similarity(clusterable[i].get("title",""), clusterable[j].get("title",""))

    n = len(clusterable)
    parent, find, union = _make_uf(n)

    for a, b in _candidate_pairs(clusterable, min_word_len=4):
        if find(a) == find(b):
            continue
        ia, ib = clusterable[a], clusterable[b]
        if ia.get("language") != ib.get("language"):
            continue
        if not _categories_compatible(ia.get("category",""), ib.get("category","")):
            continue
        if sim(a, b) >= threshold:
            union(a, b, clusterable)

    groups: dict[int, list[dict]] = defaultdict(list)
    for idx in range(n):
        groups[find(idx)].append(clusterable[idx])

    collapsed = []
    multi = 0
    for group_items in groups.values():
        group_sorted = sorted(group_items, key=lambda i: (
            _priority(i),
            int(i.get("date_verified", False)),
            len(i.get("summary","")),
        ), reverse=True)
        if len(group_sorted) == 1:
            group_sorted[0]["cluster_size"] = 1
            collapsed.append(group_sorted[0])
        else:
            multi += 1
            collapsed.append(_collapse_group(group_sorted))

    log.info(f"[cluster] {multi} multi-source clusters formed from {len(clusterable)} items "
             f"→ {len(collapsed)} collapsed items")

    all_items = collapsed + passthrough
    all_items.sort(key=lambda i: i.get("published",""), reverse=True)
    return all_items


def _collapse_group(group: list[dict]) -> dict:
    """
    Merge a list of related items into one canonical feed item.
    group[0] is the highest-priority source (lead item).
    """
    lead = group[0]

    # Build structured multi-source summary for LLM consumption
    blocks = []
    for item in group:
        parts = [f"[{item.get('source','?')}] {item.get('title','')}"]
        desc = item.get("summary","").strip()
        if desc:
            parts.append(desc)
        url = item.get("url","")
        if url:
            parts.append(f"URL: {url}")
        blocks.append("\n".join(parts))

    combined_summary = "\n\n".join(blocks)

    # Preserve all URLs and source names as flat metadata
    all_urls    = "\n".join(i.get("url","") for i in group if i.get("url"))
    all_sources = "; ".join(dict.fromkeys(i.get("source","") for i in group))

    return {
        "id":            lead.get("id",""),
        "title":         lead.get("title",""),
        "url":           lead.get("url",""),
        "summary":       combined_summary,
        "source":        lead.get("source",""),
        "all_sources":   all_sources,
        "all_urls":      all_urls,
        "language":      lead.get("language",""),
        "category":      lead.get("category",""),
        "published":     lead.get("published",""),
        "date_verified": lead.get("date_verified", False),
        "cluster_size":  len(group),
    }


def _categories_compatible(cat_a: str, cat_b: str) -> bool:
    if cat_a == cat_b:
        return True
    return {cat_a, cat_b} in [{"local","regional"}, {"regional","global"}]


# ═══════════════════════════════════════════════════════════════════════════
# 3. BULLETIN DEDUPLICATION FIX
# ═══════════════════════════════════════════════════════════════════════════

def deduplicate_bulletin_text(text: str) -> str:
    """
    The Met Service page sometimes renders the bulletin text twice in different
    HTML containers. Detect the repeated marker and truncate at second occurrence.
    """
    if not text:
        return text
    marker = "Weather news for Mauritius issued at"
    first  = text.find(marker)
    second = text.find(marker, first + len(marker))
    if second != -1 and len(text[:second].strip()) > 100:
        return text[:second].strip()
    return text
