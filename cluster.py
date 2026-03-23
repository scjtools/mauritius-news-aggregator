"""
cluster.py — Deduplication and semantic clustering for the Mauritius news aggregator.

WHAT THIS MODULE DOES
─────────────────────
1. DEDUPLICATION  — removes items that are the same article:
   • Pass 1: canonical URL match (strips UTM params, /amp suffixes)
   • Pass 2: ID hash match (aggregator re-ingestion)
   • Pass 3: semantic cosine similarity on sentence embeddings (threshold 0.92)

2. CLUSTERING — groups items that cover the same news event:
   • Full dense matrix comparison (no word-bucket pre-filtering)
   • Catches synonym-based rewrites: "jet hits vehicle" == "plane collides with truck"
   • Language-aware: fr and en items cluster separately
   • Each cluster collapsed into one feed item with all sources/titles/descriptions

3. EMBEDDING MODEL
   • all-MiniLM-L6-v2 (80MB, MIT licence)
   • Loaded once per process via module-level singleton
   • Cached at ~/.cache/huggingface between GitHub Actions runs
   • Falls back to hybrid title similarity if unavailable

USAGE
─────
    from cluster import deduplicate_items, cluster_and_collapse, deduplicate_bulletin_text
"""

from __future__ import annotations

import re
import unicodedata
import logging
from collections import defaultdict
from typing import Any

log = logging.getLogger(__name__)

# ── Model configuration ──────────────────────────────────────────────────────

_MODEL_NAME        = "all-MiniLM-L6-v2"
_DEDUP_THRESHOLD   = 0.92   # above this → same article, keep best source
_CLUSTER_THRESHOLD = 0.72   # above this → same event, merge into cluster

# ── Singleton model loader ────────────────────────────────────────────────────

_model = None
_USE_EMBEDDINGS = None   # None = not yet attempted


def _get_model():
    global _model, _USE_EMBEDDINGS
    if _USE_EMBEDDINGS is False:
        return None
    if _model is not None:
        return _model
    try:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(_MODEL_NAME)
        _USE_EMBEDDINGS = True
        log.info(f"[cluster] Loaded {_MODEL_NAME}")
        return _model
    except Exception as e:
        log.warning(f"[cluster] sentence-transformers unavailable ({e}), using title similarity")
        _USE_EMBEDDINGS = False
        return None


def _embed(texts: list[str]):
    """
    Encode texts to L2-normalised embeddings (numpy array shape [n, d]).
    Returns None if model unavailable.
    """
    model = _get_model()
    if model is None:
        return None
    try:
        import numpy as np
        vecs = model.encode(
            texts,
            batch_size=64,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        return np.array(vecs, dtype=np.float32)
    except Exception as e:
        log.warning(f"[cluster] Embedding failed: {e}")
        return None


def _similarity_matrix(embeddings) -> "np.ndarray":
    """
    Full pairwise cosine similarity matrix via matrix multiply.
    Embeddings must be L2-normalised (dot product == cosine sim).
    Shape: [n, n], dtype float32.
    """
    import numpy as np
    return embeddings @ embeddings.T


# ── Text utilities ────────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _canonical_url(url: str) -> str:
    url = re.sub(r"[?#].*$", "", url)
    url = re.sub(r"/amp/?$", "", url)
    return url.rstrip("/").lower()


def _ngrams(tokens: list[str], n: int) -> set[str]:
    return {" ".join(tokens[i : i + n]) for i in range(len(tokens) - n + 1)}


def _title_similarity(a: str, b: str) -> float:
    """Hybrid bigram Jaccard + unigram containment. Fallback when no embeddings."""
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


# ── Source priority ───────────────────────────────────────────────────────────

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
    return max(candidates, key=lambda i: (
        _priority(i),
        int(i.get("date_verified", False)),
        len(i.get("summary", "")),
    ))


# ── Union-Find ────────────────────────────────────────────────────────────────

def _make_uf(n: int, items: list[dict]):
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx == ry:
            return
        if _priority(items[rx]) >= _priority(items[ry]):
            parent[ry] = rx
        else:
            parent[rx] = ry

    return parent, find, union


# ═══════════════════════════════════════════════════════════════════════════
# 1. DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════

def deduplicate_items(items: list[dict]) -> list[dict]:
    """
    Three-pass deduplication.

    Pass 1 — canonical URL
    Pass 2 — ID hash
    Pass 3 — semantic similarity (same article, reworded title)
              Uses dense matrix comparison when embeddings available.
    """
    # Pass 1: canonical URL
    url_groups: dict[str, list[dict]] = defaultdict(list)
    for item in items:
        url_groups[_canonical_url(item.get("url", ""))].append(item)
    deduped = [_best_item(g) for g in url_groups.values()]

    # Pass 2: ID hash
    seen: dict[str, dict] = {}
    for item in deduped:
        iid = item.get("id", "")
        if iid not in seen:
            seen[iid] = item
        else:
            seen[iid] = _best_item([seen[iid], item])
    deduped = list(seen.values())

    # Pass 3: semantic
    before = len(deduped)
    deduped = _semantic_dedup(deduped)
    removed = before - len(deduped)
    if removed:
        log.info(f"[cluster] Semantic dedup removed {removed} items")

    return deduped


def _semantic_dedup(items: list[dict]) -> list[dict]:
    if len(items) < 2:
        return items

    titles = [item.get("title", "") for item in items]
    embeddings = _embed(titles)

    n = len(items)
    parent, find, union = _make_uf(n, items)

    if embeddings is not None:
        # Dense matrix — catches all pairs regardless of vocabulary overlap
        sim_matrix = _similarity_matrix(embeddings)
        threshold = _DEDUP_THRESHOLD
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                if items[i].get("language") != items[j].get("language"):
                    continue
                if float(sim_matrix[i, j]) >= threshold:
                    union(i, j)
    else:
        # Fallback: word-bucket candidate pairs + title similarity
        threshold = 0.72
        buckets: dict[str, list[int]] = defaultdict(list)
        for idx, item in enumerate(items):
            for word in _normalise(item.get("title", "")).split():
                if len(word) >= 5:
                    buckets[word].append(idx)
        checked: set[tuple[int,int]] = set()
        for indices in buckets.values():
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    a, b = indices[i], indices[j]
                    pair = (min(a,b), max(a,b))
                    if pair in checked:
                        continue
                    checked.add(pair)
                    if items[a].get("language") != items[b].get("language"):
                        continue
                    if _title_similarity(items[a].get("title",""), items[b].get("title","")) >= threshold:
                        union(a, b)

    groups: dict[int, list[dict]] = defaultdict(list)
    for idx in range(n):
        groups[find(idx)].append(items[idx])
    return [_best_item(g) for g in groups.values()]


# ═══════════════════════════════════════════════════════════════════════════
# 2. CLUSTERING + COLLAPSE
# ═══════════════════════════════════════════════════════════════════════════

_NO_CLUSTER_CATEGORIES = {"finance", "weather", "utilities"}


def cluster_and_collapse(items: list[dict]) -> list[dict]:
    """
    Groups items covering the same news event and collapses each cluster into
    a single feed item with structured multi-source description.

    Uses dense matrix comparison (no word-bucket pre-filtering) so synonym-based
    rewrites ("jet hits vehicle" == "plane collides with truck") are caught.

    Finance / weather / utilities pass through unclustered.
    French and English items cluster independently.
    """
    passthrough = [i for i in items if i.get("category","") in _NO_CLUSTER_CATEGORIES]
    clusterable = [i for i in items if i.get("category","") not in _NO_CLUSTER_CATEGORIES]

    for item in passthrough:
        item.setdefault("cluster_size", 1)

    if not clusterable:
        return sorted(passthrough, key=lambda i: i.get("published",""), reverse=True)

    n = len(clusterable)
    titles = [i.get("title","") for i in clusterable]
    embeddings = _embed(titles)

    parent, find, union = _make_uf(n, clusterable)

    if embeddings is not None:
        # Full pairwise comparison — no candidate pre-filter needed at n<500
        sim_matrix = _similarity_matrix(embeddings)
        threshold = _CLUSTER_THRESHOLD
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ia, ib = clusterable[i], clusterable[j]
                if ia.get("language") != ib.get("language"):
                    continue
                if not _categories_compatible(ia.get("category",""), ib.get("category","")):
                    continue
                if float(sim_matrix[i, j]) >= threshold:
                    union(i, j)
    else:
        # Fallback: word-bucket candidates + title similarity
        threshold = 0.40
        buckets: dict[str, list[int]] = defaultdict(list)
        for idx, item in enumerate(clusterable):
            for word in _normalise(item.get("title","")).split():
                if len(word) >= 4:
                    buckets[word].append(idx)
        checked: set[tuple[int,int]] = set()
        for indices in buckets.values():
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    a, b = indices[i], indices[j]
                    pair = (min(a,b), max(a,b))
                    if pair in checked:
                        continue
                    checked.add(pair)
                    ia, ib = clusterable[a], clusterable[b]
                    if ia.get("language") != ib.get("language"):
                        continue
                    if not _categories_compatible(ia.get("category",""), ib.get("category","")):
                        continue
                    if _title_similarity(ia.get("title",""), ib.get("title","")) >= threshold:
                        union(a, b)

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

    log.info(f"[cluster] {multi} multi-source clusters from {len(clusterable)} items "
             f"→ {len(collapsed)} collapsed")

    all_items = collapsed + passthrough
    all_items.sort(key=lambda i: i.get("published",""), reverse=True)
    return all_items


def _collapse_group(group: list[dict]) -> dict:
    """
    Merge related items into one canonical feed item.
    group[0] is highest-priority source (lead).

    Summary format for LLM consumption:
        [SOURCE A] Title from source A
        Description from source A.
        URL: https://...

        [SOURCE B] Title from source B
        ...
    """
    lead = group[0]

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
    """Remove duplicate bulletin text caused by double-rendering on Met Service page."""
    if not text:
        return text
    marker = "Weather news for Mauritius issued at"
    first  = text.find(marker)
    second = text.find(marker, first + len(marker))
    if second != -1 and len(text[:second].strip()) > 100:
        return text[:second].strip()
    return text
