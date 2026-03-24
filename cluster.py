"""
cluster.py — Deduplication and semantic clustering for the Mauritius news aggregator.

WHAT THIS MODULE DOES
─────────────────────
1. DEDUPLICATION  — removes items that are the same article:
   • Pass 1: canonical URL match (strips UTM params, /amp suffixes)
   • Pass 2: ID hash match (aggregator re-ingestion)
   • Pass 3: exact normalised-title match
   • Pass 4: semantic cosine similarity on sentence embeddings (threshold 0.92)

2. CLUSTERING — groups items that cover the same news event:
   • Pass A: same-language clustering on raw items
   • Pass B: collapse each cluster into one feed item
   • Pass C: second-pass cross-language merge on collapsed clusters

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
import hashlib
from datetime import datetime, timezone
from collections import defaultdict

log = logging.getLogger(__name__)

# ── Model configuration ──────────────────────────────────────────────────────

_MODEL_NAME = "all-MiniLM-L6-v2"
_DEDUP_THRESHOLD = 0.92          # same article
_CLUSTER_THRESHOLD = 0.70        # same-event clustering within same language
_XLANG_CLUSTER_THRESHOLD = 0.70  # second-pass EN/FR merge on collapsed clusters
_SUMMARY_EMBED_CHARS = 500       # chars of summary to include in semantic text

# ── Singleton model loader ───────────────────────────────────────────────────

_model = None
_USE_EMBEDDINGS = None  # None = not yet attempted


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


def _similarity_matrix(embeddings):
    """Full pairwise cosine similarity matrix via matrix multiply."""
    return embeddings @ embeddings.T


# ── Text utilities ───────────────────────────────────────────────────────────

def _normalise(text: str) -> str:
    text = unicodedata.normalize("NFD", text or "")
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_summary_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def _semantic_text(item: dict) -> str:
    """
    Semantic text for within-language clustering and dedup.
    Uses normalised title + summary excerpt.
    """
    title = _normalise(item.get("title", ""))
    summary = _clean_summary_text(item.get("summary", ""))
    if summary:
        return f"{title} {summary[:_SUMMARY_EMBED_CHARS]}"
    return title


def _cross_language_semantic_text(item: dict) -> str:
    """
    Semantic text for second-pass cross-language cluster merge.
    """
    title = _normalise(item.get("title", ""))
    summary = _clean_summary_text(item.get("summary", ""))
    if summary:
        return f"{title} {summary[:_SUMMARY_EMBED_CHARS]}"
    return title


def _normalised_title_key(item: dict) -> str:
    return _normalise(item.get("title", ""))


def _canonical_url(url: str) -> str:
    url = re.sub(r"[?#].*$", "", url or "")
    url = re.sub(r"/amp/?$", "", url)
    return url.rstrip("/").lower()


def _ngrams(tokens: list[str], n: int) -> set[str]:
    return {" ".join(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


def _title_similarity(a: str, b: str) -> float:
    """Hybrid bigram Jaccard + unigram containment. Fallback when no embeddings."""
    ta, tb = _normalise(a).split(), _normalise(b).split()
    if not ta or not tb:
        return 0.0
    ng_a = _ngrams(ta, 2) or set(ta)
    ng_b = _ngrams(tb, 2) or set(tb)
    bigram_j = len(ng_a & ng_b) / len(ng_a | ng_b) if (ng_a | ng_b) else 0.0
    shorter = set(ta) if len(ta) <= len(tb) else set(tb)
    longer = set(tb) if len(ta) <= len(tb) else set(ta)
    containment = len(shorter & longer) / len(shorter) if shorter else 0.0
    return 0.4 * bigram_j + 0.6 * containment


def _headline_quality(item: dict) -> tuple[int, int, int]:
    """
    Prefer clearer, more informative lead headlines without overcomplicating it.
    Higher is better.
    """
    title = (item.get("title", "") or "").strip()
    title_len = len(title)

    has_question = int("?" in title)
    has_separator = int(" : " in title or " - " in title or " | " in title)
    reasonable_len = int(35 <= title_len <= 140)

    return (
        reasonable_len,
        has_separator,
        -has_question,
    )


def _parse_iso_dt(value: str | None) -> datetime:
    if not value:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _latest_published(items: list[dict]) -> str:
    latest = max((_parse_iso_dt(i.get("published")) for i in items), default=datetime(1970, 1, 1, tzinfo=timezone.utc))
    return latest.isoformat()


def _unique_nonblank(values: list[str]) -> list[str]:
    return list(dict.fromkeys(v for v in values if v))


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
        _headline_quality(i),
        len(i.get("summary", "")),
        len(i.get("title", "")),
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
    Four-pass deduplication.

    Pass 1 — canonical URL
    Pass 2 — ID hash
    Pass 3 — exact normalised title
    Pass 4 — semantic similarity (same article, reworded title)
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

    # Pass 3: exact normalised title
    title_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in deduped:
        lang = item.get("language", "") or ""
        key = _normalised_title_key(item)
        if key:
            title_groups[(lang, key)].append(item)
        else:
            title_groups[(lang, f"__blank__::{id(item)}")].append(item)

    deduped = [_best_item(g) for g in title_groups.values()]

    # Pass 4: semantic dedup
    before = len(deduped)
    deduped = _semantic_dedup(deduped)
    removed = before - len(deduped)
    log.info(f"[cluster] After semantic dedup: {len(deduped)} items ({removed} removed)")
    return deduped


def _semantic_dedup(items: list[dict]) -> list[dict]:
    if len(items) < 2:
        return items

    semantic_texts = [_semantic_text(item) for item in items]
    embeddings = _embed(semantic_texts)

    n = len(items)
    parent, find, union = _make_uf(n, items)

    if embeddings is not None:
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
        threshold = 0.72
        buckets: dict[str, list[int]] = defaultdict(list)
        for idx, item in enumerate(items):
            for word in _normalise(item.get("title", "")).split():
                if len(word) >= 5:
                    buckets[word].append(idx)
        checked: set[tuple[int, int]] = set()
        for indices in buckets.values():
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    a, b = indices[i], indices[j]
                    pair = (min(a, b), max(a, b))
                    if pair in checked:
                        continue
                    checked.add(pair)
                    if items[a].get("language") != items[b].get("language"):
                        continue
                    if _title_similarity(items[a].get("title", ""), items[b].get("title", "")) >= threshold:
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
    a single, schema-consistent feed item.

    Flow:
      1) cluster raw items within language
      2) collapse groups
      3) merge collapsed groups across language
      4) normalise passthrough items to same schema
    """
    passthrough_raw = [i for i in items if i.get("category", "") in _NO_CLUSTER_CATEGORIES]
    clusterable = [i for i in items if i.get("category", "") not in _NO_CLUSTER_CATEGORIES]

    passthrough = [_normalise_singleton_item(i) for i in passthrough_raw]

    if not clusterable:
        return sorted(passthrough, key=lambda i: i.get("cluster_time", ""), reverse=True)

    groups = _cluster_raw_items_same_language(clusterable)

    collapsed = []
    multi = 0
    for group_items in groups:
        group_sorted = sorted(group_items, key=lambda i: (
            _priority(i),
            int(i.get("date_verified", False)),
            _headline_quality(i),
            len(i.get("summary", "")),
            len(i.get("title", "")),
        ), reverse=True)
        if len(group_sorted) == 1:
            collapsed.append(_normalise_singleton_item(group_sorted[0]))
        else:
            multi += 1
            collapsed.append(_collapse_group(group_sorted))

    log.info(
        f"[cluster] Same-language pass: {multi} multi-source clusters "
        f"from {len(clusterable)} items → {len(collapsed)} collapsed"
    )

    before_xlang = len(collapsed)
    collapsed = _merge_cross_language_clusters(collapsed)
    after_xlang = len(collapsed)

    log.info(
        f"[cluster] Cross-language merge: {before_xlang} collapsed items "
        f"→ {after_xlang}"
    )

    all_items = collapsed + passthrough
    all_items.sort(key=lambda i: i.get("cluster_time", ""), reverse=True)
    return all_items


def _cluster_raw_items_same_language(clusterable: list[dict]) -> list[list[dict]]:
    n = len(clusterable)
    if n == 0:
        return []

    semantic_texts = [_semantic_text(i) for i in clusterable]
    embeddings = _embed(semantic_texts)

    parent, find, union = _make_uf(n, clusterable)

    if embeddings is not None:
        sim_matrix = _similarity_matrix(embeddings)
        threshold = _CLUSTER_THRESHOLD
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue
                ia, ib = clusterable[i], clusterable[j]
                if ia.get("language") != ib.get("language"):
                    continue
                if not _categories_compatible(ia.get("category", ""), ib.get("category", "")):
                    continue
                if float(sim_matrix[i, j]) >= threshold:
                    union(i, j)
    else:
        threshold = 0.40
        buckets: dict[str, list[int]] = defaultdict(list)
        for idx, item in enumerate(clusterable):
            for word in _normalise(item.get("title", "")).split():
                if len(word) >= 4:
                    buckets[word].append(idx)

        checked: set[tuple[int, int]] = set()
        for indices in buckets.values():
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    a, b = indices[i], indices[j]
                    pair = (min(a, b), max(a, b))
                    if pair in checked:
                        continue
                    checked.add(pair)
                    ia, ib = clusterable[a], clusterable[b]
                    if ia.get("language") != ib.get("language"):
                        continue
                    if not _categories_compatible(ia.get("category", ""), ib.get("category", "")):
                        continue
                    if _title_similarity(ia.get("title", ""), ib.get("title", "")) >= threshold:
                        union(a, b)

    groups: dict[int, list[dict]] = defaultdict(list)
    for idx in range(n):
        groups[find(idx)].append(clusterable[idx])
    return list(groups.values())


def _merge_cross_language_clusters(items: list[dict]) -> list[dict]:
    """
    Second-pass merge on already-collapsed items.
    This is where EN/FR versions of the same event can be joined.
    """
    if len(items) < 2:
        return items

    semantic_texts = [_cross_language_semantic_text(item) for item in items]
    embeddings = _embed(semantic_texts)

    n = len(items)
    parent, find, union = _make_uf(n, items)

    if embeddings is not None:
        sim_matrix = _similarity_matrix(embeddings)
        threshold = _XLANG_CLUSTER_THRESHOLD

        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue

                ia, ib = items[i], items[j]

                if ia.get("language") == ib.get("language"):
                    continue

                if not _categories_compatible(ia.get("category", ""), ib.get("category", "")):
                    continue

                if float(sim_matrix[i, j]) >= threshold:
                    union(i, j)
    else:
        threshold = 0.55
        for i in range(n):
            for j in range(i + 1, n):
                if find(i) == find(j):
                    continue

                ia, ib = items[i], items[j]

                if ia.get("language") == ib.get("language"):
                    continue

                if not _categories_compatible(ia.get("category", ""), ib.get("category", "")):
                    continue

                if _title_similarity(ia.get("title", ""), ib.get("title", "")) >= threshold:
                    union(i, j)

    groups: dict[int, list[dict]] = defaultdict(list)
    for idx in range(n):
        groups[find(idx)].append(items[idx])

    merged = []
    for group in groups.values():
        if len(group) == 1:
            merged.append(group[0])
        else:
            group_sorted = sorted(group, key=lambda i: (
                _priority(i),
                _headline_quality(i),
                len(i.get("summary", "")),
                len(i.get("title", "")),
            ), reverse=True)
            merged.append(_collapse_group(group_sorted))

    return merged


def _normalise_singleton_item(item: dict) -> dict:
    title = item.get("title", "")
    url = item.get("url", "")
    source = item.get("source", "")
    language = item.get("language", "")
    summary = (item.get("summary") or "").strip() or title
    cluster_time = item.get("published", "")

    cluster_seed = f"{title} || {source} || {url}"
    cluster_id = hashlib.md5(cluster_seed.encode("utf-8")).hexdigest()[:12]

    return {
        "id": item.get("id", ""),
        "title": title,
        "url": url,
        "summary": summary,
        "source": source,
        "category": item.get("category", ""),
        "language": language,
        "cluster_time": cluster_time,
        "published": cluster_time,  # keep internal compatibility
        "cluster_size": 1,
        "source_count": 1,
        "sources": [source] if source else [],
        "urls": [url] if url else [],
        "titles": [title] if title else [],
        "languages": [language] if language else [],
        "cluster_id": cluster_id,
    }


def _summary_block(item: dict) -> str:
    source = item.get("source", "?")
    summary = _clean_summary_text(item.get("summary", ""))
    title = (item.get("title", "") or "").strip()

    body = summary or title
    if not body:
        return f"[{source}]"

    return f"[{source}] {body}"


def _collapse_group(group: list[dict]) -> dict:
    """
    Merge related items into one canonical feed item.
    group[0] is highest-priority source (lead).

    Output schema is intentionally lean for downstream LLM use.
    """
    lead = group[0]

    summary_blocks = []
    all_title_list = []
    all_url_list = []
    all_source_list = []
    all_language_list = []

    for item in group:
        title = item.get("title", "")
        source = item.get("source", "")
        url = item.get("url", "")
        lang = item.get("language", "")

        if title:
            all_title_list.append(title)
        if url:
            all_url_list.append(url)
        if source:
            all_source_list.append(source)
        if lang:
            all_language_list.append(lang)

        block = _summary_block(item)
        if block:
            summary_blocks.append(block)

    unique_titles = _unique_nonblank(all_title_list)
    unique_urls = _unique_nonblank(all_url_list)
    unique_sources = _unique_nonblank(all_source_list)
    unique_languages = _unique_nonblank(all_language_list)
    unique_blocks = _unique_nonblank(summary_blocks)

    combined_summary = "\n\n".join(unique_blocks)

    cluster_seed = " | ".join(unique_titles[:5]) + " || " + " | ".join(unique_sources[:5])
    cluster_id = hashlib.md5(cluster_seed.encode("utf-8")).hexdigest()[:12]
    cluster_time = _latest_published(group)

    return {
        "id": lead.get("id", ""),
        "title": lead.get("title", ""),
        "url": lead.get("url", ""),
        "summary": combined_summary or lead.get("title", ""),
        "source": lead.get("source", ""),
        "category": lead.get("category", ""),
        "language": lead.get("language", ""),
        "cluster_time": cluster_time,
        "published": cluster_time,  # keep internal compatibility
        "cluster_size": len(group),
        "source_count": len(unique_sources),
        "sources": unique_sources,
        "urls": unique_urls,
        "titles": unique_titles,
        "languages": unique_languages,
        "cluster_id": cluster_id,
    }


def _categories_compatible(cat_a: str, cat_b: str) -> bool:
    if cat_a == cat_b:
        return True
    return {cat_a, cat_b} in [{"local", "regional"}, {"regional", "global"}]


# ═══════════════════════════════════════════════════════════════════════════
# 3. BULLETIN DEDUPLICATION FIX
# ═══════════════════════════════════════════════════════════════════════════

def deduplicate_bulletin_text(text: str) -> str:
    """Remove duplicate bulletin text caused by double-rendering on Met Service page."""
    if not text:
        return text
    marker = "Weather news for Mauritius issued at"
    first = text.find(marker)
    second = text.find(marker, first + len(marker))
    if second != -1 and len(text[:second].strip()) > 100:
        return text[:second].strip()
    return text
