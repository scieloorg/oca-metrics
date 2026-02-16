"""
SciELO Document Merging Strategies
---------------------------------

This module provides three configurable strategies for merging SciELO article records that may represent the same underlying article published in multiple forms or with slight metadata variations. The strategies can be enabled or disabled when calling `merge_scielo_documents`.

1. DOI-based Merge (merge_by_doi):
   - Articles are considered for merging if they share the same DOI (including any language-variant DOIs).
   - Additionally, the articles must have at least one normalized title in common (case-insensitive, accent-insensitive, whitespace removed).
   - If both conditions are met, the articles are merged into a single record.

2. PID-based Merge (merge_by_pid):
   - Articles are considered for merging if they share the same SciELO PID (publisher ID).
   - The articles must also have the same publication year.
   - The journals must match, either by having at least one ISSN in common or by having the same normalized journal title.
   - The articles must have at least one normalized title in common.
   - If all these conditions are met, the articles are merged.

3. Title-based Merge (merge_by_title):
   - Articles are considered for merging if they share the same normalized title (case-insensitive, accent-insensitive, whitespace removed).
   - The title must not be a generic editorial/commentary (e.g., 'editorial', 'errata', etc.) and must be at least 15 characters long.
   - The articles must have the same publication year.
   - The journals must match, either by having at least one ISSN in common or by having the same normalized journal title.
   - If all these conditions are met, the articles are merged.

These strategies are applied in sequence and can be enabled or disabled via the `strategies` parameter in `merge_scielo_documents`. This modular approach allows for flexible and robust deduplication and consolidation of SciELO article metadata.
"""

from collections import defaultdict
from itertools import combinations
from pathlib import Path
from tqdm import tqdm
from xylose.scielodocument import Article

import bson
import datetime
import json
import logging

from oca_metrics.utils.normalization import (
    extract_year,
    stz_doi,
)
from oca_metrics.utils.scielo import (
    extract_document_type,
    extract_journal_issns,
    extract_journal_title,
    extract_titles,
)


logger = logging.getLogger(__name__)


def transform_article_to_doc(a, pub_year):
    doc_data = {
        "collection": a.collection_acronym,
        "pid_v2": a.publisher_id,
        "publication_year": pub_year,
        "doi_with_lang": {},
        "doi": "",
        "titles": extract_titles(a),
        "document_type": extract_document_type(a),
        "journal_title": extract_journal_title(a),
        "journal_issns": extract_journal_issns(a),
    }

    if a.doi:
        doi_stz = stz_doi(a.doi)
        if doi_stz:
            doc_data["doi"] = doi_stz

    if a.doi_and_lang:
        for lang_doi in a.doi_and_lang:
            lang, doi = lang_doi
            doi_stz = stz_doi(doi)
            if not doi_stz:
                continue

            doc_data["doi_with_lang"][lang] = doi_stz
    
    return doc_data

def load_raw_scl(path, start_year=2018, end_year=None):
    if end_year is None:
        end_year = datetime.datetime.now().year

    docs = []
    with open(path) as fin:
        for line in tqdm(fin, desc="Loading SciELO JSONL", unit="line"):
            j_evaluated = json.loads(line)

            pub_year = int(j_evaluated.get("publication_year"))
            if pub_year < start_year or pub_year > end_year:
                continue

            # Fix 'article' field which often comes as string representation in some dumps
            if isinstance(j_evaluated.get("article"), str):
                j_evaluated["article"] = eval(j_evaluated["article"])
            
            a = Article(j_evaluated)
            
            try:
                if not a.doi_and_lang and not a.doi:
                    continue

            except Exception:
                continue

            docs.append(transform_article_to_doc(a, pub_year))

    return docs

def load_bson_scl(path, start_year=2018, end_year=None):
    if end_year is None:
        end_year = datetime.datetime.now().year

    docs = []
    with open(path, 'rb') as f:
        for doc in tqdm(bson.decode_file_iter(f), desc="Loading SciELO BSON", unit="doc"):
            pub_year = extract_year(doc.get("publication_year"))
            if not pub_year or pub_year < start_year or pub_year > end_year:
                continue

            a = Article(doc)
            try:
                if not a.doi_and_lang and not a.doi:
                    continue

            except Exception:
                continue

            docs.append(transform_article_to_doc(a, pub_year))

    return docs

def _merge_by_doi(docs, doi_to_indices, union, f_audit=None):
    """
    DOI-based Merge Strategy
    -----------------------
    Articles are considered for merging if they share the same DOI (including any language-variant DOIs).
    Additionally, the articles must have at least one normalized title in common (case-insensitive, accent-insensitive, whitespace removed).
    If both conditions are met, the articles are merged into a single record.
    """
    for doi, indices in doi_to_indices.items():
        if len(indices) < 2:
            continue

        for i, j in combinations(indices, 2):
            titles1 = set(docs[i].get('titles', []))
            titles2 = set(docs[j].get('titles', []))
            has_title_overlap = bool(titles1 & titles2)

            if f_audit:
                audit_entry = {
                    "doi": doi,
                    "pid1": docs[i].get("pid_v2"),
                    "pid2": docs[j].get("pid_v2"),
                    "merged": has_title_overlap,
                    "reason": "doi_match"
                }
                f_audit.write(json.dumps(audit_entry) + "\n")

            if has_title_overlap:
                union(i, j)

def _merge_by_pid(docs, pid_to_indices, find, union):
    """
    PID-based Merge Strategy
    -----------------------
    Articles are considered for merging if they share the same SciELO PID (publisher ID).
    The articles must also have the same publication year.
    The journals must match, either by having at least one ISSN in common or by having the same normalized journal title.
    The articles must have at least one normalized title in common.
    If all these conditions are met, the articles are merged.
    """
    for pid, indices in pid_to_indices.items():
        if len(indices) < 2:
            continue

        for i, j in combinations(indices, 2):
            if find(i) == find(j):
                continue

            d1, d2 = docs[i], docs[j]
            if d1.get('publication_year') != d2.get('publication_year'):
                continue

            issns1 = set(d1.get('journal_issns', []))
            issns2 = set(d2.get('journal_issns', []))
            same_journal = bool(issns1 & issns2) or (d1.get('journal_title') == d2.get('journal_title') and d1.get('journal_title'))
            if not same_journal:
                continue

            if not (set(d1.get('titles', [])) & set(d2.get('titles', []))):
                continue

            union(i, j)

def _merge_by_title(docs, title_to_indices, find, union):
    """
    Title-based Merge Strategy
    -------------------------
    Articles are considered for merging if they share the same normalized title (case-insensitive, accent-insensitive, whitespace removed).
    The title must not be a generic editorial/commentary (e.g., 'editorial', 'errata', etc.) and must be at least 15 characters long.
    The articles must have the same publication year.
    The journals must match, either by having at least one ISSN in common or by having the same normalized journal title.
    If all these conditions are met, the articles are merged.
    """
    generic_titles = {"editorial", "errata", "introduction", "introduccion", "introducao",
                      "prefacio", "preface", "lettertoeditor", "cartaoeditor", "comentario", "commentary"}
    for title, indices in title_to_indices.items():
        if len(indices) < 2:
            continue

        if title in generic_titles or len(title) < 15:
            continue

        for i, j in combinations(indices, 2):
            if find(i) == find(j):
                continue

            d1, d2 = docs[i], docs[j]
            if d1.get('publication_year') != d2.get('publication_year'):
                continue

            issns1 = set(d1.get('journal_issns', []))
            issns2 = set(d2.get('journal_issns', []))
            same_journal = bool(issns1 & issns2) or (d1.get('journal_title') == d2.get('journal_title') and d1.get('journal_title'))
            if not same_journal:
                continue

            union(i, j)

def merge_scielo_documents(docs, audit_log_path=None, strategies=("doi", "pid", "title")):
    parents = list(range(len(docs)))

    def find(i):
        while parents[i] != i:
            parents[i] = parents[parents[i]]
            i = parents[i]

        return i

    def union(i, j):
        root_i, root_j = find(i), find(j)
        if root_i != root_j:
            parents[root_i] = root_j

    doi_to_indices = defaultdict(list)
    pid_to_indices = defaultdict(list)
    title_to_indices = defaultdict(list)

    for idx, doc in enumerate(docs):
        current_dois = {doc.get('doi', '')} | set(doc.get('doi_with_lang', {}).values())
        current_dois.discard("")
        for d in current_dois:
            doi_to_indices[d].append(idx)

        pid = doc.get('pid_v2')
        if pid:
            pid_to_indices[pid].append(idx)

        for t in doc.get('titles', []):
            if t:
                title_to_indices[t].append(idx)

    f_audit = None
    if audit_log_path:
        p = Path(audit_log_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        f_audit = open(p, "w")

    try:
        if "doi" in strategies:
            _merge_by_doi(docs, doi_to_indices, union, f_audit)

        if "pid" in strategies:
            _merge_by_pid(docs, pid_to_indices, find, union)

        if "title" in strategies:
            _merge_by_title(docs, title_to_indices, find, union)
    finally:
        if f_audit: f_audit.close()

    components = defaultdict(list)
    for i in range(len(docs)):
        components[find(i)].append(docs[i])

    merged_docs = []
    for group in tqdm(components.values(), desc="Consolidating SciELO groups", unit="group"):
        if len(group) == 1:
            doc = group[0].copy()

            doc['collection'] = [doc['collection']]
            doc['pid_v2'] = [doc['pid_v2']]

            merged_docs.append(doc)
            continue

        m_collections = sorted(list(set(d['collection'] for d in group)))
        m_pids = sorted(list(set(d['pid_v2'] for d in group)))
        m_years = sorted(list(set(d['publication_year'] for d in group)))

        m_doi_with_lang = {}
        for d in group:
            for lang, val in d.get('doi_with_lang', {}).items():
                if val: m_doi_with_lang[lang] = val

        m_titles = set()
        for d in group:
            m_titles.update(d.get('titles', []))

        m_issns = set()
        for d in group:
            m_issns.update(d.get('journal_issns', []))

        m_doc_types = sorted(list(set(d.get('document_type', '') for d in group if d.get('document_type'))))
        m_journal_titles = sorted(list(set(d.get('journal_title', '') for d in group if d.get('journal_title'))))

        m_doi = next((d.get('doi') for d in group if d.get('doi')), "")
        if not m_doi and m_doi_with_lang:
            m_doi = sorted(m_doi_with_lang.values())[0]

        merged_docs.append({
            "collection": m_collections,
            "pid_v2": m_pids,
            "publication_year": m_years[0] if m_years else None,
            "doi_with_lang": m_doi_with_lang,
            "doi": m_doi,
            "titles": sorted(list(m_titles)),
            "document_type": m_doc_types[0] if m_doc_types else "",
            "journal_title": m_journal_titles[0] if m_journal_titles else "",
            "journal_issns": sorted(list(m_issns)),
        })

    return merged_docs
