"""
SciELO-OpenAlex and OpenAlex-OpenAlex Merging Strategies
-------------------------------------------------------

This module implements two main merging/consolidation processes:

1. SciELO-OpenAlex Matching (match_scielo_with_openalex):
   - For each SciELO article, all DOIs (including language variants) are used to search for matching OpenAlex works in the Parquet dataset.
   - If multiple OpenAlex works match a single SciELO article (e.g., due to multilingual versions), all are grouped under that SciELO article.
   - For each group, all relevant metrics (citations, windows, etc.) are aggregated (summed) to represent the total impact of the article, regardless of language/version.
   - Individual OpenAlex work details are preserved for reference.
   - The taxonomy fields (domain, field, subfield, topic) are consolidated from all matched works.
   - No OpenAlex-OpenAlex merging is performed at this stage; only grouping under SciELO articles.

2. OpenAlex-OpenAlex Consolidation (generate_merged_parquet):
   - In the final Parquet, all OpenAlex works that matched a single SciELO article are consolidated into a single record (the 'survivor'), with all metrics aggregated.
   - The 'all_work_ids' field lists all OpenAlex work IDs that were merged.
   - The 'is_merged' flag indicates if the record is a result of merging multiple OpenAlex works.
   - The 'oa_individual_works' field stores the details of each original OpenAlex work in JSON format.
   - OpenAlex works not matched to any SciELO article are kept as-is, with 'is_merged' set to False.
   - This ensures that each article is uniquely represented, with all versions and citations consolidated, avoiding double counting.

This two-step process ensures robust deduplication and accurate metric computation for articles published in multiple languages or with metadata variations.
"""

from collections import defaultdict
from pathlib import Path
from tqdm import tqdm

import datetime
import gc
import json
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from oca_metrics.utils.normalization import stz_doi


logger = logging.getLogger(__name__)


def _get_doi_to_scl_idx(scl_docs):
    """Maps SciELO DOIs (including language variants) to their article index."""
    doi_to_scl_idx = {}
    for idx, doc in enumerate(scl_docs):
        if doc.get('doi'):
            doi_to_scl_idx[doc['doi']] = idx

        for lang_doi in doc.get('doi_with_lang', {}).values():
            if lang_doi:
                doi_to_scl_idx[lang_doi] = idx

    return doi_to_scl_idx


def _scan_openalex_for_matches(ds_oa, doi_to_scl_idx, columns_to_load, start_year, end_year):
    """Scans OpenAlex dataset for works matching SciELO DOIs."""
    oa_matches = defaultdict(list)
    scanner = ds_oa.scanner(
        columns=columns_to_load,
        filter=(ds.field("publication_year") >= start_year) & (ds.field("publication_year") <= end_year),
        batch_size=100_000,
    )

    for rb in tqdm(scanner.to_batches(), desc="Searching for matches in OpenAlex", unit="batch"):
        df_batch = rb.to_pandas()
        df_batch["doi_stz"] = df_batch["doi"].apply(stz_doi)

        mask = df_batch["doi_stz"].isin(doi_to_scl_idx)
        df_matched = df_batch[mask]

        for _, row in df_matched.iterrows():
            scl_idx = doi_to_scl_idx[row["doi_stz"]]
            oa_matches[scl_idx].append(row.to_dict())

        del df_batch
        gc.collect()
    
    return oa_matches


def _consolidate_scl_oa_results(scl_docs, oa_matches):
    """Consolidates OpenAlex matches and metrics for each SciELO article."""
    scl_oa_merged = []
    tax_fields = ["domain", "field", "subfield", "topic"]
    safe_int = lambda x: int(x) if pd.notna(x) else 0

    for idx, scl_doc in enumerate(scl_docs):
        merged_entry = scl_doc.copy()
        matches = oa_matches.get(idx, [])

        if not matches:
            merged_entry["oa_metrics"] = None
            merged_entry["has_oa_match"] = False
        else:
            unique_oa_records = {}
            for m in matches:
                wid = m.get("work_id")
                if wid:
                    unique_oa_records[wid] = m

            global_agg = {
                "total_citations": 0,
                "citations_window_2y": 0,
                "citations_window_3y": 0,
                "citations_window_5y": 0,
            }
            works_detailed = {}
            found_taxonomy = {field: set() for field in tax_fields}

            for wid, m in unique_oa_records.items():
                work_metrics = {
                    "language": m.get("language"),
                    "source_id": m.get("source_id"),
                    "total_citations": safe_int(m.get("citations_total")),
                    "citations_window_2y": safe_int(m.get("citations_window_2y")),
                    "citations_window_3y": safe_int(m.get("citations_window_3y")),
                    "citations_window_5y": safe_int(m.get("citations_window_5y")),
                }

                global_agg["total_citations"] += work_metrics["total_citations"]
                global_agg["citations_window_2y"] += work_metrics["citations_window_2y"]
                global_agg["citations_window_3y"] += work_metrics["citations_window_3y"]
                global_agg["citations_window_5y"] += work_metrics["citations_window_5y"]

                for y in range(2012, datetime.datetime.now().year + 1):
                    col = f"citations_{y}"
                    if col in m:
                        val = safe_int(m.get(col))
                        work_metrics[col] = val
                        global_agg[col] = global_agg.get(col, 0) + val

                works_detailed[wid] = work_metrics
                for field in tax_fields:
                    val = m.get(field)
                    if pd.notna(val) and val:
                        found_taxonomy[field].add(str(val))

            merged_entry["oa_metrics"] = {
                "work_ids": sorted(list(unique_oa_records.keys())),
                "match_count": len(unique_oa_records),
                "global_totals": global_agg,
                "individual_works": works_detailed
            }
            merged_entry["has_oa_match"] = True
            for field in tax_fields:
                merged_entry[field] = sorted(list(found_taxonomy[field]))
        
        scl_oa_merged.append(merged_entry)
    
    return scl_oa_merged


def match_scielo_with_openalex(scl_docs, oa_parquet_dir, start_year=2018, end_year=None):
    """
    SciELO-OpenAlex Matching
    -----------------------
    For each SciELO article, all DOIs (including language variants) are used to find matching OpenAlex works.
    - All OpenAlex works with a matching DOI are grouped under the SciELO article.
    - Metrics from all matched OpenAlex works are aggregated (summed) to represent the total impact.
    - Individual OpenAlex work details are preserved.
    - Taxonomy fields are consolidated from all matched works.
    - No OpenAlex-OpenAlex merging is performed here; only grouping under SciELO articles.
    """
    if end_year is None:
        end_year = datetime.datetime.now().year

    doi_to_scl_idx = _get_doi_to_scl_idx(scl_docs)
    logger.info(f"Mapped {len(doi_to_scl_idx)} unique DOIs from {len(scl_docs)} SciELO articles.")

    oa_path = Path(oa_parquet_dir)
    parquet_files = sorted(p for p in oa_path.rglob("*") if p.is_file() and p.stat().st_size > 0)
    datasets = [ds.dataset(p, format="parquet") for p in parquet_files]

    unified_schema = pa.unify_schemas([d.schema for d in datasets], promote_options="permissive")
    ds_oa = ds.dataset(parquet_files, format="parquet", schema=unified_schema)

    columns_to_load = [
        "work_id", "doi", "publication_year", "language", "source_id",
        "domain", "field", "subfield", "topic",
        "citations_total", "citations_window_2y",
        "citations_window_3y", "citations_window_5y"
    ]
    specific_years = [f"citations_{y}" for y in range(2012, datetime.datetime.now().year + 1)]
    columns_to_load.extend([c for c in specific_years if c in unified_schema.names])

    oa_matches = _scan_openalex_for_matches(ds_oa, doi_to_scl_idx, columns_to_load, start_year, end_year)
    logger.info(f"Found OpenAlex matches for {len(oa_matches)} SciELO articles.")

    scl_oa_merged = _consolidate_scl_oa_results(scl_docs, oa_matches)

    return scl_oa_merged, unified_schema


def _get_wid_mappings(scl_oa_merged):
    """Generates mappings for work IDs and their corresponding SciELO article data."""
    wid_to_merged = {}
    all_merged_wids = set()
    survivor_to_merged = {}

    for data in scl_oa_merged:
        if data.get("has_oa_match"):
            wids = data["oa_metrics"]["work_ids"]
            if not wids:
                continue

            survivor = wids[0]
            survivor_to_merged[survivor] = data
            for wid in wids:
                all_merged_wids.add(wid)
                wid_to_merged[wid] = survivor
    
    return wid_to_merged, all_merged_wids, survivor_to_merged


def _consolidate_row(row, merged_data):
    """Consolidates an OpenAlex row with merged SciELO metadata and metrics."""
    new_row = row.to_dict()

    totals = merged_data["oa_metrics"]["global_totals"]
    for k, v in totals.items():
        col_name = "citations_total" if k == "total_citations" else k
        new_row[col_name] = v

    for y in range(2012, datetime.datetime.now().year + 1):
        col = f"citations_{y}"
        if col in totals:
            new_row[col] = totals[col]

    new_row["scielo_collection"] = merged_data.get("collection", [])
    new_row["scielo_pid_v2"] = merged_data.get("pid_v2", [])
    new_row["all_work_ids"] = merged_data["oa_metrics"]["work_ids"]
    new_row["is_merged"] = len(merged_data["oa_metrics"]["work_ids"]) > 1
    new_row["oa_individual_works"] = json.dumps(merged_data["oa_metrics"]["individual_works"])

    for tax in ["domain", "field", "subfield", "topic"]:
        if (not new_row.get(tax) or pd.isna(new_row.get(tax))) and merged_data.get(tax):
            new_row[tax] = merged_data[tax][0]

    return new_row


def generate_merged_parquet(scl_oa_merged, oa_parquet_dir, output_file, unified_schema):
    """
    OpenAlex-OpenAlex Consolidation
    ------------------------------
    In the final Parquet, all OpenAlex works that matched a single SciELO article are consolidated into a single record.
    - Metrics are aggregated for all merged works.
    - The 'all_work_ids' field lists all OpenAlex work IDs that were merged.
    - The 'is_merged' flag indicates if the record is a result of merging multiple OpenAlex works.
    - The 'oa_individual_works' field stores the details of each original OpenAlex work in JSON format.
    - OpenAlex works not matched to any SciELO article are kept as-is.
    - This ensures unique representation and avoids double counting.
    """
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    wid_to_merged, all_merged_wids, survivor_to_merged = _get_wid_mappings(scl_oa_merged)

    additional_fields = [
        pa.field("scielo_collection", pa.list_(pa.string())),
        pa.field("scielo_pid_v2", pa.list_(pa.string())),
        pa.field("all_work_ids", pa.list_(pa.string())),
        pa.field("is_merged", pa.bool_()),
        pa.field("oa_individual_works", pa.string())
    ]
    new_schema = pa.schema(list(unified_schema) + additional_fields)
    
    oa_path = Path(oa_parquet_dir)
    parquet_files = sorted(p for p in oa_path.rglob("*") if p.is_file() and p.stat().st_size > 0)
    dataset_original = ds.dataset(parquet_files, schema=unified_schema)
    scanner = dataset_original.scanner(columns=unified_schema.names, batch_size=1_000_000)

    emitted_survivors = set()
    writer = pq.ParquetWriter(output_file, new_schema)
    
    try:
        for batch in tqdm(scanner.to_batches(), desc="Generating Merged Parquet"):
            df_batch = batch.to_pandas()
            cit_cols = [c for c in df_batch.columns if c.startswith("citations_") or "window" in c]
            df_batch[cit_cols] = df_batch[cit_cols].fillna(0)
            
            rows_to_keep = []
            for _, row in df_batch.iterrows():
                wid = row["work_id"]

                if wid in all_merged_wids:
                    survivor = wid_to_merged[wid]

                    if survivor not in emitted_survivors:
                        merged_data = survivor_to_merged[survivor]
                        rows_to_keep.append(_consolidate_row(row, merged_data))
                        emitted_survivors.add(survivor)

                else:
                    new_row = row.to_dict()
                    new_row["scielo_collection"] = None
                    new_row["scielo_pid_v2"] = None
                    new_row["all_work_ids"] = [wid]
                    new_row["is_merged"] = False
                    new_row["oa_individual_works"] = None
                    rows_to_keep.append(new_row)
            
            if rows_to_keep:
                df_out = pd.DataFrame(rows_to_keep)
                for field in new_schema.names:
                    if field not in df_out.columns: df_out[field] = None

                df_out = df_out[new_schema.names]
                table_out = pa.Table.from_pandas(df_out, schema=new_schema)
                writer.write_table(table_out)
            del df_batch
            gc.collect()

        # Add SciELO articles without OpenAlex matches
        _write_unmatched_scielo(writer, scl_oa_merged, new_schema, unified_schema)

    finally:
        writer.close()
    
    logger.info(f"Merged dataset saved to {output_file}")


def _write_unmatched_scielo(writer, scl_oa_merged, new_schema, unified_schema):
    """Processes SciELO articles without OpenAlex matches and writes them to the Parquet file."""
    unmatched_rows = []
    for data in scl_oa_merged:
        if not data.get("has_oa_match"):
            row = {col: None for col in unified_schema.names}

            row["publication_year"] = data.get("publication_year")
            row["doi"] = data.get("doi")
            row["work_id"] = f"scielo:{data.get('pid_v2', [''])[0]}"

            row["citations_total"] = 0
            for col in unified_schema.names:
                if col.startswith("citations_") or "window" in col:
                    row[col] = 0

            row["scielo_collection"] = data.get("collection", [])
            row["scielo_pid_v2"] = data.get("pid_v2", [])
            row["all_work_ids"] = []
            row["is_merged"] = False
            row["oa_individual_works"] = None

            for tax in ["domain", "field", "subfield", "topic"]:
                if data.get(tax):
                    row[tax] = data[tax][0]

            unmatched_rows.append(row)

            if len(unmatched_rows) >= 50_000:
                df_out = pd.DataFrame(unmatched_rows)

                for field in new_schema.names:
                    if field not in df_out.columns:
                        df_out[field] = None

                df_out = df_out[new_schema.names]
                writer.write_table(pa.Table.from_pandas(df_out, schema=new_schema))
                unmatched_rows = []

    if unmatched_rows:
        df_out = pd.DataFrame(unmatched_rows)

        for field in new_schema.names:
            if field not in df_out.columns:
                df_out[field] = None

        df_out = df_out[new_schema.names]
        writer.write_table(pa.Table.from_pandas(df_out, schema=new_schema))
