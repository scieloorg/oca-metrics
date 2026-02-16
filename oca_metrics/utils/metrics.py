from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
)

import logging
import numpy as np
import os
import pandas as pd


logger = logging.getLogger(__name__)


OPENALEX_URL_PREFIX = "https://openalex.org/"


def load_global_metadata(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        logger.warning(f"Global metadata file not found at {path}")
        return pd.DataFrame()
    
    logger.info(f"Loading global metadata from {path}...")
    try:
        cols_xlsx = [
            'OpenAlex ID',
            'Journal',
            'YEAR',
            'is SciELO',
            'SciELO Active and Valid in the Year',
        ]

        df = pd.read_excel(path, usecols=cols_xlsx)

        df['source_id'] = df['OpenAlex ID'].apply(
            lambda x: f"{OPENALEX_URL_PREFIX}{x}" if pd.notna(x) and str(x).startswith('S') else x
        )

        df = df.rename(columns={
            'Journal': 'journal_title',
            'YEAR': 'publication_year',
            'is SciELO': 'is_scielo'
        })
        return df

    except Exception as e:
        logger.error(f"Error loading global metadata: {e}")
        return pd.DataFrame()


def get_csv_schema_order(windows: Sequence[int], target_percentiles: Optional[Sequence[int]] = None) -> List[str]:
    if target_percentiles is None:
        target_percentiles = [99, 95, 90, 50]
        
    cols: List[str] = []
    cols += ["category_id", "topic_level", "journal_id", "journal_issn", "journal_title", "publication_year"]
    cols += ["category_citations_mean"]
    cols += [f"category_citations_mean_window_{w}y" for w in windows]
    cols += ["category_citations_total"]
    cols += [f"category_citations_total_window_{w}y" for w in windows]
    cols += ["category_publications_count"]
    cols += [f"citations_window_{w}y" for w in windows]
    cols += [f"citations_window_{w}y_works" for w in windows]
    cols += ["is_scielo", "journal_citations_mean"]
    cols += [f"journal_citations_mean_window_{w}y" for w in windows]
    cols += ["journal_citations_total", "journal_impact_normalized"]
    cols += [f"journal_impact_normalized_window_{w}y" for w in windows]
    cols += ["journal_publications_count"]
    
    for p in target_percentiles:
        pct = 100 - p

        cols += [f"top_{pct}pct_all_time_citations_threshold", f"top_{pct}pct_all_time_publications_count", f"top_{pct}pct_all_time_publications_share_pct"]

        for w in windows:
            cols += [f"top_{pct}pct_window_{w}y_citations_threshold", f"top_{pct}pct_window_{w}y_publications_count", f"top_{pct}pct_window_{w}y_publications_share_pct"]

    return cols


def format_output_header_name(internal_key: str) -> str:
    return internal_key.replace("_", " ")


def shorten_openalex_id(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    v = value.strip()
    if v.startswith(OPENALEX_URL_PREFIX):
        return v[len(OPENALEX_URL_PREFIX):]

    return v


def compute_percentiles(citations: List[int], percentiles: List[float]) -> Dict[float, float]:
    """Compute citation thresholds for given percentiles."""
    if not isinstance(citations, (list, tuple)) or not isinstance(percentiles, (list, tuple)):
        raise ValueError("Inputs must be lists.")

    if not citations:
        return {p: 0 for p in percentiles}

    arr = np.array(citations)

    return {p: float(np.percentile(arr, p * 100)) for p in percentiles}


def compute_normalized_impact(journal_mean: float, category_mean: float) -> float:
    """Compute normalized impact (journal mean / category mean), returns 0 if denominator is 0."""
    if not category_mean:
        return 0.0

    return journal_mean / category_mean
