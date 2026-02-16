from typing import (
    Optional,
    Sequence,
)

import logging
import pandas as pd

from oca_metrics.adapters.base import BaseAdapter
from oca_metrics.utils.metrics import compute_normalized_impact


logger = logging.getLogger(__name__)


TARGET_CITATION_PERCENTILES = [99, 95, 90, 50]


class MetricsEngine:
    """Metrics computation engine that uses a data adapter."""

    def __init__(self, adapter: BaseAdapter, target_percentiles: Sequence[int] = None):
        self.adapter = adapter
        self.target_percentiles = target_percentiles or TARGET_CITATION_PERCENTILES

    def process_category(self, year: int, level: str, cat_id: str, windows: Sequence[int], df_meta: pd.DataFrame = None) -> Optional[pd.DataFrame]:
        """Processes a single category and returns enriched metrics per journal."""
        baseline_res = self.adapter.compute_baseline(year, level, cat_id, windows)
        if baseline_res is None:
            return None
        
        thresholds = self.adapter.compute_thresholds(year, level, cat_id, windows, self.target_percentiles)
        if not thresholds:
            return None
            
        df_journals = self.adapter.compute_journal_metrics(year, level, cat_id, windows, thresholds)
        if df_journals.empty:
            return None

        # Add category and year information
        df_journals['category_id'] = cat_id
        df_journals['topic_level'] = level
        df_journals['publication_year'] = year
        df_journals['category_publications_count'] = baseline_res['total_docs']
        df_journals['category_citations_total'] = baseline_res['total_citations']
        df_journals['category_citations_mean'] = baseline_res['mean_citations']
        df_journals['journal_impact_normalized'] = df_journals['journal_citations_mean'].apply(
            lambda x: compute_normalized_impact(x, baseline_res['mean_citations'])
        )
        
        for w in windows:
            df_journals[f'category_citations_total_window_{w}y'] = baseline_res[f'total_citations_window_{w}y']
            df_journals[f'category_citations_mean_window_{w}y'] = baseline_res[f'mean_citations_window_{w}y']
            df_journals[f'journal_impact_normalized_window_{w}y'] = df_journals[f'journal_citations_mean_window_{w}y'].apply(
                lambda x: compute_normalized_impact(x, baseline_res[f'mean_citations_window_{w}y'])
            )
            
        for p in self.target_percentiles:
            pct_val = 100 - p

            df_journals[f'top_{pct_val}pct_all_time_citations_threshold'] = thresholds.get(f"C_top{pct_val}pct", 0)
            df_journals[f'top_{pct_val}pct_all_time_publications_share_pct'] = (df_journals[f'top_{pct_val}pct_all_time_publications_count'] / df_journals['journal_publications_count']) * 100

            for w in windows:
                df_journals[f'top_{pct_val}pct_window_{w}y_citations_threshold'] = thresholds.get(f"C_top{pct_val}pct_window_{w}y", 0)
                df_journals[f'top_{pct_val}pct_window_{w}y_publications_share_pct'] = (df_journals[f'top_{pct_val}pct_window_{w}y_publications_count'] / df_journals['journal_publications_count']) * 100

        if df_meta is not None and not df_meta.empty:
            df_journals = pd.merge(
                df_journals, 
                df_meta[['source_id', 'publication_year', 'journal_title', 'is_scielo']], 
                left_on=['journal_id', 'publication_year'], 
                right_on=['source_id', 'publication_year'], 
                how='left', 
                suffixes=('', '_meta')
            )
            df_journals['journal_title'] = df_journals['journal_title'].fillna(df_journals['journal_id'])
            df_journals['is_scielo'] = df_journals['is_scielo'].fillna(0).astype(int)

        else:
            df_journals['journal_title'] = df_journals['journal_id']
            df_journals['is_scielo'] = 0
            
        return df_journals
