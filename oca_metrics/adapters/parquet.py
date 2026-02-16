from typing import Any, Dict, List, Optional, Sequence

import duckdb
import logging
import pandas as pd

from oca_metrics.adapters.base import BaseAdapter


logger = logging.getLogger(__name__)


class ParquetAdapter(BaseAdapter):
    """Adapter for extraction and computation of bibliometric indicators from Parquet data using DuckDB."""

    def __init__(self, parquet_path: str, table_name: str = "metrics"):
        self.con = duckdb.connect(database=':memory:')
        self.table_name = table_name

        try:
            self.con.execute(f"CREATE VIEW {self.table_name} AS SELECT * FROM read_parquet('{parquet_path}', union_by_name=True)")
        except Exception as e:
            logger.error(f"Failed to load parquet file at {parquet_path}: {e}")
            raise

    def get_categories(self, year: int, level: str, category_id: Optional[str] = None) -> List[str]:
        cat_filter = f"AND {level} = '{category_id}'" if category_id else ""
        query = f"SELECT DISTINCT {level} FROM {self.table_name} WHERE publication_year = {year} AND {level} IS NOT NULL {cat_filter}"

        try:
            categories = self.con.execute(query).fetchall()
            return [c[0] for c in categories]
        except Exception as e:
            logger.error(f"Error fetching categories: {e}")
            return []

    def compute_baseline(self, year: int, level: str, cat_id: str, windows: Sequence[int]) -> Optional[pd.Series]:
        query = f"""
        SELECT 
            COUNT(*) as total_docs,
            SUM(citations_total) as total_citations,
            AVG(citations_total) as mean_citations,
            {", ".join([f"SUM(citations_window_{w}y) as total_citations_window_{w}y" for w in windows])},
            {", ".join([f"AVG(citations_window_{w}y) as mean_citations_window_{w}y" for w in windows])}
        FROM {self.table_name}
        WHERE publication_year = {year} AND {level} = '{cat_id}'
        """
        try:
            res = self.con.execute(query).df()
            if res.empty or res.iloc[0]['total_docs'] == 0:
                return None

            return res.iloc[0]

        except Exception as e:
            logger.error(f"Error computing baseline for {cat_id} in {year}: {e}")
            return None

    def compute_thresholds(self, year: int, level: str, cat_id: str, windows: Sequence[int], target_percentiles: Sequence[int]) -> Dict[str, Any]:
        threshold_cols = []
        for p in target_percentiles:
            threshold_cols.append(f"CAST(quantile_cont(citations_total, {p/100.0}) AS INT) + 1 as C_top{100-p}pct")

            for w in windows:
                threshold_cols.append(f"CAST(quantile_cont(citations_window_{w}y, {p/100.0}) AS INT) + 1 as C_top{100-p}pct_window_{w}y")
        
        query = f"SELECT {', '.join(threshold_cols)} FROM {self.table_name} WHERE publication_year = {year} AND {level} = '{cat_id}'"
        try:
            return self.con.execute(query).df().iloc[0].to_dict()

        except Exception as e:
            logger.error(f"Error computing thresholds for {cat_id} in {year}: {e}")
            return {}

    def compute_journal_metrics(self, year: int, level: str, cat_id: str, windows: Sequence[int], thresholds: Dict[str, Any]) -> pd.DataFrame:
        top_counts_sql = []
        # Extract percentiles from threshold keys
        percentiles = set()
        for key in thresholds.keys():
            if key.startswith("C_top") and "pct" in key:
                parts = key.split("top")[1].split("pct")
                percentiles.add(int(parts[0]))

        for pct_val in sorted(percentiles, reverse=True):
            t_all = thresholds.get(f"C_top{pct_val}pct", 0)
            top_counts_sql.append(f"SUM(CASE WHEN citations_total >= {t_all} THEN 1 ELSE 0 END) as top_{pct_val}pct_all_time_publications_count")

            for w in windows:
                t_w = thresholds.get(f"C_top{pct_val}pct_window_{w}y", 0)
                top_counts_sql.append(f"SUM(CASE WHEN citations_window_{w}y >= {t_w} THEN 1 ELSE 0 END) as top_{pct_val}pct_window_{w}y_publications_count")

        query = f"""
        SELECT 
            source_id as journal_id,
            ANY_VALUE(source_issn_l) as journal_issn,
            COUNT(*) as journal_publications_count,
            SUM(citations_total) as journal_citations_total,
            AVG(citations_total) as journal_citations_mean,
            {", ".join([f"SUM(citations_window_{w}y) as citations_window_{w}y" for w in windows])},
            {", ".join([f"SUM(CASE WHEN citations_window_{w}y >= 1 THEN 1 ELSE 0 END) as citations_window_{w}y_works" for w in windows])},
            {", ".join([f"AVG(citations_window_{w}y) as journal_citations_mean_window_{w}y" for w in windows])},
            {", ".join(top_counts_sql)}
        FROM {self.table_name}
        WHERE publication_year = {year} AND {level} = '{cat_id}' AND source_id IS NOT NULL
        GROUP BY source_id
        """
        try:
            return self.con.execute(query).df()

        except Exception as e:
            logger.error(f"Error computing journal metrics for {cat_id} in {year}: {e}")
            return pd.DataFrame()
