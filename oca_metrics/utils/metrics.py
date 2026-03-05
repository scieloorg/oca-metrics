from typing import (
    Any,
    Dict,
    List,
    Optional
)

import re
import numpy as np
import pandas as pd


THRESHOLD_KEY_PATTERN = re.compile(r"^C_top(\d+)pct(?:_window_(\d+)y)?$")
DEFAULT_IMPACT_MIN_PUBS_ABS = 8
DEFAULT_IMPACT_MIN_PUBS_MEDIAN_RATIO = 0.5


def compute_percentiles(citations: List[int], percentiles: List[float]) -> Dict[float, float]:
    """Compute citation thresholds for given percentiles."""
    if not isinstance(citations, (list, tuple)) or not isinstance(percentiles, (list, tuple)):
        raise ValueError("Inputs must be lists.")

    if not citations:
        return {p: 0 for p in percentiles}

    arr = np.array(citations)

    return {p: float(np.percentile(arr, p * 100)) for p in percentiles}


def compute_cohort_impact(journal_mean: float, category_mean: float) -> float:
    """Compute cohort impact (journal mean / category mean), returns 0 if denominator is 0."""
    if not category_mean:
        return 0.0

    return journal_mean / category_mean


def compute_impact_comparability_reference(
    publication_counts: pd.Series,
    min_publications_abs: int = DEFAULT_IMPACT_MIN_PUBS_ABS,
    median_ratio: float = DEFAULT_IMPACT_MIN_PUBS_MEDIAN_RATIO,
) -> Dict[str, float]:
    """
    Compute cohort-level comparability reference values for impact metrics.

    Rule:
      min_required = max(min_publications_abs, ceil(median(publication_counts) * median_ratio))
    """
    counts = pd.to_numeric(publication_counts, errors="coerce").dropna()
    cohort_median = float(counts.median()) if not counts.empty else 0.0

    safe_abs = max(1, int(min_publications_abs))
    safe_ratio = max(0.0, float(median_ratio))
    ratio_required = int(np.ceil(cohort_median * safe_ratio))
    min_required = max(safe_abs, ratio_required)

    return {
        "cohort_journal_publications_median": cohort_median,
        "cohort_impact_min_pubs_required": min_required,
    }


def compute_impact_is_comparable(
    publication_counts: pd.Series,
    min_required: float,
) -> pd.Series:
    """Flag whether each journal has enough publications for comparable cohort impact."""
    counts = pd.to_numeric(publication_counts, errors="coerce").fillna(0)
    threshold = max(1, int(min_required))
    return counts.ge(threshold).astype(int)


def build_threshold_key(pct_val: int, window: Optional[int] = None) -> str:
    if window is None:
        return f"C_top{pct_val}pct"

    return f"C_top{pct_val}pct_window_{window}y"


def extract_threshold_pct_values(thresholds: Dict[str, Any]) -> List[int]:
    pct_values = set()
    for key in thresholds.keys():
        match = THRESHOLD_KEY_PATTERN.match(key)
        if match:
            pct_values.add(int(match.group(1)))

    return sorted(pct_values, reverse=True)


def compute_share_pct(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce").fillna(0)
    den = pd.to_numeric(denominator, errors="coerce")
    return (num / den.replace(0, np.nan) * 100).fillna(0.0)
