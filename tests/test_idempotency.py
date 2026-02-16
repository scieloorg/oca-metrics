from oca_metrics.utils.metrics import compute_percentiles


def test_idempotent_percentiles():
    citations = [1, 2, 3, 4, 5]
    p1 = compute_percentiles(citations, [0.5, 0.9])
    p2 = compute_percentiles(citations, [0.5, 0.9])
    assert p1 == p2
