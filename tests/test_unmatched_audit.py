import pandas as pd
import pyarrow.parquet as pq


def test_unmatched_scielo_parquet_exists(tmp_path):
    # Simulate creation of unmatched_scielo.parquet
    df = pd.DataFrame({
        'work_id': ['scielo:1', 'scielo:2'],
        'publication_year': [2020, 2021],
        'citations_total': [0, 0],
        'domain': ['Health', 'Biology']
    })
    out_path = tmp_path / 'unmatched_scielo.parquet'
    df.to_parquet(out_path)
    assert out_path.exists()

    # Check schema
    table = pq.read_table(out_path)
    assert 'work_id' in table.column_names
    assert 'citations_total' in table.column_names

    # Check content
    loaded = pd.read_parquet(out_path)
    assert (loaded['citations_total'] == 0).all()
    assert len(loaded) == 2
