import pandas as pd
import pyarrow.parquet as pq


def test_parquet_schema_change(tmp_path):
    # Simulate a schema change: missing required column
    df = pd.DataFrame({'work_id': ['a', 'b']})
    out_path = tmp_path / 'test.parquet'
    df.to_parquet(out_path)

    # Try to read expecting a column that does not exist
    table = pq.read_table(out_path)
    assert 'work_id' in table.column_names
    assert 'publication_year' not in table.column_names
